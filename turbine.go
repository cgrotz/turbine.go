package main

import (
	"cgrotz/turbined/backend"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

type Server struct {
	Backend backend.Backend
}

func main() {
	server := &Server{}
	server.Backend = new(backend.RedisBackend)

	r := mux.NewRouter()
	r.Path("/api/v1/pipelines").Methods("GET").HandlerFunc(server.listPipelines)
	r.Path("/api/v1/pipelines").Methods("POST").HandlerFunc(server.createPipeline)

	r.Path("/api/v1/pipelines/{id}").Methods("GET").HandlerFunc(server.getPipeline)
	r.Path("/api/v1/pipelines/{id}").Methods("PUT").HandlerFunc(server.updatePipeline)
	r.Path("/api/v1/pipelines/{id}").Methods("DELETE").HandlerFunc(server.deletePipeline)

	r.Path("/api/v1/pipelines/{id}/statistics").Methods("GET").HandlerFunc(server.getPipelineStatistics)

	// TODO not sure how to solve that yet...
	r.Path("/api/v1/pipelines/{id}/datapoints").Headers("Accept", "text/event-stream").Methods("GET").HandlerFunc(server.stream)

	r.Path("/api/v1/pipelines/{id}/datapoints").Methods("GET").HandlerFunc(server.popDatapoint)

	r.Path("/api/v1/pipelines/{id}/datapoints").Methods("POST").HandlerFunc(server.pushDatapoint)

	http.Handle("/api/v1/", r)

	http.Handle("/", http.FileServer(http.Dir("ui/build")))

	http.ListenAndServe(":3000", nil)

	go turbine()
}

/**
* This is only required once we support the fanout into QoSed consumers
 */
func turbine() {
	/*
		  TODO
		  for {

				jedis, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
				if err != nil {
					log.Fatal("Error opening connection to redis:", err.Error())
				}

			}*/
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {

	pipelines, err := s.Backend.GetPipelines()
	if err != nil {
		log.Fatal("Error retrieving pipeline:", err.Error())
	}

	str, err := json.Marshal(pipelines)
	if err != nil {
		log.Fatal("Error marshalling pipeline:", err.Error())
	}

	w.Write(str)
	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	pipeline := &backend.Pipeline{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&pipeline)
	if err != nil {
		log.Fatal("ERROR decoding JSON - ", err.Error())
	}

	_, err = s.Backend.CreatePipeline(pipeline)
	if err != nil {
		log.Fatal("Error saving pipeline:", err.Error())
	}

	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	pipeline, err := s.Backend.GetPipeline(id)
	if err != nil {
		log.Fatal("Error retrieving pipeline:", err.Error())
	}

	str, err := json.Marshal(pipeline)
	if err != nil {
		log.Fatal("Error marshalling pipeline response:", err.Error())
	}

	w.Write(str)
}

func (s *Server) updatePipeline(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	pipeline := &backend.Pipeline{}
	decoder := json.NewDecoder(r.Body)
	decodingErr := decoder.Decode(&pipeline)
	if decodingErr != nil {
		log.Fatal("ERROR decoding JSON - ", decodingErr.Error())
	}

	_, err := s.Backend.UpdatePipeline(id, pipeline)
	if err != nil {
		log.Fatal("Error retrieving pipeline:", err.Error())
	}

	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) deletePipeline(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	_, err := s.Backend.DeletePipeline(id)
	if err != nil {
		log.Fatal("Error deleting pipeline:", err.Error())
	}

	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) getPipelineStatistics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	pipelineStatistic, err := s.Backend.RetrievePipelineStatistic(id)
	if err != nil {
		log.Fatal("Error retrieving pipeline statistic:", err.Error())
	}

	str, err := json.Marshal(pipelineStatistic)
	if err != nil {
		log.Fatal("Error marshalling response:", err.Error())
	}

	w.Write(str)
	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) popDatapoint(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pipelineId := vars["id"]

	query := r.URL.Query()
	consumerId := query["consumer"]
	if len(consumerId) > 0 {
		datapoints, err := s.Backend.PopDatapoint(pipelineId, consumerId[0])
		if err != nil {
			log.Fatal("Error retrieving pipeline:", err.Error())
		}

		str, err := json.Marshal(datapoints)
		if err != nil {
			log.Fatal("Error retrieving pipeline:", err.Error())
		}

		w.Write(str)
	}
	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) pushDatapoint(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	bodyStr, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal("Error opening connection to redis:", err.Error())
	}

	_, err = s.Backend.PushDatapoint(id, string(bodyStr))
	if err != nil {
		log.Fatal("Error pushing datapoint:", err.Error())
	}

	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pipelineId := vars["id"]

	if pipelineId != "" {
		// Make sure that the writer supports flushing.
		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
			return
		}

		messageChan := make(chan string)
		// TODO register messageChan against pipeline, maybe redis pub/sub?

		// Listen to the closing of the http connection via the CloseNotifier
		notify := w.(http.CloseNotifier).CloseNotify()
		go func() {
			<-notify
			// TODO deregister channel
			log.Println("HTTP connection just closed.")
		}()

		// Add SSE Headers
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		for {
			// Read from our messageChan.
			msg := <-messageChan

			// Write to the ResponseWriter, `w`.
			fmt.Fprintf(w, "data: Message: %s\n\n", msg)

			// Flush the response.  This is only possible if
			// the repsonse supports streaming.
			f.Flush()
		}
	}
	log.Println("Finished HTTP request at ", r.URL.Path)
}
