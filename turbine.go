package main

import (
	"cgrotz/turbined/backend"
	"encoding/json"
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

func main() {
	//mqtt.Run()
	app := cli.NewApp()
	app.Name = "turbined"
	app.Usage = "message queue for the cloud"
	app.Commands = []cli.Command{
		{
			Name:      "run",
			ShortName: "r",
			Usage:     "run the Turbine server",
			Action: func(c *cli.Context) {
				run(c.GlobalString("redisUrl"), c.GlobalString("bind"))
			},
		},
		{
			Name:      "status",
			ShortName: "s",
			Usage:     "show cluster status",
			Action: func(c *cli.Context) {
				println("status")
			},
		},
	}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "bind",
			Value:  ":3000",
			Usage:  "http bind for communication, e.g. ':3000'",
			EnvVar: "TURBINE_HTTP_BIND",
		},
		cli.StringFlag{
			Name:   "redisUrl",
			Value:  "tcp://127.0.0.1:6379",
			Usage:  "addresses of redis, e.g. tcp://127.0.0.1:6379",
			EnvVar: "REDIS_PORT_6379_TCP",
		},
	}

	app.Run(os.Args)
}

type Server struct {
	Backend backend.Backend
}

func run(address string, binding string) {
	println("___________          ___.   .__")
	println("\\__    ___/_ ________\\_ |__ |__| ____   ____")
	println("  |    | |  |  \\_  __ \\ __ \\|  |/    \\_/ __ \\")
	println("  |    | |  |  /|  | \\/ \\_\\ \\  |   |  \\  ___/")
	println("  |____| |____/ |__|  |___  /__|___|  /\\___  >")
	println("                          \\/        \\/     \\/")

	println()
	log.Println("printing configuration")
	log.Printf("redis: %s", address)
	log.Printf("http bind to: %s", binding)
	server := &Server{}
	redisBackend := backend.RedisBackend{RedisUrl: address}
	server.Backend = backend.Backend(redisBackend)

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

	http.ListenAndServe(binding, nil)
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {

	pipelines, err := s.Backend.GetPipelines()
	if err != nil {
		log.Fatal("Error retrieving pipeline:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	str, err := json.Marshal(pipelines)
	if err != nil {
		log.Fatal("Error marshalling pipeline:", err.Error())
		http.Error(w, err.Error(), 500)
		return
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
		http.Error(w, err.Error(), 500)
		return
	}

	pipeline, err = s.Backend.CreatePipeline(pipeline)
	if err != nil {
		log.Fatal("Error saving pipeline:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	str, err := json.Marshal(pipeline)
	if err != nil {
		log.Fatal("Error marshalling pipeline response:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	w.Write(str)

	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	pipeline, err := s.Backend.GetPipeline(id)
	if err != nil {
		log.Fatal("Error retrieving pipeline:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	str, err := json.Marshal(pipeline)
	if err != nil {
		log.Fatal("Error marshalling pipeline response:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	w.Write(str)
	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) updatePipeline(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	pipeline := &backend.Pipeline{}
	decoder := json.NewDecoder(r.Body)
	decodingErr := decoder.Decode(&pipeline)
	if decodingErr != nil {
		log.Fatal("ERROR decoding JSON - ", decodingErr.Error())
		http.Error(w, decodingErr.Error(), 500)
		return
	}

	_, err := s.Backend.UpdatePipeline(id, pipeline)
	if err != nil {
		log.Fatal("Error retrieving pipeline:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) deletePipeline(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	_, err := s.Backend.DeletePipeline(id)
	if err != nil {
		log.Fatal("Error deleting pipeline:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) getPipelineStatistics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	pipelineStatistic, err := s.Backend.RetrievePipelineStatistic(id)
	if err != nil {
		log.Fatal("Error retrieving pipeline statistic:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	str, err := json.Marshal(pipelineStatistic)
	if err != nil {
		log.Fatal("Error marshalling response:", err.Error())
		http.Error(w, err.Error(), 500)
		return
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
			http.Error(w, err.Error(), 500)
			return
		}

		str, err := json.Marshal(datapoints)
		if err != nil {
			log.Fatal("Error retrieving pipeline:", err.Error())
			http.Error(w, err.Error(), 500)
			return
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
		http.Error(w, err.Error(), 500)
		return
	}

	datapointIndex, err := s.Backend.PushDatapoint(id, string(bodyStr))
	if err != nil {
		log.Fatal("Error pushing datapoint:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Add("Location", fmt.Sprintf("http://localhost:3000/api/v1/%s/datapoints/%d", id, datapointIndex))
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
