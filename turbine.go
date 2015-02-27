package main

import (
	"cgrotz/turbined/backend"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/codegangsta/cli"
	"github.com/gorilla/mux"
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
				run(c.GlobalInt("writers"), c.GlobalString("redisUrl"), c.GlobalString("bind"))
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
		cli.IntFlag{
			Name:   "writers",
			Value:  100,
			Usage:  "amount of parallel running turbine noozles",
			EnvVar: "TURBINE_WRITERS",
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

func run(writers int, address string, binding string) {
	println("___________          ___.   .__")
	println("\\__    ___/_ ________\\_ |__ |__| ____   ____")
	println("  |    | |  |  \\_  __ \\ __ \\|  |/    \\_/ __ \\")
	println("  |    | |  |  /|  | \\/ \\_\\ \\  |   |  \\  ___/")
	println("  |____| |____/ |__|  |___  /__|___|  /\\___  >")
	println("                          \\/        \\/     \\/")

	log.Println("printing configuration")
	log.Printf("redis: %s", address)
	log.Printf("http bind to: %s", binding)
	log.Printf("writers: %d", writers)

	go metrics.Log(metrics.DefaultRegistry, 10e9, log.New(os.Stdout, "metrics: ", log.Lmicroseconds))

	server := &Server{}
	redisBackend := backend.RedisBackend{RedisUrl: address, Datapoints: make(chan *backend.Datapoint)}
	server.Backend = backend.Backend(redisBackend)

	hash, err := redisBackend.StartScripting()
	if err != nil {
		log.Fatal("Unable to load scripts to redis")
	}

	t := metrics.NewTimer()
	metrics.Register("messageloop", t)
	// Initialize writers
	for i := 1; i < writers+1; i++ {
		go redisBackend.Start(t, hash, redisBackend.Datapoints)
	}

	// Rest Interface
	r := mux.NewRouter()
	// Pipelines
	r.Path("/api/v1/pipelines").Methods("GET").HandlerFunc(server.listPipelines)
	r.Path("/api/v1/pipelines").Methods("POST").HandlerFunc(server.createPipeline)

	// Pipeline
	r.Path("/api/v1/pipelines/{id}").Methods("GET").HandlerFunc(server.getPipeline)
	r.Path("/api/v1/pipelines/{id}").Methods("PUT").HandlerFunc(server.updatePipeline)
	r.Path("/api/v1/pipelines/{id}").Methods("DELETE").HandlerFunc(server.deletePipeline)

	// Pipeline statistics
	r.Path("/api/v1/pipelines/{id}/statistics").Methods("GET").HandlerFunc(server.getPipelineStatistics)

	// Datapoint Endpoints
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

	marshalResponse(w, r, pipelines)
	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	pipeline := &backend.Pipeline{}
	decodeBody(w, r, pipeline)

	pipeline, err := s.Backend.CreatePipeline(pipeline)
	if err != nil {
		log.Fatal("Error saving pipeline:", err.Error())
		http.Error(w, err.Error(), 500)
		return
	}

	marshalResponse(w, r, pipeline)

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

	marshalResponse(w, r, pipeline)
	log.Println("Finished HTTP request at ", r.URL.Path)
}

func (s *Server) updatePipeline(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	pipeline := &backend.Pipeline{}
	decodeBody(w, r, pipeline)

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

	marshalResponse(w, r, pipelineStatistic)
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

		marshalResponse(w, r, datapoints)
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
		// TODO register message/Chan against pipeline, maybe redis pub/sub?

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

func marshalResponse(w http.ResponseWriter, r *http.Request, obj interface{}) {
	if len(r.Header["Accept"]) > 0 && r.Header["Accept"][0] == "text/xml" {
		str, err := xml.Marshal(obj)
		if err != nil {
			log.Fatal("Error marshalling pipeline:", err.Error())
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/xml")
		w.Write(str)
	} else {
		str, err := json.Marshal(obj)
		if err != nil {
			log.Fatal("Error marshalling pipeline:", err.Error())
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(str)
	}
}

func decodeBody(w http.ResponseWriter, r *http.Request, obj interface{}) {
	if len(r.Header["Content-Type"]) > 0 && r.Header["Content-Type"][0] == "text/xml" {
		decoder := xml.NewDecoder(r.Body)
		err := decoder.Decode(obj)
		if err != nil {
			log.Fatal("ERROR decoding XML - ", err.Error())
			http.Error(w, err.Error(), 500)
			return
		}

	} else {
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(obj)
		if err != nil {
			log.Fatal("ERROR decoding JSON - ", err.Error())
			http.Error(w, err.Error(), 500)
			return
		}
	}
}
