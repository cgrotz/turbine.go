package backend

/*
import (
	"github.com/boltdb/bolt"
	"log"
	"os"
)

type BoltBackend struct {
	DB bolt.DB
}

func (b *BoltBackend) Open(filename string, mod os.FileMode) {
	db, err := bolt.Open(filename, mod, nil)
	if err != nil {
		log.Fatal(err)
	}
	b.DB = *db
}

func (b *BoltBackend) Close() {
	b.DB.Close()
}

func (b *BoltBackend) GetPipelines() ([]Pipeline, error) {
	err := b.DB.View(func(tx *bolt.Tx) error {

		return nil
	})

	var pipelines []Pipeline

	for _, pipelineKey := range pipelineKeys {
		val, err := client.Get(pipelineKey)
		if err != nil {
			log.Panic("Error retrieving pipeline:", err.Error())
			return nil, err
		}
		log.Println("Retrieved pipeline:", val)

		var pipeline Pipeline
		decodingErr := json.Unmarshal(val, &pipeline)
		if decodingErr != nil {
			log.Panic("Error decoding pipeline:", decodingErr.Error())
			return nil, err
		}
		pipelines = append(pipelines, pipeline)
	}
	log.Println("Returning pipeline:", pipelines)

	return pipelines, nil
}

/*
func (b *BoltBackend) CreatePipeline(pipeline *Pipeline) (*Pipeline, error) {
	id := fmt.Sprintf("%s", uuid.NewV4())

	log.Println("storing pipeline ", *pipeline)

	pipeline.Id = id

	client, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return nil, err
	}
	log.Println("Inserting pipeline ", pipeline)

	pipelineStr, marshallingErr := json.Marshal(pipeline)
	if marshallingErr != nil {
		log.Panic("Error retrieving pipelines:", marshallingErr.Error())
		return nil, marshallingErr
	}
	marshalledPipeline := string(pipelineStr[:])
	log.Println("Storing pipeline ", marshalledPipeline)

	client.Set("pipelines:"+id, marshalledPipeline, 0, 0, false, false)
	log.Println("Stored pipeline ", string(pipelineStr[:]))

	return pipeline, nil
}

func (b *BoltBackend) GetPipeline(id string) (*Pipeline, error) {
	client, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return nil, err
	}

	readPipelineStr, err := client.Get("pipelines:" + id)
	if err != nil {
		log.Panic("Error retrieving pipeline from redis:", err.Error())
		return nil, err
	}

	readPipeline := &Pipeline{}
	decodingErr := json.Unmarshal(readPipelineStr, &readPipeline)
	if decodingErr != nil {
		log.Panic("Error decoding pipeline:", decodingErr.Error())
		return nil, decodingErr
	}
	// maybe better via expand
	pipelineStatistic, err := b.RetrievePipelineStatistic(id)
	if decodingErr != nil {
		log.Panic("Error retrieving pipeline statistic:", decodingErr.Error())
		return nil, decodingErr
	}
	readPipeline.PipelineStatistic = *pipelineStatistic
	return readPipeline, nil
}

func (b *BoltBackend) RetrievePipelineStatistic(id string) (*PipelineStatistic, error) {
	jedis, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return nil, err
	}

	currentTime := time.Now()

	today, err := jedis.IncrBy("pipeline:"+id+":statistics:"+currentTime.Format("yyyy-MM-dd"), 0)
	if err != nil {
		log.Panic("Error retrieving todays statistic information:", err.Error())
		return nil, err
	}
	yesterday, err := jedis.IncrBy("pipeline:"+id+":statistics:"+currentTime.AddDate(0, 0, -1).Format("yyyy-MM-dd"), 0)
	if err != nil {
		log.Panic("Error retrieving yesterdays statistic information:", err.Error())
		return nil, err
	}

	pipelineStatistic := &PipelineStatistic{}

	pipelineStatistic.Today = today
	if yesterday != 0 {
		pipelineStatistic.ChangeRate = (((float64(today) - float64(yesterday)) / float64(yesterday)) * 100.0)
	} else {
		pipelineStatistic.ChangeRate = 0.0
	}

	for i := 0; i < 30; i++ {
		pipelineStatisticElement := &PipelineStatisticElement{}
		elementDate := currentTime.AddDate(0, 0, -1*i)
		pipelineStatisticElement.Date = elementDate
		pipelineStatisticElement.Intake, err = jedis.IncrBy("pipeline:"+id+":statistics:"+elementDate.Format("yyyy-MM-dd"), 0)
		if err != nil {
			log.Fatal("Error retrieving statistics information:", err.Error())
		}

		pipelineStatistic.Statistics = append(pipelineStatistic.Statistics, *pipelineStatisticElement)
	}
	return pipelineStatistic, nil
}

func (b *BoltBackend) UpdatePipeline(id string, pipeline *Pipeline) (*Pipeline, error) {
	client, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return nil, err
	}

	readPipelineStr, err := client.Get("pipelines:" + id)
	if err != nil {
		log.Panic("Error reading pipeline:", err.Error())
		return nil, err
	}

	readPipeline := &Pipeline{}
	decodingErr2 := json.Unmarshal(readPipelineStr, &readPipeline)
	if decodingErr2 != nil {
		log.Panic("Error retrieving pipeline:", decodingErr2.Error())
		return nil, decodingErr2
	}

	readPipeline.Name = pipeline.Name
	readPipeline.Description = pipeline.Description

	pipelineStr, marshallingErr := json.Marshal(readPipeline)
	if marshallingErr != nil {
		log.Panic("Error marshalling stored pipeline:", marshallingErr.Error())
		return nil, marshallingErr
	}
	marshalledPipeline := string(pipelineStr[:])
	client.Set("pipelines:"+id, marshalledPipeline, 0, 0, false, false)

	return readPipeline, nil
}

func (b *BoltBackend) DeletePipeline(id string) (bool, error) {
	client, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return false, err
	}

	_, err = client.Del("pipelines:" + id)
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return false, err
	}
	return true, nil
}

func (b *BoltBackend) PopDatapoint(id string, consumerId string) ([]string, error) {
	jedis, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("error opening connection to redis:", err.Error())
		return nil, err
	}

	consumersSetKey := "pipeline:" + id + ":consumers"
	consumerKey := "pipeline:" + id + ":consumers:" + consumerId

	jedis.SAdd(consumersSetKey, consumerKey)
	consumerListExists, err := jedis.Exists(consumerKey)
	if err != nil {
		log.Panic("Error checking key on existence:", err.Error())
		return nil, err
	}

	if consumerListExists {
		tx, err := jedis.Transaction()
		if err != nil {
			log.Panic("Error opening transaction on redis:", err.Error())
			return nil, err
		}
		for i := 0; i < 10; i++ {
			err := tx.Command("LPOP", consumerKey)
			if err != nil {
				log.Panic("Error adding command to transaction:", err.Error())
				return nil, err
			}
		}
		reply, err := tx.Exec()
		if err != nil {
			log.Panic("Error executing transaction:", err.Error())
			return nil, err
		}

		var datapoints []string

		for _, element := range reply {
			stringvalue, err := element.StringValue()
			if err != nil {
				log.Panic("Error reading result from transaction:", err.Error())
				return nil, err
			}
			if len(stringvalue) > 0 {
				datapoints = append(datapoints, stringvalue)
			}
		}
		return datapoints, nil
	}
	return nil, nil
}

func (b *BoltBackend) PushDatapoint(pipelineId string, value string) (bool, error) {
	client, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return false, err
	}

	pipelineExists, err := client.Exists("pipelines:" + pipelineId)
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return false, err
	}
	if !pipelineExists {
		client.Set("pipelines:"+pipelineId, "{ id:'"+pipelineId+"', name: '"+pipelineId+"', name: 'Dynamically generated pipeline' }", 0, 0, false, false)
	}

	pointer, err := client.Incr("pipeline:" + pipelineId + ":datapoints")
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return false, err
	}

	client.Set(fmt.Sprintf("pipeline:%s:datapoints:%i:%i", pipelineId, pointer, time.Now().Unix()), fmt.Sprintf("%s", value), 0, 0, false, false)

	client.Incr("pipeline:" + pipelineId + ":statistics:" + time.Now().Format("yyyy-MM-dd"))

	consumers, err := client.SMembers("pipeline:" + pipelineId + ":consumers")
	for _, consumerKey := range consumers {
		client.LPush(consumerKey, fmt.Sprintf("%s", value))
	}

	return true, nil
}
*/
