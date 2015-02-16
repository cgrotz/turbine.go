package backend

import (
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/xuyu/goredis"
	"log"
	"strings"
	"time"
)

type RedisBackend struct {
}

func (b *RedisBackend) GetPipelines() ([]Pipeline, error) {
	client, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return nil, err
	}

	pipelineKeys, err := client.Keys("pipelines:*")
	if err != nil {
		log.Panic("Error retrieving pipeline keys:", err.Error())
		return nil, err
	}
	log.Println("Retrieved pipeline keys:", pipelineKeys)

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
			log.Panic("Error decoding pipeline \"%s\"", string(val), decodingErr.Error())
			return nil, err
		}

		// maybe better via expand
		pipelineStatistic, err := b.RetrievePipelineStatistic(pipeline.Id)
		if decodingErr != nil {
			log.Panic("Error retrieving pipeline statistic:", decodingErr.Error())
			return nil, decodingErr
		}
		pipeline.PipelineStatistic = *pipelineStatistic

		pipelines = append(pipelines, pipeline)
	}
	log.Println("Returning pipeline:", pipelines)

	return pipelines, nil
}

func (b *RedisBackend) CreatePipeline(pipeline *Pipeline) (*Pipeline, error) {
	if pipeline.Id != "" {
		log.Println("storing pipeline ", (*pipeline).Id, *pipeline)
	} else {
		id := fmt.Sprintf("%s", uuid.NewV4())
		log.Println("storing pipeline ", *pipeline)

		pipeline.Id = id
	}

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

	client.Set("pipelines:"+pipeline.Id, marshalledPipeline, 0, 0, false, false)
	log.Println("Stored pipeline ", string(pipelineStr[:]))

	return pipeline, nil
}

func (b *RedisBackend) GetPipeline(id string) (*Pipeline, error) {
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

	var consumers []Consumer
	consumerKeys, err := client.SMembers("pipeline:" + id + ":consumers")
	for _, consumerKey := range consumerKeys {
		var consumer Consumer
		consumer.Id = consumerKey[(strings.LastIndex(consumerKey, ":") + 1):]
		len, _ := client.LLen(consumerKey)
		consumer.UnreadElements = len

		consumers = append(consumers, consumer)
	}
	readPipeline.Consumers = consumers

	return readPipeline, nil
}

func (b *RedisBackend) RetrievePipelineStatistic(id string) (*PipelineStatistic, error) {
	jedis, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return nil, err
	}

	currentTime := time.Now()

	todayFormatted := fmt.Sprintf("%d-%02d-%02d", currentTime.Year(), currentTime.Month(), currentTime.Day())
	today, err := jedis.IncrBy("pipeline:"+id+":statistics:"+todayFormatted, 0)
	if err != nil {
		log.Panic("Error retrieving todays statistic information:", err.Error())
		return nil, err
	}

	yesterdayFormatted := fmt.Sprintf("%d-%02d-%02d", currentTime.AddDate(0, 0, -1).Year(), currentTime.AddDate(0, 0, -1).Month(), currentTime.AddDate(0, 0, -1).Day())
	yesterday, err := jedis.IncrBy("pipeline:"+id+":statistics:"+yesterdayFormatted, 0)
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

	for i := 0; i < 10; i++ {
		pipelineStatisticElement := &PipelineStatisticElement{}
		elementDate := currentTime.AddDate(0, 0, -1*i)
		dateString := fmt.Sprintf("%d-%02d-%02d", elementDate.Year(), elementDate.Month(), elementDate.Day())
		pipelineStatisticElement.Date = dateString
		pipelineStatisticElement.Intake, err = jedis.IncrBy("pipeline:"+id+":statistics:"+dateString, 0)
		if err != nil {
			log.Fatal("Error retrieving statistics information:", err.Error())
		}

		pipelineStatistic.Statistics = append(pipelineStatistic.Statistics, *pipelineStatisticElement)
	}
	return pipelineStatistic, nil
}

func (b *RedisBackend) UpdatePipeline(id string, pipeline *Pipeline) (*Pipeline, error) {
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

func (b *RedisBackend) DeletePipeline(id string) (bool, error) {
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

func (b *RedisBackend) PopDatapoint(pipelineId string, consumerId string) ([]string, error) {
	jedis, err := goredis.Dial(&goredis.DialConfig{Address: "127.0.0.1:6379"})
	if err != nil {
		log.Panic("error opening connection to redis:", err.Error())
		return nil, err
	}
	consumerKey := "pipeline:" + pipelineId + ":consumers:" + consumerId
	// Add consumer to set of consumers for pipeline
	jedis.SAdd("pipeline:"+pipelineId+":consumers", consumerKey)

	// current pointer
	currentElementPointer, _ := jedis.IncrBy("pipeline:"+pipelineId+":datapoints", 0)
	// first readable element; the plan would be for a cleanup job to run, increasing this pointer ever forward
	firstElementPointer, _ := jedis.IncrBy("pipeline:"+pipelineId+":firstdatapoint", 0)
	// pointer for the consumer
	consumerPointer, _ := jedis.IncrBy(consumerKey, 0)
	if consumerPointer == 0 {
		consumerPointer = firstElementPointer
	}

	readableElements := currentElementPointer - consumerPointer
	log.Printf("Readable elements:%d for pipeline:%s consumerId:%s", readableElements, pipelineId, consumerId)

	if readableElements > 0 {
		var datapoints []string

		for i := int64(0); i < 10 && i < readableElements; i++ {
			elementKey := fmt.Sprintf("pipeline:%s:datapoints:%d:*", pipelineId, consumerPointer+i)
			log.Printf("Read keys for element %s", elementKey)
			keys, _ := jedis.Keys(elementKey)
			log.Printf("Read keys for element %d values %s", consumerPointer+i, keys)
			if len(keys) > 0 {
				value, _ := jedis.Get(keys[0])
				log.Printf("Read element v %d values %s", keys[0], value)
				stringvalue := string(value)
				if stringvalue != "" {
					datapoints = append(datapoints, stringvalue)
				}
			}
		}
		if readableElements < 10 {
			jedis.Set(consumerKey, fmt.Sprintf("%d", consumerPointer+readableElements), 0, 0, false, false)
			log.Printf("Set consumer pointer to %d", consumerPointer+readableElements)
		} else {
			jedis.Set(consumerKey, fmt.Sprintf("%d", consumerPointer+10), 0, 0, false, false)
			log.Printf("Set consumer pointer to %d", consumerPointer+10)
		}
		log.Printf("Returning datapoints: %s", datapoints)
		return datapoints, nil
	}

	// The below code works against consumers that consum basically a fanout into redis lists
	// Should be converted in a QoS based approach
	/*
		consumersSetKey := "pipeline:" + pipelineId + ":consumers"
		consumerKey := "pipeline:" + pipelineId + ":consumers:" + consumerId

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
	*/
	return nil, nil
}

/*
 *
 */
func (b *RedisBackend) PushDatapoint(pipelineId string, value string) (bool, error) {
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
		var newPipeline Pipeline
		newPipeline.Id = pipelineId
		newPipeline.Name = pipelineId
		newPipeline.Description = "Dynamically generated pipeline"
		b.CreatePipeline(&newPipeline)
	}

	pointer, err := client.Incr("pipeline:" + pipelineId + ":datapoints")
	if err != nil {
		log.Panic("Error opening connection to redis:", err.Error())
		return false, err
	}

	elementKey := fmt.Sprintf("pipeline:%s:datapoints:%d:%d", pipelineId, pointer, time.Now().Unix())
	elementValue := fmt.Sprintf("%s", value)
	client.Set(elementKey, elementValue, 0, 0, false, false)
	log.Printf("Added element at %s with value %s", elementKey, elementValue)

	client.Incr("pipeline:" + pipelineId + ":statistics:" + fmt.Sprintf("%d-%02d-%02d", time.Now().Year(), time.Now().Month(), time.Now().Day()))

	// At the moment here the distribution to the consumers happens here
	// Maybe this should be kept for consumers with a specific QoS requirement?
	/*consumers, err := client.SMembers("pipeline:" + pipelineId + ":consumers")
	for _, consumerKey := range consumers {
		client.LPush(consumerKey, fmt.Sprintf("%s", value))
	}*/

	// TODO add redis pubsub distribution of message?

	return true, nil
}
