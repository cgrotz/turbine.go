package backend

import (
	"encoding/json"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"github.com/satori/go.uuid"
	"github.com/xuyu/goredis"
	"log"
	"strings"
	"time"
)

type RedisBackend struct {
	RedisUrl   string
	Datapoints chan *Datapoint
}

func (b RedisBackend) openConnection() (*goredis.Redis, error) {
	return goredis.DialURL(b.RedisUrl + "/0?timeout=10s&maxidle=10")
}

func (b RedisBackend) GetPipelines() ([]Pipeline, error) {
	redis, err := b.openConnection()
	if err != nil {
		log.Fatal("Error opening connection to redis:", err.Error())
		return nil, err
	}

	pipelineKeys, err := redis.Keys("pipelines:*")
	if err != nil {
		log.Fatal("Error retrieving pipeline keys:", err.Error())
		return nil, err
	}

	var pipelines []Pipeline

	for _, pipelineKey := range pipelineKeys {
		val, err := redis.Get(pipelineKey)
		if err != nil {
			log.Fatal("Error retrieving pipeline:", err.Error())
			return nil, err
		}

		var pipeline Pipeline
		decodingErr := json.Unmarshal(val, &pipeline)
		if decodingErr != nil {
			log.Fatal("Error decoding pipeline \"%s\"", string(val), decodingErr.Error())
			return nil, err
		}

		// maybe better via expand
		pipelineStatistic, err := b.RetrievePipelineStatistic(pipeline.Id)
		if decodingErr != nil {
			log.Fatal("Error retrieving pipeline statistic:", decodingErr.Error())
			return nil, decodingErr
		}
		pipeline.PipelineStatistic = *pipelineStatistic

		pipelines = append(pipelines, pipeline)
	}

	return pipelines, nil
}

func (b RedisBackend) CreatePipeline(pipeline *Pipeline) (*Pipeline, error) {
	if pipeline.Id == "" {
		id := fmt.Sprintf("%s", uuid.NewV4())
		pipeline.Id = id
	}

	redis, err := b.openConnection()
	if err != nil {
		log.Fatal("Error opening connection to redis:", err.Error())
		return nil, err
	}

	pipelineStr, marshallingErr := json.Marshal(pipeline)
	if marshallingErr != nil {
		log.Fatal("Error retrieving pipelines:", marshallingErr.Error())
		return nil, marshallingErr
	}
	marshalledPipeline := string(pipelineStr[:])

	redis.Set("pipelines:"+pipeline.Id, marshalledPipeline, 0, 0, false, false)

	return pipeline, nil
}

func (b RedisBackend) GetPipeline(id string) (*Pipeline, error) {
	redis, err := b.openConnection()
	if err != nil {
		log.Fatal("Error opening connection to redis:", err.Error())
		return nil, err
	}

	readPipelineStr, err := redis.Get("pipelines:" + id)
	if err != nil {
		log.Fatal("Error retrieving pipeline from redis:", err.Error())
		return nil, err
	}

	readPipeline := &Pipeline{}
	decodingErr := json.Unmarshal(readPipelineStr, &readPipeline)
	if decodingErr != nil {
		log.Fatal("Error decoding pipeline:", decodingErr.Error())
		return nil, decodingErr
	}
	// maybe better via expand
	pipelineStatistic, err := b.RetrievePipelineStatistic(id)
	if decodingErr != nil {
		log.Fatal("Error retrieving pipeline statistic:", decodingErr.Error())
		return nil, decodingErr
	}
	readPipeline.PipelineStatistic = *pipelineStatistic

	currentElementPointer, _ := redis.IncrBy("pipeline:"+id+":datapoints", 0)

	var consumers []Consumer
	consumerKeys, err := redis.SMembers("pipeline:" + id + ":consumers")
	for _, consumerKey := range consumerKeys {
		var consumer Consumer
		consumer.Id = consumerKey[(strings.LastIndex(consumerKey, ":") + 1):]
		len, _ := redis.IncrBy(consumerKey, 0)
		consumer.UnreadElements = currentElementPointer - len

		consumers = append(consumers, consumer)
	}
	readPipeline.Consumers = consumers

	return readPipeline, nil
}

func (b RedisBackend) RetrievePipelineStatistic(id string) (*PipelineStatistic, error) {
	redis, err := b.openConnection()
	if err != nil {
		log.Fatal("Error opening connection to redis:", err.Error())
		return nil, err
	}

	currentTime := time.Now()

	todayFormatted := fmt.Sprintf("%d-%02d-%02d", currentTime.Year(), currentTime.Month(), currentTime.Day())
	today, err := redis.IncrBy("pipeline:"+id+":statistics:"+todayFormatted, 0)
	if err != nil {
		log.Fatal("Error retrieving todays statistic information:", err.Error())
		return nil, err
	}

	yesterdayFormatted := fmt.Sprintf("%d-%02d-%02d", currentTime.AddDate(0, 0, -1).Year(), currentTime.AddDate(0, 0, -1).Month(), currentTime.AddDate(0, 0, -1).Day())
	yesterday, err := redis.IncrBy("pipeline:"+id+":statistics:"+yesterdayFormatted, 0)
	if err != nil {
		log.Fatal("Error retrieving yesterdays statistic information:", err.Error())
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
		pipelineStatisticElement.Intake, err = redis.IncrBy("pipeline:"+id+":statistics:"+dateString, 0)
		if err != nil {
			log.Fatal("Error retrieving statistics information:", err.Error())
		}

		pipelineStatistic.Statistics = append(pipelineStatistic.Statistics, *pipelineStatisticElement)
	}
	return pipelineStatistic, nil
}

func (b RedisBackend) UpdatePipeline(id string, pipeline *Pipeline) (*Pipeline, error) {
	redis, err := b.openConnection()
	if err != nil {
		log.Fatal("Error opening connection to redis:", err.Error())
		return nil, err
	}

	readPipelineStr, err := redis.Get("pipelines:" + id)
	if err != nil {
		log.Fatal("Error reading pipeline:", err.Error())
		return nil, err
	}

	readPipeline := &Pipeline{}
	decodingErr2 := json.Unmarshal(readPipelineStr, &readPipeline)
	if decodingErr2 != nil {
		log.Fatal("Error retrieving pipeline:", decodingErr2.Error())
		return nil, decodingErr2
	}

	readPipeline.Name = pipeline.Name
	readPipeline.Description = pipeline.Description

	pipelineStr, marshallingErr := json.Marshal(readPipeline)
	if marshallingErr != nil {
		log.Fatal("Error marshalling stored pipeline:", marshallingErr.Error())
		return nil, marshallingErr
	}
	marshalledPipeline := string(pipelineStr[:])
	redis.Set("pipelines:"+id, marshalledPipeline, 0, 0, false, false)

	return readPipeline, nil
}

func (b RedisBackend) DeletePipeline(id string) (bool, error) {
	redis, err := b.openConnection()
	if err != nil {
		log.Fatal("Error opening connection to redis:", err.Error())
		return false, err
	}

	_, err = redis.Del("pipelines:" + id)
	if err != nil {
		log.Fatal("Failed deleting pipeline entry:", err.Error())
		return false, err
	}
	return true, nil
}

func (b RedisBackend) PopDatapoint(pipelineId string, consumerId string) ([]string, error) {
	redis, err := b.openConnection()
	if err != nil {
		log.Fatal("error opening connection to redis:", err.Error())
		return nil, err
	}
	consumerKey := "pipeline:" + pipelineId + ":consumers:" + consumerId
	// Add consumer to set of consumers for pipeline
	redis.SAdd("pipeline:"+pipelineId+":consumers", consumerKey)

	// current pointer
	currentElementPointer, _ := redis.IncrBy("pipeline:"+pipelineId+":datapoints", 0)
	// first readable element; the plan would be for a cleanup job to run, increasing this pointer ever forward
	firstElementPointer, _ := redis.IncrBy("pipeline:"+pipelineId+":firstdatapoint", 0)
	// pointer for the consumer
	consumerPointer, _ := redis.IncrBy(consumerKey, 0)
	if consumerPointer == 0 {
		consumerPointer = firstElementPointer
	}

	readableElements := currentElementPointer - consumerPointer

	if readableElements > 0 {
		var datapoints []string

		for i := int64(0); i < 10 && i < readableElements; i++ {
			elementKey := fmt.Sprintf("pipeline:%s:datapoints:%d", pipelineId, consumerPointer+i)
			value, _ := redis.Get(elementKey)
			stringvalue := string(value)
			if stringvalue != "" {
				datapoints = append(datapoints, stringvalue)
			}
		}
		if readableElements < 10 {
			redis.Set(consumerKey, fmt.Sprintf("%d", consumerPointer+readableElements), 0, 0, false, false)
		} else {
			redis.Set(consumerKey, fmt.Sprintf("%d", consumerPointer+10), 0, 0, false, false)
		}
		return datapoints, nil
	}

	return nil, nil
}

/*
 *
 */
func (b RedisBackend) PushDatapoint(pipelineId string, value string) (int64, error) {
	b.Datapoints <- &Datapoint{pipelineId, value}
	return 0, nil
}

func (b RedisBackend) StartScripting() (string, error) {
	redis, err := b.openConnection()
	if err != nil {
		log.Fatal("Error opening connection to redis:", err.Error())
		return "", err
	}
	err = redis.ScriptFlush()
	if err != nil {
		log.Fatal("StartScripting, Failed flushing redis scripts:", err.Error())
		return "", err
	}

	hash, err := redis.ScriptLoad(`
			local link_id = redis.call("INCR", KEYS[1])
			redis.call("SET", KEYS[1] .. ":" .. link_id, ARGV[1])
			redis.call("INCR", KEYS[2])
			return link_id`)
	if err != nil {
		log.Fatal("StartScripting, Failed loading script into redis:", err.Error())
		return "", err
	}

	return hash, nil
}

func (b RedisBackend) Start(t metrics.Timer, scriptHash string, datapoints chan *Datapoint) {
	for {
		redis, err := b.openConnection()
		if err != nil {
			log.Fatal("Error opening connection to redis:", err.Error())
		}

		for {
			datapoint := <-datapoints

			t.Time(func() {
				reply, err := redis.EvalSha(scriptHash, []string{"pipeline:" + datapoint.PipelineId + ":datapoints", fmt.Sprintf("pipeline:%s:statistics:%d-%02d-%02d", datapoint.PipelineId, time.Now().Year(), time.Now().Month(), time.Now().Day())}, []string{datapoint.Value})
				if err != nil {
					log.Fatal("Error executing script:", err.Error())
				}

				_, err = reply.IntegerValue()
				if err != nil {
					log.Fatal("Error reading index:", err.Error())
				}
			})
		}
	}
}
