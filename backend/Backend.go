package backend

import ()

type Backend interface {
	GetPipelines() ([]Pipeline, error)
	GetPipeline(id string) (*Pipeline, error)
	CreatePipeline(pipeline *Pipeline) (*Pipeline, error)
	UpdatePipeline(id string, pipeline *Pipeline) (*Pipeline, error)
	DeletePipeline(id string) (bool, error)

	RetrievePipelineStatistic(id string) (*PipelineStatistic, error)

	PopDatapoint(id string, consumerId string) ([]string, error)
	PushDatapoint(pipelineId string, value string) (bool, error)
}

type Pipeline struct {
	Id                string            `json:"id"`
	Name              string            `json:"name"`
	Description       string            `json:"description"`
	PipelineStatistic PipelineStatistic `json:"statistic"`
	Consumers         []Consumer        `json:"consumers"`
}

type Consumer struct {
	Id             string `json:"id"`
	UnreadElements int64  `json:"unread_elements"`
}

type PipelineStatisticElement struct {
	Date   string `json:"date"`
	Intake int64  `json:"intake"`
}

type PipelineStatistic struct {
	Today      int64                      `json:"today"`
	ChangeRate float64                    `json:"change_rate"`
	Statistics []PipelineStatisticElement `json:"statistics"`
}
