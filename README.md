Turbine - Message Queue
=======================

Turbine is a Kafka like message queue that is based on Redis. The purpose was to remove the hassle of running a Apache Zookeeper Cluster and allowing an easier setup in cloud environments like Cloudfoundry.

The default consumer model of turbine works without replication. This is due to the way that inside Turbine a consumer is just a pointer.

                   ┏ Current last element of pipeline
    ┏━┳━┳━┳━┳━┳━┳━┳┻┳━┳━┓
    ┃1┃2┃3┃3┃4┃5┃6┃7┃8┃9┃
    ┗┳┻━┻━┻━┻┳┻━┻━┻━┻━┻━┛
     ┃       ┗ Pointer for consumer 1
     ┗ Pointer for consumer 2

This way Turbine can achieve a high throughput while also allowing a at least once behavior for a clustered consumer.

# REST Interface #

## Pipelines [/api/v1/pipelines]
This resource represents all pipelines.

### Retrieve Pipelines [GET]
Retrieves all pipeline.

+ Response 200 (application/json)

        [{
          "id": "9d436fd2-fdeb-41e0-b110-09d31ddc2a50",
          "name": "Awesome Pipeline 1",
          "description": "Dynamically generated pipeline",
          "statistic": {
              "today": 10000,
              "change_rate": 10,
              "statistics": [
                  {
                      "date": "2015-02-17",
                      "intake": 9000
                  },
                  {
                      "date": "2015-02-16",
                      "intake": 8000
                  },
                  {
                      "date": "2015-02-15",
                      "intake": 7000
                  },
                  {
                      "date": "2015-02-14",
                      "intake": 6000
                  },
                  {
                      "date": "2015-02-13",
                      "intake": 5000
                  },
                  {
                      "date": "2015-02-12",
                      "intake": 4000
                  },
                  {
                      "date": "2015-02-11",
                      "intake": 3000
                  },
                  {
                      "date": "2015-02-10",
                      "intake": 2000
                  },
                  {
                      "date": "2015-02-09",
                      "intake": 1000
                  },
                  {
                      "date": "2015-02-08",
                      "intake": 0
                  }
              ]
          }
      }]

### Create Pipeline [POST]
Creates a new pipeline.

+ Request

        {
          "name": "Awesome Pipeline 1",
          "description": "Data of awesome sensors in swimming"
        }

+ Response 200 (application/json)

        {
            "id": "af8aae16-caaf-40b7-bc4e-2e1f8ceb5330",
            "name": "Awesome Pipeline 1",
            "description": "Data of awesome sensors in swimming pools"
        }

## Pipeline [/api/v1/pipelines/{id}]
This resource represents one particular pipeline identified by its *id*.

### Retrieve Pipeline [GET]
Retrieve a pipeline by its *id*.

+ Response 200 (application/json)

        {
          "id": "9d436fd2-fdeb-41e0-b110-09d31ddc2a50",
          "name": "Awesome Pipeline 1",
          "description": "Dynamically generated pipeline",
          "statistic":{
            "today": 10000,
            "change_rate": 10,
            "statistics": [{
              "date": "2015-02-17",
              "intake": 9000
            },{
              "date": "2015-02-16",
              "intake": 8000
            },{
              "date": "2015-02-15",
              "intake": 7000
            },{
              "date": "2015-02-14",
              "intake": 6000
            },{
              "date": "2015-02-13",
              "intake": 5000
            },{
              "date": "2015-02-12",
              "intake": 4000
            },{
              "date": "2015-02-11",
              "intake": 3000
            },{
              "date": "2015-02-10",
              "intake": 2000
            },{
              "date": "2015-02-09",
              "intake": 1000
            },{
              "date": "2015-02-08",
              "intake": 0
            }]
          },
          "consumers": [{
            "id": "consumer5",
            "unread_elements": 182793
          },{
            "id": "consumer6",
            "unread_elements": 183033
          },{
            "id": "consumer3",
            "unread_elements": 182163
          },{
            "id": "consumer8",
            "unread_elements": 183333
          },{
            "id": "consumer9",
            "unread_elements": 183553
          },{
            "id": "consumer7",
            "unread_elements": 183293
          },{
            "id": "consumer10",
            "unread_elements": 183693
          },{
            "id": "consumer2",
            "unread_elements": 181493
          },{
            "id": "consumer4",
            "unread_elements": 182593
          },{
            "id": "consumer1",
            "unread_elements": 180183
          }]
        }

### Update Pipeline [PUT]
Updates the name or description of a pipeline, identified by its *id*.

+ Request

        {
          "name": "Awesome Pipeline 1",
          "description": "Data of awesome sensors in swimming"
        }

+ Response 200 (application/json)

        {
            "id": "af8aae16-caaf-40b7-bc4e-2e1f8ceb5330",
            "name": "Awesome Pipeline 1",
            "description": "Data of awesome sensors in swimming pools"
        }


### Delete Pipeline [DELETE]
Delete a pipeline. **Warning:** This action **permanently** removes the pipeline from the system.
/
+ Response 204

## Datapoints [/api/v1/pipelines/{id}/datapoints]
This resource represents the stream of datapoints of one pipeline.

### Retrieve Datapoints [GET]
Retrieves the next 10 datapoints (or less) in the pipeline.

+ Response 200 (application/json)

        ["Event 1", "Event 2", "Event 3", "Event 4"]

### Push Datapoint [POST]
Pushes a new datapoint onto the pipleine.

+ Request

        Event 1

+ Response 204
