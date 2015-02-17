Turbine - Message Queue
=======================

Turbine is a Kafka like message queue that is based on Redis.

The default consumer model of turbine works without replication. This is due to the way that inside Turbine a consumer is just a pointer.

                   ┏ Current last element of pipeline
    ┏━┳━┳━┳━┳━┳━┳━┳┻┳━┳━┓
    ┃1┃2┃3┃3┃4┃5┃6┃7┃8┃9┃
    ┗┳┻━┻━┻━┻┳┻━┻━┻━┻━┻━┛
     ┃       ┗ Pointer for consumer 1
     ┗ Pointer for consumer 2

This way Turbine can achieve a high throughput while also allowing a at least once behavior for a clustered consumer.

#TODO#
* Add UDP support for datapoint storage

# Interface #

##REST##
TODO list of Rest Calls