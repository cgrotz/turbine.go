Turbine - Message Queue
=======================

Turbine is a Kafka like message queue that is based on Redis.

                   Current last element of pipeline
                   ❙
                   ⬇
    _____________________
    |1|2|3|3|4|5|6|7|8|9|
    ⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺
     ↑       ↑
     ❙       ❙
     ❙       Pointer for consumer 1
     Pointer for consumer 2
