TASK 1


(.venv) ryan@DESKTOP-T1DV1V9:~/ryan_zimmerman$ docker exec kafka-broker kafka-topics --describe \
    --topic user-signups \
    --bootstrap-server localhost:9092
Topic: user-signups     TopicId: UroAKgpVTqyhrDzo8NSqwg PartitionCount: 2       ReplicationFactor: 1    Configs:
        Topic: user-signups     Partition: 0    Leader: 1       Replicas: 1     Isr: 1
        Topic: user-signups     Partition: 1    Leader: 1       Replicas: 1     Isr: 1

(.venv) ryan@DESKTOP-T1DV1V9:~/ryan_zimmerman$ docker exec kafka-broker kafka-topics --describe     --topic page-views     --b
ootstrap-server localhost:9092
Topic: page-views       TopicId: YDN0tvYXSsOR23nu6tlLnQ PartitionCount: 6       ReplicationFactor: 1    Configs:
        Topic: page-views       Partition: 0    Leader: 1       Replicas: 1     Isr: 1
        Topic: page-views       Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: page-views       Partition: 2    Leader: 1       Replicas: 1     Isr: 1
        Topic: page-views       Partition: 3    Leader: 1       Replicas: 1     Isr: 1
        Topic: page-views       Partition: 4    Leader: 1       Replicas: 1     Isr: 1
        Topic: page-views       Partition: 5    Leader: 1       Replicas: 1     Isr: 1

(.venv) ryan@DESKTOP-T1DV1V9:~/ryan_zimmerman$ docker exec kafka-broker kafka-topics --describe     --topic purchases     --bo
otstrap-server localhost:9092
Topic: purchases        TopicId: J54UnbeTQdWqEc5KG5Zstw PartitionCount: 3       ReplicationFactor: 1    Configs:
        Topic: purchases        Partition: 0    Leader: 1       Replicas: 1     Isr: 1
        Topic: purchases        Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: purchases        Partition: 2    Leader: 1       Replicas: 1     Isr: 1


TASK 2


==================================================
KAFKA TOPIC CREATION EXERCISE
==================================================

Creating 3 topics in batch...
  [SUCCESS] Created 3 topics

==================================================
TOPIC CREATION COMPLETE
==================================================


TASK 3

==================================================
KAFKA TOPIC INSPECTION
==================================================

Found 7 user topics:

Topic: price-changes
  Partitions: 2
  Partition 0: Leader=1, Replicas=[1], ISR=[1]
  Partition 1: Leader=1, Replicas=[1], ISR=[1]

Topic: purchases
  Partitions: 3
  Partition 0: Leader=1, Replicas=[1], ISR=[1]
  Partition 2: Leader=1, Replicas=[1], ISR=[1]
  Partition 1: Leader=1, Replicas=[1], ISR=[1]

Topic: inventory-updates
  Partitions: 4
  Partition 0: Leader=1, Replicas=[1], ISR=[1]
  Partition 2: Leader=1, Replicas=[1], ISR=[1]
  Partition 3: Leader=1, Replicas=[1], ISR=[1]
  Partition 1: Leader=1, Replicas=[1], ISR=[1]

Topic: page-views
  Partitions: 6
  Partition 0: Leader=1, Replicas=[1], ISR=[1]
  Partition 5: Leader=1, Replicas=[1], ISR=[1]
  Partition 4: Leader=1, Replicas=[1], ISR=[1]
  Partition 1: Leader=1, Replicas=[1], ISR=[1]
  Partition 2: Leader=1, Replicas=[1], ISR=[1]
  Partition 3: Leader=1, Replicas=[1], ISR=[1]

Topic: notifications
  Partitions: 3
  Partition 0: Leader=1, Replicas=[1], ISR=[1]
  Partition 1: Leader=1, Replicas=[1], ISR=[1]
  Partition 2: Leader=1, Replicas=[1], ISR=[1]

Topic: website_clicks
  Partitions: 3
  Partition 0: Leader=1, Replicas=[1], ISR=[1]
  Partition 2: Leader=1, Replicas=[1], ISR=[1]
  Partition 1: Leader=1, Replicas=[1], ISR=[1]

Topic: user-signups
  Partitions: 2
  Partition 0: Leader=1, Replicas=[1], ISR=[1]
  Partition 1: Leader=1, Replicas=[1], ISR=[1]

==================================================

