Kafka Architecture Concepts - Answers
Part 1: Concept Questions
1. What is the difference between a topic and a partition?
Topics and Partitions are how Brokers organize kafka events. 
Topics are channels for related events which are made of topics organized by event key.

2. Why might you choose to have multiple partitions for a single topic?
Multiple consumers can access different partitions simultaneously

3. What is the role of ZooKeeper in a Kafka cluster? What is KRaft and how is it different?
ZooKeeper and Kraft are cluster amnagers for your broker. If the lead broker goes down they will manage replacing it with a follower.
ZooKeeper is older and has mostly been replaced by KRaft

4. Explain the difference between a leader replica and a follower replica.
A leader is the partition that is activly taking events from consumers. Followers periodically update themselves to match the leader. 

5. What does ISR stand for, and why is it important for fault tolerance?
In-Sync-Replicas, it is the set of follower replicas that match the leader. If the leader goes down, one of these will replace it.

6. How does Kafka differ from traditional message queues like RabbitMQ?
Kafka uses simple topics and partitions to handle events allowing it to take in far more data.

7. What is the publish-subscribe pattern, and how does Kafka implement it?
Publishers sends out data through channels, subscribers receive data from channels they are connected to.
Publisher does not know where data will end up and subscriber does not know where it came from.
Kafka has data sources that send data to kafka brokers which organize the events into topics and partitions.
Kafka consumers then read from these partitions.


8. What happens when a Kafka broker fails? How does the cluster recover?
The cluster manager will look for ISR's to replace the leading partitions.
The publishers/consumers are updated to send/receive to the new broker/partition.