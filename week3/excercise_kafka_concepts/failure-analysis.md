Failure Analysis
Scenario
Your Kafka cluster has 3 brokers. The "orders" topic has 3 partitions with replication factor 2:

Partition	    Leader	    Follower
Partition 0	    Broker 1	Broker 2
Partition 1	    Broker 2	Broker 3
Partition 2	    Broker 3	Broker 1

Event: Broker 2 suddenly crashes.

Partition	    Leader	    Follower
Partition 0	    Broker 1	
Partition 1	   	Broker 3
Partition 2	    Broker 3	Broker 1


Analysis Questions
1. Which partitions are affected?

Partition 1 loses its leader, Partition 0 loses its folower

2. What happens to Partition 0?
Consider: Broker 2 was the follower for Partition 0. What is the impact?

If Broker 1 goes down, partition 0 will have no way to recover

3. What happens to Partition 1?
Consider: Broker 2 was the leader for Partition 1. Who becomes the new leader?

Broker 3

4. Can producers still send messages to all partitions? Why or why not?

Yes, broker 1 leads partition 0 and broker 3 leaders partition 1 and 2

5. What is the cluster's replication status after the failure?
Consider: How many replicas does each partition have now? Is the cluster at risk?

1 Replica for for partition 0 and 1, 2 replicas for partion 2
If broker 1 goes down partition 0 is lost
If broker 3 goes down partition 1 is lost
The cluster is at risk, any failure will result in data loss.

Bonus Question
If Broker 3 also fails immediately after Broker 2, what happens to Partition 1?

Partition 1 would have no follower to fall back on so it would be lost.