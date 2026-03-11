"""
Inspect Kafka Topics
=====================
Complete the TODO sections to list and describe topics.

Prerequisites:
    pip install kafka-python
"""

from kafka.admin import KafkaAdminClient


def list_topics(bootstrap_servers: str = "localhost:9092"):
    """
    List all user topics and display their details.
    
    Complete this function to:
    1. Create admin client
    2. List all topics (filter out internal ones starting with __)
    3. Describe each topic and print details
    """
    print("=" * 50)
    print("KAFKA TOPIC INSPECTION")
    print("=" * 50)
    
    admin_client =  KafkaAdminClient(
        bootstrap_servers = bootstrap_servers,
        client_id = "excercise1"
    )
    
    # Get list of all topics using admin_client.list_topics()
    all_topics = admin_client.list_topics()
    
    # Filter out internal topics
    user_topics = [t for t in all_topics if not t.startswith("__")]
    
    print(f"\nFound {len(user_topics)} user topics:\n")
    
    if not user_topics:
        print("No user topics found.")
        admin_client.close()
        return
    
    # Describe topics using admin_client.describe_topics(user_topics)
    topic_descriptions = admin_client.describe_topics(user_topics)
    
    # Display topic details
    for topic_info in topic_descriptions:
        topic_name = topic_info["topic"]
        partitions = topic_info["partitions"]
        
        print(f"Topic: {topic_name}")
        print(f"  Partitions: {len(partitions)}")
        
        for p in partitions:
            partition_id = p["partition"]
            leader = p["leader"]
            replicas = p["replicas"]
            isr = p["isr"]
            
            print(f"  Partition {partition_id}: Leader={leader}, Replicas={replicas}, ISR={isr}")
        
        print()
    
    admin_client.close()
    print("=" * 50)


if __name__ == "__main__":
    list_topics()