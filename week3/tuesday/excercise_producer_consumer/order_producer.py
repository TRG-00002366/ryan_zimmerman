"""
Order Producer
===============
Complete the TODO sections to send order messages to Kafka.

Prerequisites:
    pip install kafka-python
"""

from kafka import KafkaProducer
import json
import random
from datetime import datetime


def create_producer(bootstrap_servers: str = "localhost:9092"):
    """
    Create a KafkaProducer with JSON serialization.
    
    Create and return a KafkaProducer with:
    - key_serializer for string keys (encode to bytes)
    - value_serializer for JSON values
    - acks='all' for reliability
    """
    # Create and return the producer
    # Hint: 
    #   key_serializer=lambda k: k.encode('utf-8') if k else None
    #   value_serializer=lambda v: json.dumps(v).encode('utf-8')
    
    return KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )


def generate_order(order_number: int) -> dict:
    """
    Generate a sample order message.
    
    Create an order dictionary with:
    - order_id: formatted as "ORD-XXX" (e.g., "ORD-001")
    - customer_id: formatted as "CUST-XXX"
    - product: pick from a list of products
    - quantity: 1-5
    - price: based on product
    - timestamp: current ISO timestamp
    """
    products = [
        ("Laptop", 999.99),
        ("Mouse", 29.99),
        ("Keyboard", 79.99),
        ("Monitor", 349.99),
        ("Headphones", 149.99)
    ]
    
    # Create and return the order dictionary
    # Hint: Use order_number to create unique IDs and select products
    product = products[order_number%5]
    order = {
        "order_id": f"ORD-{str(order_number).rjust(3, "0")}",
        "customer_id": f"CUST-{str(random.randint(1, 999)).rjust(3, "0")}",
        "product": product[0],
        "quantity": random.randint(1, 5),
        "price": product[1],
        "timestamp": str(datetime.now())
    }
    
    return order


def send_orders(producer, topic: str, count: int = 10):
    """
    Send multiple orders to the Kafka topic.
    
    Complete this function to:
    1. Generate orders using generate_order()
    2. Send each order with order_id as the key
    3. Wait for acknowledgment and print partition/offset
    """
    print(f"Sending {count} orders to topic '{topic}'...")
    print("-" * 50)
    
    for i in range(1, count + 1):
        # Generate the order
        order = generate_order(i)
        
        # Send the order to Kafka
        # Use order["order_id"] as the key
        # Hint: future = producer.send(topic, key=..., value=...)
        future = producer.send(topic, key=order["order_id"], value=order )

        # Get the result and print partition/offset
        # Hint: metadata = future.get(timeout=10)
        #       print(f"  Order {order['order_id']} sent to partition {metadata.partition}, offset {metadata.offset}")
        
        metadata = future.get(timeout=10)
        print(f"  Order {order['order_id']} sent to partition {metadata.partition}, offset {metadata.offset}")
    
    print("-" * 50)
    print(f"All {count} orders sent successfully!")


def main():
    """Main function to run the order producer."""
    print("=" * 50)
    print("ORDER PRODUCER")
    print("=" * 50)
    
    topic = "orders-exercise"
    
    # Create producer
    producer = create_producer()
    
    if producer is None:
        print("\n[ERROR] Producer not created. Complete the TODO!")
        return
    
    # Send orders
    send_orders(producer, topic, count=10)
    
    # Cleanup
    producer.flush()
    producer.close()
    
    print("\n" + "=" * 50)
    print("PRODUCER COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    main()