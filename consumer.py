from kafka import KafkaConsumer
import json

# Kafka consumer configuration
consumer = KafkaConsumer(
    'stock_data',  # Topic to subscribe to
    bootstrap_servers=['localhost:9092'],  # Broker address
    auto_offset_reset='earliest',  # Read from the beginning if no offsets are present
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON
)

# Consume messages
for message in consumer:
    print(f"Received message: {message.value}")
