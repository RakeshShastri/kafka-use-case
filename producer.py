import pandas as pd
import json
import time
from kafka import KafkaProducer

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Replace with your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize as JSON
)

# CSV file path
csv_file = './stock_data_new.csv'

# Kafka topic name
topic = 'stock_data'

# Read the CSV file using pandas
df = pd.read_csv(csv_file)

# Iterate over the rows of the DataFrame
for _, row in df.iterrows():
    # Convert the row to a dictionary
    row_dict = row.to_dict()
    
    # Send the dictionary as JSON to Kafka
    producer.send(topic, value=row_dict)
    print(f"Sent: {json.dumps(row_dict)}")  # For debugging purposes
    
    # Wait for 1 second before sending the next message
    time.sleep(1)

# Ensure all messages are sent
producer.flush()
producer.close()
