import pandas as pd
import json
import time
from kafka import KafkaProducer

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  
)


csv_file = './stock_data_new.csv'


topic = 'stock_data'


df = pd.read_csv(csv_file)


for _, row in df.iterrows():
   
    row_dict = row.to_dict()
    
   
    producer.send(topic, value=row_dict)
    print(f"Sent: {json.dumps(row_dict)}")  
    
   
    time.sleep(2)


producer.flush()
producer.close()
