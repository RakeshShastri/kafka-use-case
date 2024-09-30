import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
import json
from datetime import datetime
import mysql.connector


def kafka_consumer():
    while True:
        consumer = KafkaConsumer('stock_topic', bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
        for message in consumer:
            print(message)
            print(message.value)
