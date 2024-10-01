from kafka import KafkaConsumer
import json
import mysql.connector
from datetime import datetime

# Kafka consumer configuration
consumer = KafkaConsumer(
    'stock_data',  # Kafka topic name
    bootstrap_servers=['localhost:9092'],  # Kafka broker address
    auto_offset_reset='latest',  # Start from the beginning of the topic
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON
)

# Database connection configuration
db = mysql.connector.connect(
    host="localhost",       # Change to your DB host
    user="root",     # Change to your DB user
    password="root",  # Change to your DB password
    database="stock_db"  # Change to your DB name
)

# Create a cursor object to execute SQL queries
cursor = db.cursor()

# SQL insert query template
insert_query = """
INSERT INTO stock_data (idx, date, open, high, low, close, adj_close, volume, close_usd)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def validate_and_format_data(data):
    try:
        # Extract and validate idx (index)
        idx = data.get('Index', 'Unknown').strip()[:10]  # Truncate if longer than 10 chars

        # Validate and format date
        date_str = data.get('Date', None)
        try:
            # Attempt to convert date into 'YYYY-MM-DD' format
            date = datetime.strptime(date_str, '%Y-%b-%d').strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            print(f"Invalid date format: {date_str}. Skipping record.")
            return None  # Skip invalid date rows

        # Convert and validate numeric fields
        def safe_float(value, default=0.0):
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        open_value = safe_float(data.get('Open'))
        high = safe_float(data.get('High'))
        low = safe_float(data.get('Low'))
        close = safe_float(data.get('Close'))
        adj_close = safe_float(data.get('Adj Close'))
        volume = safe_float(data.get('Volume'))
        close_usd = safe_float(data.get('CloseUSD'))

        # Return the validated and formatted values
        return (idx, date, open_value, high, low, close, adj_close, volume, close_usd)
    
    except Exception as e:
        print(f"Error processing data: {data}. Error: {e}")
        return None

# Consume messages from Kafka and insert into the database
for message in consumer:
    data = message.value

    # Validate and format the received data
    validated_data = validate_and_format_data(data)

    # Insert into the database if the data is valid
    if validated_data:
        try:
            cursor.execute(insert_query, validated_data)
            db.commit()  # Commit the transaction
            print(f"Inserted into DB: {validated_data}")
        except mysql.connector.Error as db_err:
            print(f"Database error: {db_err}. Failed to insert: {validated_data}")

# Close the database connection and cursor when done
cursor.close()
db.close()
