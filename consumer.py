from kafka import KafkaConsumer
import json
import mysql.connector
from datetime import datetime

# Kafka consumer configuration
consumer = KafkaConsumer(
    'stock_data',  
    bootstrap_servers=['localhost:9092'],  
    auto_offset_reset='latest',  
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
)

# Database connection configuration
db = mysql.connector.connect(
    host="localhost",      
    user="python-conn",     
    password="pythonconn",  
    database="stock_db" 
)

# DB Curosr
cursor = db.cursor()

# SQL insert query template
insert_query = """
INSERT INTO stock_data (idx, date, open, high, low, close, adj_close, volume, close_usd)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# SQL query to update True Range (TR) in the database
update_tr_query = """
UPDATE stock_data SET tr = %s WHERE idx = %s AND date = %s
"""

def validate_and_format_data(data):
    try:
        
        idx = data.get('Index', 'Unknown').strip()[:10]  # Truncate if longer than 10 chars

        #  Date Validation/type convsersion
        date_str = data.get('Date', None)
        try:
            
            date = datetime.strptime(date_str, '%Y-%b-%d').strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            print(f"Invalid date format: {date_str}. Skipping record.")
            return None  

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

        return (idx, date, open_value, high, low, close, adj_close, volume, close_usd)
    
    except Exception as e:
        print(f"Error processing data: {data}. Error: {e}")
        return None

def calculate_true_range(high, low, close, previous_close):
    try:
        tr = max(high - low, abs(high - previous_close), abs(low - previous_close))
        return tr
    except Exception as e:
        print(f"Error calculating True Range: {e}")
        return None

def get_previous_close(cursor, idx, date):
    query = """
    SELECT close FROM stock_data
    WHERE idx = %s AND date < %s
    ORDER BY date DESC LIMIT 1
    """
    cursor.execute(query, (idx, date))
    result = cursor.fetchone()
    return result[0] if result else None

# Consume messages from Kafka and insert into the database
for message in consumer:
    data = message.value

    
    validated_data = validate_and_format_data(data)

    if validated_data:
        idx, date, open_value, high, low, close, adj_close, volume, close_usd = validated_data

       
        try:
            cursor.execute(insert_query, validated_data)
            db.commit()
            print(f"Inserted into DB: {validated_data}")

            
            previous_close = get_previous_close(cursor, idx, date)

           
            if previous_close is not None:
                tr = calculate_true_range(high, low, close, previous_close)
                if tr is not None:
                    
                    cursor.execute(update_tr_query, (tr, idx, date))
                    db.commit()
                    print(f"Updated True Range for {idx} on {date}: {tr}")
            else:
                print(f"No previous close found for {idx} on {date}. TR calculation skipped.")
        
        except mysql.connector.Error as db_err:
            print(f"Database error: {db_err}. Failed to insert or update TR.")
        

cursor.close()
db.close()
