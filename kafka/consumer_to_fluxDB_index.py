import os
import json
import time
from kafka import KafkaConsumer
from influxdb import InfluxDBClient
import datetime
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "index-realtime"
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST", "influxdb")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", 8086))
INFLUXDB_DB = os.getenv("INFLUXDB_DB", "grafana")

print(f"Kafka: {KAFKA_BROKER}/{KAFKA_TOPIC}")
print(f"InfluxDB: {INFLUXDB_HOST}:{INFLUXDB_PORT}/{INFLUXDB_DB}")
print("-" * 60)

# Connect with retry logic
max_retries = 3
influx_client = None

for attempt in range(max_retries):
    try:
        influx_client = InfluxDBClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            username='admin',
            password='admin123',
            database=INFLUXDB_DB
        )
        
        influx_client.ping()
        print("✓ InfluxDB connected")
        
        # Create database if doesn't exist
        dbs = influx_client.get_list_database()
        if not any(db['name'] == INFLUXDB_DB for db in dbs):
            influx_client.create_database(INFLUXDB_DB)
            print(f"✓ Database '{INFLUXDB_DB}' created")
        else:
            print(f"✓ Database '{INFLUXDB_DB}' exists")
        
        break
    except Exception as e:
        print(f"✗ Connection attempt {attempt + 1}/{max_retries} failed: {e}")
        if attempt < max_retries - 1:
            time.sleep(3)
        else:
            print("✗ Failed to connect to InfluxDB")
            exit(1)

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest', 
    group_id='influxdb_consumer',
    consumer_timeout_ms=60000  
)

print(f"Listening to {KAFKA_TOPIC}...\n")

msg_count = 0
error_count = 0

for msg in consumer:
    data = msg.value
    msg_count += 1
    
    try:
        if 'index' in data: 
            point = {
                "measurement": "stock_index",
                "tags": {
                    "index_name": data.get('index'),
                    "source": data.get('source'),
                    "unit": data.get('unit'),
                },
                "fields": {
                    "price": float(data['price']),
                    "change": float(data['change']),
                    "percent": float(data['percent']) if data['percent'] else 0,
                    "volume": float(data['volume']),
                    "value": float(data['value']),
                    "advancers": int(data.get('advancers', 0)),
                    "unchanged": int(data.get('unchanged', 0)),
                    "decliners": int(data.get('decliners', 0)),
                },
                "time": data['timestamp']
            }
 
            success = influx_client.write_points([point])
      
            if success:
                print(f"[{msg_count}] {data['timestamp']}-{data['index']}: {data['price']} Points → InfluxDB")
                error_count = 0  
            else:
                print(f"[{msg_count}] {data['timestamp']}-{data['index']}: Failed to write")
                error_count += 1
        else:
            print(f"[{msg_count}] Unrecognized data format: {data}")
            continue
        
    except Exception as e:
        error_count += 1
        print(f"✗ [{msg_count}] Error: {e}")

        if error_count > 5:
            try:
                print("Attempting to reconnect to InfluxDB...")
                influx_client = InfluxDBClient(
                    host=INFLUXDB_HOST,
                    port=INFLUXDB_PORT,
                    username='admin',
                    password='admin123',
                    database=INFLUXDB_DB
                )
                influx_client.ping()
                print("✓ Reconnected to InfluxDB")
                error_count = 0
            except Exception as reconnect_error:
                print(f"✗ Reconnection failed: {reconnect_error}")