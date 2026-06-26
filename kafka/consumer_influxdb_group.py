"""
Consumer Group B: InfluxDB + Grafana
Ghi dữ liệu time-series vào InfluxDB để visualization
"""

import os
import json
import time
import sys
from kafka import KafkaConsumer
from influxdb import InfluxDBClient

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST", "influxdb")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", 8086))
INFLUXDB_DB = os.getenv("INFLUXDB_DB", "grafana")

TOPICS = ["ticker-realtime", "index-world-realtime", "product-realtime", "index-realtime"]

# Connect InfluxDB
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
        dbs = influx_client.get_list_database()
        if not any(db['name'] == INFLUXDB_DB for db in dbs):
            influx_client.create_database(INFLUXDB_DB)
        break
    except Exception as e:
        if attempt < max_retries - 1:
            time.sleep(3)
        else:
            print(f"ERROR: Cannot connect InfluxDB")
            sys.exit(1)

# Consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='influxdb_consumer_group',
    auto_offset_reset='latest',
    enable_auto_commit=True,
)

msg_count = 0
error_count = 0

for msg in consumer:
    data = msg.value
    msg_count += 1
    
    try:
        topic = msg.topic
        
        # Map topic to measurement
        if topic == "ticker.realtime":
            measurement = "currency"
            tags = {"symbol": data.get('symbol'), "asset_class": data.get('asset_class')}
            fields = {
                "price": float(data['lastPrice']),
                "prev_close": float(data['previousClose']),
                "open": float(data['open']),
                "high": float(data['dayHigh']),
                "low": float(data['dayLow']),
                "volume": float(data['volume']),
            }
        elif topic == "index_world_realtime":
            measurement = "world_index"
            tags = {"symbol": data.get('symbol'), "currency": data.get('currency')}
            fields = {
                "price": float(data['price']),
                "open": float(data['open']),
                "high": float(data['high']),
                "low": float(data['low']),
                "change": float(data['change']),
                "change_percent": float(data['change_percent']),
                "volume": float(data['volume']),
            }
        elif topic == "product_realtime":
            measurement = "product"
            tags = {"symbol": data.get('symbol'), "name": data.get('name'), "unit": data.get('unit')}
            fields = {
                "price": float(data['price']),
                "open": float(data['open']),
                "high": float(data['high']),
                "low": float(data['low']),
                "change": float(data['change']),
                "change_percent": float(data['change_percent']),
                "volume": float(data['volume']),
            }
        elif topic == "index_realtime":
            measurement = "stock_index"
            tags = {"index_name": data.get('index'), "source": data.get('source')}
            fields = {
                "price": float(data['price']),
                "change": float(data['change']),
                "percent": float(data['percent']) if data['percent'] else 0,
                "volume": float(data['volume']),
                "value": float(data['value']),
                "advancers": int(data.get('advancers', 0)),
                "unchanged": int(data.get('unchanged', 0)),
                "decliners": int(data.get('decliners', 0)),
            }
        else:
            continue
        
        point = {
            "measurement": measurement,
            "tags": tags,
            "fields": fields,
            "time": data['timestamp']
        }
        
        success = influx_client.write_points([point])
        if not success:
            error_count += 1
        
        if msg_count % 100 == 0:
            print(f"[InfluxDB] {msg_count} records processed")
        
    except Exception as e:
        error_count += 1
        if error_count > 10:
            time.sleep(1)
            error_count = 0

print("Consumer Group B (InfluxDB) stopped")
