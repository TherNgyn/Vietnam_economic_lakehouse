import os
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from influxdb import InfluxDBClient

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ticker.realtime")
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST", "influxdb")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", 8086))
INFLUXDB_DB = os.getenv("INFLUXDB_DB", "grafana")

print(f"Kafka: {KAFKA_BROKER}")
print(f"InfluxDB: {INFLUXDB_HOST}:{INFLUXDB_PORT}")
print("-" * 50)

kafka_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

influx_client = None
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
except Exception as e:
    print(f"✗ InfluxDB skipped: {e}")
    influx_client = None

# Test data: giả lập nến 5 mút
test_ticks = [
    {'open': 26300, 'high': 26350, 'low': 26290, 'close': 26320, 'volume': 1000},
    {'open': 26320, 'high': 26380, 'low': 26310, 'close': 26360, 'volume': 1500},
    {'open': 26360, 'high': 26400, 'low': 26350, 'close': 26390, 'volume': 1200},
    {'open': 26390, 'high': 26420, 'low': 26370, 'close': 26405, 'volume': 1100},
    {'open': 26405, 'high': 26430, 'low': 26395, 'close': 26415, 'volume': 900},
    {'open': 26415, 'high': 26450, 'low': 26410, 'close': 26440, 'volume': 1300},
    {'open': 26440, 'high': 26480, 'low': 26430, 'close': 26470, 'volume': 1600},
    {'open': 26470, 'high': 26500, 'low': 26460, 'close': 26490, 'volume': 1400},
    {'open': 26490, 'high': 26520, 'low': 26480, 'close': 26510, 'volume': 1200},
    {'open': 26510, 'high': 26540, 'low': 26500, 'close': 26530, 'volume': 1100},
    {'open': 26530, 'high': 26550, 'low': 26510, 'close': 26540, 'volume': 900},
    {'open': 26540, 'high': 26560, 'low': 26520, 'close': 26550, 'volume': 1000},
    {'open': 26550, 'high': 26570, 'low': 26530, 'close': 26560, 'volume': 1200},
    {'open': 26560, 'high': 26580, 'low': 26540, 'close': 26570, 'volume': 1500},
    {'open': 26570, 'high': 26600, 'low': 26560, 'close': 26590, 'volume': 1800},
    {'open': 26590, 'high': 26620, 'low': 26580, 'close': 26610, 'volume': 1400},
    {'open': 26610, 'high': 26630, 'low': 26600, 'close': 26620, 'volume': 1100},
    {'open': 26620, 'high': 26650, 'low': 26610, 'close': 26640, 'volume': 1300},
    {'open': 26640, 'high': 26670, 'low': 26630, 'close': 26660, 'volume': 1600},
    {'open': 26660, 'high': 26680, 'low': 26650, 'close': 26670, 'volume': 1200},
]

def send_ticks():
    print("Sending test data ticks...\n")
    
    for i, tick_data in enumerate(test_ticks):
        tick = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'symbol': 'USDVND',
            'asset_class': 'currency',
            'unit': 'VND',
            'last_price': float(tick_data['close']),
            'prev_close': float(test_ticks[i-1]['close']) if i > 0 else 26300,
            'open': float(tick_data['open']),
            'day_high': float(tick_data['high']),
            'day_low': float(tick_data['low']),
            'volume': float(tick_data['volume']),
        }
        
        # Push to Kafka
        try:
            kafka_producer.send(KAFKA_TOPIC, value=tick)
            print(f"✓ [{i+1}] Close: {tick_data['close']} → Kafka", end="")
        except Exception as e:
            print(f"✗ [{i+1}] Kafka error: {e}")
            continue
        
        # Push to InfluxDB
        if influx_client:
            try:
                point = {
                    "measurement": "ticker",
                    "tags": {
                        "symbol": "USDVND",
                        "asset_class": "currency",
                    },
                    "fields": {
                        "open": tick['open'],
                        "high": tick['day_high'],
                        "low": tick['day_low'],
                        "close": tick['last_price'],
                        "volume": tick['volume'],
                        "change": round(tick['last_price'] - tick['prev_close'], 4),
                        "change_pct": round((tick['last_price'] - tick['prev_close']) / tick['prev_close'] * 100, 2),
                    },
                    "time": tick['timestamp']
                }
                influx_client.write_points([point])
                print(f" + InfluxDB ✓")
            except Exception as e:
                print(f" (InfluxDB error: {e})")
        else:
            print()
        
        time.sleep(0.5)
    
    kafka_producer.flush()
    print("\n" + "-" * 50)
    print("✓ Done! 5 ticks sent")

if __name__ == "__main__":
    send_ticks()