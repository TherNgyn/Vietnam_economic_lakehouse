import os
import json
import time
import sys
import re
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
from influxdb import InfluxDBClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer-influxdb")

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "kafka:29092")
INFLUXDB_HOST  = os.getenv("INFLUXDB_HOST", "influxdb")
INFLUXDB_PORT  = int(os.getenv("INFLUXDB_PORT", 8086))
INFLUXDB_DB    = os.getenv("INFLUXDB_DB", "grafana")
INFLUXDB_USER  = os.getenv("INFLUXDB_ADMIN_USER", "admin")
INFLUXDB_PASS  = os.getenv("INFLUXDB_ADMIN_PASSWORD", "admin123")

TOPICS = ["ticker-realtime", "index-world-realtime", "product-realtime", "index-realtime"]

influx_client = None
for attempt in range(3):
    try:
        influx_client = InfluxDBClient(
            host=INFLUXDB_HOST, port=INFLUXDB_PORT,
            username=INFLUXDB_USER, password=INFLUXDB_PASS,
            database=INFLUXDB_DB,
        )
        influx_client.ping()
        dbs = influx_client.get_list_database()
        if not any(db['name'] == INFLUXDB_DB for db in dbs):
            influx_client.create_database(INFLUXDB_DB)
        log.info("InfluxDB connected: %s/%s", INFLUXDB_HOST, INFLUXDB_DB)
        break
    except Exception as e:
        if attempt < 2:
            time.sleep(3)
        else:
            log.error("Cannot connect InfluxDB: %s", e)
            sys.exit(1)

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='influxdb_consumer_group',
    auto_offset_reset='latest',
    enable_auto_commit=True,
)
log.info("Consumer ready, subscribed to: %s", TOPICS)

def to_ns(ts_str: str) -> int:
    ts_str = ts_str.replace("+00:00", "Z")
    if not ts_str.endswith("Z"):
        ts_str += "Z"
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000_000)

def clean_tags(tags: dict) -> dict:
    return {k: str(v) for k, v in tags.items() if v is not None and str(v).strip() != ""}

msg_count = 0
error_count = 0

for msg in consumer:
    data = msg.value
    msg_count += 1

    try:
        topic = msg.topic

        if topic == "ticker-realtime":
            measurement = "currency"
            tags = clean_tags({"symbol": data.get('symbol'), "asset_class": data.get('asset_class')})
            fields = {
                "price":      float(data['lastPrice']),
                "prev_close": float(data['previousClose']),
                "open":       float(data['open']),
                "high":       float(data['dayHigh']),
                "low":        float(data['dayLow']),
                "volume":     float(data['volume']),
            }
        elif topic == "index-world-realtime":
            measurement = "world_index"
            tags = clean_tags({"symbol": data.get('symbol'), "currency": data.get('currency')})
            fields = {
                "price":          float(data['price']),
                "open":           float(data['open']),
                "high":           float(data['high']),
                "low":            float(data['low']),
                "change":         float(data['change']),
                "change_percent": float(data['change_percent']),
                "volume":         float(data['volume']),
            }
        elif topic == "product-realtime":
            measurement = "product"
            tags = clean_tags({"symbol": data.get('symbol'), "name": data.get('name'), "unit": data.get('unit')})
            fields = {
                "price":          float(data['price']),
                "open":           float(data['open']),
                "high":           float(data['high']),
                "low":            float(data['low']),
                "change":         float(data['change']),
                "change_percent": float(data['change_percent']),
                "volume":         float(data['volume']),
            }
        elif topic == "index-realtime":
            measurement = "stock_index"
            raw_index = str(data.get('index', '')).strip()
            if re.match(r'^\d', raw_index) or ',' in raw_index:
                continue
            tags = clean_tags({"index_name": raw_index, "source": data.get('source')})
            raw_pct = data.get('percent')
            fields = {
                "price":     float(data['price']),
                "change":    float(data['change']),
                "percent":   float(raw_pct) if raw_pct is not None else 0.0,
                "volume":    float(data['volume']),
                "value":     float(data['value']),
                "advancers": int(data.get('advancers', 0)),
                "unchanged": int(data.get('unchanged', 0)),
                "decliners": int(data.get('decliners', 0)),
            }
        else:
            continue

        point = {
            "measurement": measurement,
            "tags":        tags,
            "fields":      fields,
            "time":        to_ns(data['timestamp']),
        }

        influx_client.write_points([point], time_precision='n')

        if msg_count % 20 == 0:
            log.info("[InfluxDB] %d records written", msg_count)

    except Exception as e:
        log.error("[ERROR] topic=%s err=%s: %s", msg.topic, type(e).__name__, e)
        error_count += 1
        if error_count > 20:
            time.sleep(2)
            error_count = 0