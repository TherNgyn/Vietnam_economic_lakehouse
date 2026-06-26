"""
Consumer Group A: Bronze Lakehouse (Raw Data Only)
- Stores raw records từ Kafka → Delta Lake trên MinIO
- NO OHLC calculation (def to Silver layer)
- Flush: time-based (60s) hoặc size-based (500 records)
"""

import os
import json
import sys
import pandas as pd
import time
from datetime import datetime
from kafka import KafkaConsumer
from deltalake import write_deltalake
from collections import defaultdict

# Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

# Topics mapping
TOPICS = {
    "ticker-realtime": "currency",
    "index-world-realtime": "world_index",
    "product-realtime": "product",
    "index-realtime": "vietnam_index",
}

# Flush settings
FLUSH_INTERVAL = 60  # seconds
FLUSH_SIZE = 500    # records

print(f"[INIT] Kafka: {KAFKA_BROKER}")
print(f"[INIT] MinIO: s3://{MINIO_BUCKET}")
print(f"[INIT] Flush: every {FLUSH_INTERVAL}s or {FLUSH_SIZE} records")
print(f"[INIT] Topics: {', '.join(TOPICS.keys())}")
print("-" * 60)

def get_consumer(group_id):
    return KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
    )

def run_consumer_group_bronze():
    consumer = get_consumer("bronze_consumer_group")
    consumer.subscribe(list(TOPICS.keys()))
    
    # Buffer by asset_class
    buffers = defaultdict(list)
    last_flush = time.time()
    msg_count = 0
    
    print("[Consumer] Bronze consumer started, buffering raw records...")
    
    for msg in consumer:
        msg_count += 1
        
        try:
            topic = msg.topic
            asset_class = TOPICS[topic]
            record = msg.value
            
            # Add metadata
            record['topic'] = topic
            record['asset_class'] = asset_class
            record['ingestion_timestamp'] = datetime.utcnow().isoformat()
            
            buffers[asset_class].append(record)
            
            # Check flush conditions
            time_elapsed = time.time() - last_flush
            total_buffered = sum(len(v) for v in buffers.values())
            
            should_flush = (
                time_elapsed >= FLUSH_INTERVAL or 
                total_buffered >= FLUSH_SIZE
            )
            
            if msg_count % 50 == 0:
                print(f"[Bronze] {msg_count} records | Buffered: {total_buffered} | Time: {time_elapsed:.0f}s")
            
            if should_flush:
                flush_buffers(buffers)
                buffers = defaultdict(list)
                last_flush = time.time()
                    
        except Exception as e:
            print(f"[ERROR] {e}")

def flush_buffers(buffers):
    """Write raw records to Delta tables"""
    if not any(buffers.values()):
        return
    
    summary = ' | '.join([f'{ac}:{len(records)}' for ac, records in buffers.items() if records])
    print(f"\n[Flush] {summary} records")
    
    for asset_class, records in buffers.items():
        if not records:
            continue
        
        try:
            df = pd.DataFrame(records)
            
            # Add processing date for partitioning
            df['processing_date'] = datetime.utcnow().strftime('%Y-%m-%d')
            
            delta_path = f"s3://{MINIO_BUCKET}/raw/{asset_class}"
            
            write_deltalake(
                delta_path,
                df,
                mode='append',
                partition_by=['processing_date'],
                storage_options=STORAGE_OPTIONS
            )
            print(f"  ✓ {asset_class}: {len(df)} raw records saved")
            
        except Exception as e:
            print(f"  ✗ {asset_class}: {e}")

if __name__ == "__main__":
    try:
        run_consumer_group_bronze()
    except KeyboardInterrupt:
        print("\n[Stop] Consumer stopped")
        sys.exit(0)
