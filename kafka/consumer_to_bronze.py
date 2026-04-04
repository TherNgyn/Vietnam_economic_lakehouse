import os
import pandas as pd
import json
from kafka import KafkaConsumer
from datetime import date
from deltalake import write_deltalake
from collections import defaultdict

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ticker.realtime")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

print(f"Kafka: {KAFKA_BROKER}/{KAFKA_TOPIC}")
print(f"MinIO: s3://{MINIO_BUCKET}")
print("-" * 60)

def consume_daily_aggregates(target_date=None):
    if target_date is None:
        target_date = date.today().strftime('%Y-%m-%d')
    
    print(f"Consuming Kafka for {target_date}...")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest', 
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='bronze_consumer_v2',
        max_poll_records=1000,
        consumer_timeout_ms=10000 
    )

    data_by_class = defaultdict(list)
    msg_count = 0
    
    for msg in consumer:
        tick = msg.value
        ts = tick['timestamp'][:10]
        
        if ts != target_date:
            print(f"  Skip {ts} (looking for {target_date})")
            continue
        
        data_by_class[tick['asset_class']].append(tick)
        msg_count += 1
        print(f"[{msg_count}] {tick['symbol']} @ {tick['timestamp']}")
    
    consumer.close()
    
    print(f"\nConsumed {msg_count} messages\n")
    
    if msg_count == 0:
        print("No data found")
        return

    for asset_class, ticks in data_by_class.items():
        df = pd.DataFrame(ticks)
        
        if df.empty:
            print(f"  No data for {asset_class}")
            continue
        
        print(f"Processing {asset_class}...")
        
        rows = []
        for symbol, grp in df.groupby('symbol'):
            try:
                grp = grp.sort_values('timestamp')
                first = grp.iloc[0]
                last = grp.iloc[-1]
                
                row = {
                    'date': target_date,
                    'symbol': symbol,
                    'asset_class': asset_class,
                    'unit': first.get('unit', ''),
                    'open': float(first.get('open', 0)),
                    'high': float(grp['dayHigh'].max()),    
                    'low': float(grp['dayLow'].min()),       
                    'close': float(last['lastPrice']),        
                    'volume': float(last.get('volume', 0)),
                    'prev_close': float(first['previousClose']),  
                    'source': 'kafka_realtime',
                }
                rows.append(row)
                print(f"  {symbol}: O:{row['open']} H:{row['high']} L:{row['low']} C:{row['close']}")
            except Exception as e:
                print(f" Error processing {symbol}: {e}")
                continue
        
        if not rows:
            print(f"  No valid rows")
            continue
        
        result_df = pd.DataFrame(rows)
        path = f"s3://{MINIO_BUCKET}/realtime/{asset_class}"
        
        try:
            write_deltalake(
                path, 
                result_df, 
                mode='append', 
                partition_by=['symbol'],
                storage_options=STORAGE_OPTIONS
            )
            print(f" Saved {len(result_df)} rows to {path}\n")
        except Exception as e:
            print(f" Error: {e}\n")

if __name__ == "__main__":
    try:
        consume_daily_aggregates()
    except KeyboardInterrupt:
        print("\nStopped.")