import os
import json
import sys
import time
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer
from deltalake import write_deltalake
from collections import defaultdict

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

TOPICS = {
    "ticker-realtime": "currency",
    "index-world-realtime": "world_index",
    "product-realtime": "product",
    "index-realtime": "vietnam_index",
}

FLUSH_INTERVAL = 60
FLUSH_SIZE = 500

print(f"[INIT] Kafka: {KAFKA_BROKER}")
print(f"[INIT] MinIO: s3://{MINIO_BUCKET}")
print(f"[INIT] Flush: every {FLUSH_INTERVAL}s or {FLUSH_SIZE} records")
print(f"[INIT] Topics: {', '.join(TOPICS.keys())}")
print("-" * 60)


def get_consumer(group_id):
    return KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=500,
    )


def flush_buffers(buffers):
    if not any(buffers.values()):
        return True

    summary = " | ".join(
        [f"{asset_class}:{len(records)}" for asset_class, records in buffers.items() if records]
    )

    print(f"\n[Flush] {summary} records")

    success = True

    for asset_class, records in buffers.items():
        if not records:
            continue

        try:
            df = pd.DataFrame(records)
            df["processing_date"] = datetime.utcnow().strftime("%Y-%m-%d")

            delta_path = f"s3://{MINIO_BUCKET}/raw/{asset_class}"

            write_deltalake(
                delta_path,
                df,
                mode="append",
                partition_by=["processing_date"],
                storage_options=STORAGE_OPTIONS,
            )

            print(f"  ✓ {asset_class}: {len(df)} raw records saved")

        except Exception as exc:
            success = False
            print(f"  ✗ {asset_class}: {exc}")

    return success


def run_consumer_group_bronze():
    consumer = get_consumer("bronze_consumer_group")
    consumer.subscribe(list(TOPICS.keys()))

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

            record["topic"] = topic
            record["asset_class"] = asset_class
            record["ingestion_timestamp"] = datetime.utcnow().isoformat()

            buffers[asset_class].append(record)

            time_elapsed = time.time() - last_flush
            total_buffered = sum(len(records) for records in buffers.values())

            should_flush = (
                time_elapsed >= FLUSH_INTERVAL
                or total_buffered >= FLUSH_SIZE
            )

            if msg_count % 50 == 0:
                print(
                    f"[Bronze] {msg_count} records | "
                    f"Buffered: {total_buffered} | "
                    f"Time: {time_elapsed:.0f}s"
                )

            if should_flush:
                flushed = flush_buffers(buffers)

                if flushed:
                    consumer.commit()
                    buffers = defaultdict(list)
                    last_flush = time.time()
                    print("[Commit] Kafka offsets committed after successful Bronze flush")
                else:
                    print("[Retry] Bronze flush failed, buffers kept, offsets not committed")
                    time.sleep(5)

        except Exception as exc:
            print(f"[ERROR] {exc}")


if __name__ == "__main__":
    try:
        run_consumer_group_bronze()
    except KeyboardInterrupt:
        print("\n[Stop] Consumer stopped")
        sys.exit(0)