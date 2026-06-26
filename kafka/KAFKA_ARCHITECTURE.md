# Kafka Architecture Setup

## Kiến trúc

```
Producers (P1-P3):  Currency, World Index, Products (requests)
Producer (P4):      Vietnam Index (Selenium scraper)
                    ↓
            Kafka Cluster (4 Topics)
                    ↓
        ┌───────────┴───────────┐
        ↓                       ↓
Consumer Group A        Consumer Group B
(Bronze Lakehouse)      (InfluxDB + Grafana)
   Delta/Parquet         Time-Series DB
```

## Topics

| Topic | Partitions | Replication | Producer | Consumer Group A | Consumer Group B |
|-------|-----------|-------------|----------|-----------------|-----------------|
| ticker.realtime | 4 | 2 | P1 (requests) | bronze | influxdb |
| index_world_realtime | 4 | 2 | P2 (requests) | bronze | influxdb |
| product_realtime | 4 | 2 | P3 (requests) | bronze | influxdb |
| index_realtime | 1 | 2 | P4 (selenium) | bronze | influxdb |

## Setup

### 1. Tạo Kafka Topics

```bash
docker exec -it kafka bash setup_topics.sh
```

Hoặc manual:
```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic ticker.realtime \
  --partitions 4 \
  --replication-factor 2
```

### 2. Chạy Producers

**P1-P3 (Unified):**
```bash
docker exec -it runner python kafka/producer_unified.py
```

**P4 (Selenium):**
```bash
docker exec -it runner python kafka/producer_index.py
```

### 3. Chạy Consumers

**Consumer Group A (Bronze):**
```bash
docker exec -it runner python kafka/consumer_bronze_group.py
```

**Consumer Group B (InfluxDB):**
```bash
docker exec -it runner python kafka/consumer_influxdb_group.py
```

## Monitoring

### Kafka Topics
```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list
```

### Consumer Groups
```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --list

# Chi tiết group
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group bronze_consumer_group
```

### InfluxDB
```bash
docker exec -it influxdb influx -username admin -password admin123

> USE grafana
> SHOW MEASUREMENTS
> SELECT * FROM currency LIMIT 5
```

## Scaling

### Thêm consumers (Consumer Group A)
```bash
# Terminal 2
docker exec -it runner python kafka/consumer_bronze_group.py
```

### Thêm consumers (Consumer Group B)
```bash
# Terminal 2
docker exec -it runner python kafka/consumer_influxdb_group.py
```

Kafka sẽ tự động distribute partitions.

## Troubleshooting

**Producer không gửi data:**
```bash
docker exec -it runner bash
python kafka/producer_unified.py  # Check logs
```

**Consumer lag cao:**
```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group bronze_consumer_group
```

**InfluxDB write fail:**
```bash
docker logs influxdb
```

## Files

- `producer_unified.py` - P1-P3 (requests)
- `producer_index.py` - P4 (selenium) [existing]
- `consumer_bronze_group.py` - Consumer Group A
- `consumer_influxdb_group.py` - Consumer Group B
- `setup_topics.sh` - Topic setup
