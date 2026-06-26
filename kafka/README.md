# Vietnam Economic Lakehouse - Kafka Architecture

## Overview

Hệ thống **real-time data pipeline** xử lý dữ liệu kinh tế Việt Nam:
- 💱 Tỷ giá USD/VND (yfinance)
- 📈 Chỉ số chứng khoán thế giới (yfinance)
- ⛽ Giá hàng hóa (yfinance)
- 🇻🇳 Chỉ số chứng khoán Việt Nam (web scraper)

```
┌─────────────────────────┐
│     PRODUCERS (P1-P3)   │
│    Unified Container    │  ← Currency, World Index, Product
│   (3 daemon threads)    │     Every 5 minutes
└────────────┬────────────┘
             │
┌────────────┴────────────┐
│    Kafka Cluster        │
│  4 Topics (P=4,1 / RF=1)│
│ - ticker-realtime (P=4) │
│ - index-world-realtime  │
│ - product-realtime (P=4)│
│ - index-realtime (P=1)  │
└────────────┬────────────┘
             │
     ┌───────┴───────┐
     │               │
┌────▼──────┐   ┌──────▼────┐
│ Consumer  │   │ Consumer  │
│ Group A   │   │ Group B   │
│ Bronze DL │   │ InfluxDB  │
│  (JSONL)  │   │ (Grafana) │
└───────────┘   └───────────┘
```

## Topics Design

| Topic | Partitions | Replication | Producer | Frequency | Consumer A | Consumer B |
|-------|-----------|-------------|----------|-----------|-----------|-----------|
| ticker-realtime | 4 | 1 | P1 (Currency) | 5m | ✓ Bronze | ✓ InfluxDB |
| index-world-realtime | 4 | 1 | P2 (World Index) | 5m | ✓ Bronze | ✓ InfluxDB |
| product-realtime | 4 | 1 | P3 (Product) | 5m | ✓ Bronze | ✓ InfluxDB |
| index-realtime | 1 | 1 | P4 (Vietnam Index) | 1m | ✓ Bronze | ✓ InfluxDB |

**Note:**
- RF=1 (Replication Factor) for development (single broker)
- For production with 3+ brokers: Change to RF=2 or RF=3

**Rationale:**
- P=4 cho API sources (yfinance requests) - high volume, reliable
- P=1 cho web scraper - low frequency, potential failures
- Isolated containers: P1-P3 crash không ảnh hưởng P4

## Setup & Deployment

### 1. Build Docker Image

```bash
docker-compose build
```

**Packages installed:**
- yfinance (stock prices API)
- selenium (web scraping)
- kafka-python (Kafka client)
- influxdb (time-series DB)
- pyyaml (config parser)
- requests, numpy (data processing)

### 2. Start Kafka Cluster

```bash
docker-compose up -d kafka zookeeper
```

Wait 30 seconds for Kafka to start.

### 3. Create Topics

**Option A: Execute setup script**
```bash
docker exec kafka bash /opt/bitnami/kafka/scripts/setup_topics.sh
```

**Option B: Manual**
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create \
  --topic ticker.realtime --partitions 4 --replication-factor 1 \
  --config retention.ms=604800000

docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create \
  --topic index_world_realtime --partitions 4 --replication-factor 1 \
  --config retention.ms=604800000

docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create \
  --topic product_realtime --partitions 4 --replication-factor 1 \
  --config retention.ms=604800000

docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create \
  --topic index_realtime --partitions 1 --replication-factor 1 \
  --config retention.ms=604800000
```

**Note:** RF=1 for single broker. For production with 3+ brokers, change to `--replication-factor 2`

### 4. Start Producers

```bash
# Unified producer (P1, P2, P3)
docker run -d --name producer_p1_p3 \
  --network vietnam_lake \
  -e KAFKA_BROKER=kafka:29092 \
  vietnam-lakehouse:latest python kafka/producer_unified.py

# Vietnam indices producer (P4)
docker run -d --name producer_p4 \
  --network vietnam_lake \
  -e KAFKA_BROKER=kafka:29092 \
  vietnam-lakehouse:latest python kafka/producer_index.py
```

### 5. Start Consumers

```bash
# Consumer Group A (Bronze Data Lake)
docker run -d --name consumer_bronze \
  --network vietnam_lake \
  -e KAFKA_BROKER=kafka:29092 \
  -v bronze_data:/data/bronze \
  vietnam-lakehouse:latest python kafka/consumer_bronze_group.py

# Consumer Group B (InfluxDB)
docker run -d --name consumer_influxdb \
  --network vietnam_lake \
  -e KAFKA_BROKER=kafka:29092 \
  -e INFLUXDB_HOST=influxdb \
  vietnam-lakehouse:latest python kafka/consumer_influxdb_group.py
```

### 6. Verify Setup

```bash
# View all topics
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# View consumer groups
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list

# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group bronze_consumer_group --describe

# View InfluxDB data
docker exec influxdb influx -username admin -password admin123
> USE vietnam_economic
> SELECT * FROM currency LIMIT 5
```

## Data Flow Architecture

### Bronze Layer (Raw Data Lake)
```
Real-time Producers → Kafka Topics → Consumer Group A (Bronze)
                                          ↓
                                    JSONL Files
                                 /data/bronze/
                              {date}/{symbol}/
```

**Mục đích:** Lưu trữ 100% dữ liệu gốc (raw ticks)
- Không tính toán gì, chỉ lưu JSON đúng từ Kafka
- Tổ chức theo: {data_type}/{date}/{symbol}
- Dễ tính OHLC sau (Silver layer)

### Silver Layer (Processed Data) - *Template*

**Luồng xử lý:**
```python
# 1. Đọc tất cả ticks từ 1 file bronze
bronze_file = "/data/bronze/currency/2024-01-15/USDVND_X/data.jsonl"
ticks = [json.loads(line) for line in open(bronze_file)]

# 2. Tính OHLC
ohlc = {
    'date': '2024-01-15',
    'symbol': 'USDVND_X',
    'open': ticks[0]['price'],
    'high': max(t['price'] for t in ticks),
    'low': min(t['price'] for t in ticks),
    'close': ticks[-1]['price'],
    'volume': len(ticks)
}

# 3. Append vào table Silver
# INSERT INTO silver.currency_ohlc VALUES (...)
```

**Output:** Parquet/Delta table với OHLC
- Cặp (date, symbol) là unique key
- Dễ join với table khác để tính indicator

---

**Currency (P1):**
```json
{
  "symbol": "USDVND=X",
  "timestamp": "2024-01-15T10:30:00Z",
  "price": 24150.5,
  "change": 0.05,
  "source": "yfinance"
}
```

**World Index (P2):**
```json
{
  "symbol": "^GSPC",
  "name": "S&P 500",
  "timestamp": "2024-01-15T10:30:00Z",
  "price": 4810.25,
  "high": 4820.0,
  "low": 4800.0,
  "source": "yfinance"
}
```

**Product (P3):**
```json
{
  "symbol": "CL=F",
  "name": "WTI Crude Oil",
  "timestamp": "2024-01-15T10:30:00Z",
  "price": 78.45,
  "volume": 50000000,
  "source": "yfinance"
}
```

**Vietnam Index (P4):**
```json
{
  "name": "VNINDEX",
  "symbol": "VNINDEX",
  "timestamp": "2024-01-15T10:30:00Z",
  "price": 1280.5,
  "change": 0.15,
  "source": "vikkibanks"
}
```

### Consumer Group A → Bronze Layer

**Directory Structure (theo phiên/tick):**
```
/data/bronze/
├── currency/2024-01-15/USDVND_X/data.jsonl
├── world_index/2024-01-15/GSPC/data.jsonl
├── world_index/2024-01-15/FCHI/data.jsonl
├── product/2024-01-15/CL_F/data.jsonl
└── vietnam_index/2024-01-15/VNINDEX/data.jsonl
```

**File Format (JSONL):**
```json
{"symbol":"USDVND=X","timestamp":"2024-01-15T10:30:00Z","price":24150.5,"change":0.05,"source":"yfinance"}
{"symbol":"USDVND=X","timestamp":"2024-01-15T10:35:00Z","price":24151.0,"change":0.08,"source":"yfinance"}
```

**Silver Layer sẽ:**
1. Đọc từng file Bronze: `currency/2024-01-15/USDVND_X/data.jsonl`
2. Tính OHLC (Open, High, Low, Close) theo phiên giao dịch
3. Append vào table đã xử lý: `silver.currency_ohlc`

### Consumer Group B → InfluxDB

**Measurements:**
- `currency`: Fields (price, change), Tags (symbol, source)
- `world_index`: Fields (price, high, low, change), Tags (symbol, name, source)
- `product`: Fields (price, volume, change), Tags (symbol, name, source)
- `stock_index`: Fields (price, change), Tags (symbol, name, source)

**Query Examples:**
```sql
SELECT * FROM currency WHERE time > now() - 1h
SELECT MEAN(price) FROM world_index WHERE symbol = '^GSPC' GROUP BY time(5m)
SELECT DISTINCT "symbol" FROM product
```

## Scaling Strategy

### Add More Consumers (Horizontal Scaling)

```bash
# Add 2nd consumer in Group A (bronze)
docker run -d --name consumer_bronze_2 \
  --network vietnam_lake \
  -e KAFKA_BROKER=kafka:29092 \
  -v bronze_data:/data/bronze \
  vietnam-lakehouse:latest python kafka/consumer_bronze_group.py

# Kafka auto-distributes partitions
# Examples: If P=4, partition 0,1→consumer_bronze, partition 2,3→consumer_bronze_2
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group bronze_consumer_group --describe
```

### Monitor Consumer Lag

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group bronze_consumer_group --describe

# Expected output:
# GROUP                    TOPIC              PARTITION  CURRENT-OFFSET   LOG-END-OFFSET  LAG
# bronze_consumer_group    ticker.realtime    0          1000             1000            0
# bronze_consumer_group    ticker.realtime    1          950              950             0
...
```

**Lag = 0**: Consumer caught up 🟢
**Lag > 1000**: Consumer lagging behind 🔴

## Troubleshooting

### Kafka Cluster Not Starting

```bash
# Check Kafka logs
docker logs kafka

# Common issues:
# - Port 9092 already in use
# - Not enough disk space
# - Zookeeper not running first

# Solution:
docker-compose down -v  # Remove volumes
docker-compose up -d kafka zookeeper
```

### Producer Not Sending Data

```bash
# Check producer logs
docker logs producer_p1_p3
docker logs producer_p4

# If error "KAFKA_BROKER not set":
docker run -d -e KAFKA_BROKER=kafka:29092 ...

# If error "Connection refused":
# Ensure kafka container is running: docker ps | grep kafka
# Wait 30 seconds for Kafka startup
```

### Consumer Not Receiving Data

```bash
# Check topic has data
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic ticker.realtime --from-beginning --max-messages 5

# Check consumer group was created
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list

# Check consumer logs
docker logs consumer_bronze
docker logs consumer_influxdb
```

### InfluxDB Not Recording Data

```bash
# Check InfluxDB is running
docker ps | grep influxdb

# Check connectivity
docker exec consumer_influxdb ping influxdb

# Test InfluxDB connection
docker exec influxdb influx -username admin -password admin123

# Check database/measurements created
> SHOW DATABASES
> SHOW MEASUREMENTS IN vietnam_economic
```

## Env Variables

**Producers & Consumers:**
```bash
KAFKA_BROKER=kafka:29092              # Local Kafka for testing
# KAFKA_BROKER=kafka.example.com:9092 # Remote for production

INFLUXDB_HOST=influxdb               # InfluxDB hostname
INFLUXDB_PORT=8086                   # Default InfluxDB port
INFLUXDB_USER=admin                  # InfluxDB username
INFLUXDB_PASSWORD=admin123           # InfluxDB password
INFLUXDB_DB=vietnam_economic         # Target database

BRONZE_PATH=/data/bronze             # Bronze layer path
```

## Performance Tuning

### Increase Producer Throughput

```python
# In producer_unified.py, adjust intervals
CURRENCY_INTERVAL = 60      # Reduce to 60s (was 300s)
WORLD_INDEX_INTERVAL = 60   # Reduce to 60s
PRODUCT_INTERVAL = 60       # Reduce to 60s

# Or batch multiple records per message (not recommended for real-time)
```

### Increase Consumer Throughput

```python
# In consumer_bronze_group.py, add batching
from pathlib import Path
import json

buffer = []
flush_count = 100

for msg in consumer:
    data = json.loads(msg.value.decode())
    buffer.append(data)
    if len(buffer) >= flush_count:
        # Flush all to file
        file.writelines([json.dumps(record) + '\n' for record in buffer])
        buffer.clear()
```

### Batch InfluxDB Writes

```python
# In consumer_influxdb_group.py, add batching
batch_points = []
batch_size = 100

while True:
    msg = consumer.poll(timeout_ms=1000)
    for tp, messages in msg.items():
        for message in messages:
            point = convert_to_point(message.value)
            batch_points.append(point)
            
            if len(batch_points) >= batch_size:
                client.write_points(batch_points)
                batch_points.clear()
```

## Monitoring Dashboard

View Kafka metrics:
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
docker exec kafka kafka-broker-api-versions --bootstrap-server kafka:9092
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic ticker.realtime
```

Monitor Python outputs:
```bash
docker logs -f producer_p1_p3
docker logs -f consumer_bronze
```

Integration with Grafana:
```bash
# Connect Grafana to InfluxDB
# Data Sources → Add → InfluxDB
# URL: http://influxdb:8086
# Database: vietnam_economic
# Username/Password: admin / admin123

# Create dashboard:
# Query: SELECT "price" FROM "currency" WHERE time > now() - 24h GROUP BY time(5m)
```

## File Structure

```
kafka/
├── producer.py                  # Original currency producer (P1 only)
├── producer_index.py            # Vietnam index scraper (P4)
├── producer_index_w.py          # World index producer (P2)
├── producer_product.py          # Commodity producer (P3)
├── producer_unified.py          # Unified P1+P2+P3
├── consumer_bronze_group.py     # Consumer Group A (Bronze)
├── consumer_influxdb_group.py   # Consumer Group B (InfluxDB)
├── consumer_to_fluxDB_index.py  # Old single-topic consumer
├── consumer_to_bronze.py        # Old single-topic consumer
├── setup_topics.sh              # Topic creation script
├── kafka_setup.sh               # Full setup orchestration
├── kafka_monitoring.py          # System monitoring script
├── KAFKA_ARCHITECTURE.md        # Architecture documentation
└── README.md                    # This file

historical_dataset/             # Reference datasets
├── cpi.csv
├── gasoline_prices.csv
├── money_supply_m2.csv
├── currency/
│   ├── CNY_VND.csv
│   └── USD_VND.csv
├── product_world/
│   └── crude_oil_WTI.csv
└── stock_index/
    ├── HNX_index.csv
    ├── HNX30_index.csv
    ├── UPCOME_index.csv
    ├── VN30_index.csv
    └── VN_index.csv

/data/bronze/                   # Bronze layer (runtime)
├── currency/
├── world_index/
├── product/
└── vietnam_index/
```

## Next Steps

1. **Rebuild Docker**: `docker-compose build`
2. **Start Kafka**: `docker-compose up -d kafka zookeeper`
3. **Create Topics**: `docker exec kafka bash /kafka/setup_topics.sh`
4. **Run Producers**: Start producer containers
5. **Run Consumers**: Start consumer containers
6. **Verify**: Check bronze files and InfluxDB measurements
7. **Monitor**: Use kafka_monitoring.py to track lag
8. **Scale**: Add more consumers as needed
9. **Process**: Run 2_silver layer processing on bronze data
10. **Visualize**: Create Grafana dashboards on InfluxDB data

## Support

For issues, check:
1. Docker container status: `docker ps`
2. Logs: `docker logs <container_name>`
3. Kafka topics: `docker exec kafka kafka-topics --list --bootstrap-server kafka:9092`
4. InfluxDB: `docker exec influxdb influx -username admin -password admin123`
