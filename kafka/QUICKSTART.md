# Quick Start Guide - Vietnam Economic Lakehouse

## 1. Quick Setup (5 minutes)

```bash
# Build image (if not done)
docker-compose build

# Start Kafka cluster
docker-compose up -d kafka zookeeper

# Wait 30 seconds for Kafka to be ready
sleep 30

# Create topics - copy all 4 commands below and run from PowerShell
# Note: Using RF=1 since there's only 1 broker. For production with multiple brokers, use RF=2

docker exec kafka bash /opt/bitnami/kafka/scripts/setup_topics.sh
```

## 2. Start All Services

```bash
# Terminal 1: Producer P1+P2+P3 (Unified)
docker run -it --rm --name producer_unified --network vietnam_lake -e KAFKA_BROKER=kafka:29092 vietnam_economic_lakehouse:latest python kafka/producer_unified.py

docker exec -it producer_unified python kafka/producer_unified.py

# Terminal 2: Producer P4 (Vietnam Indices)
docker run -it --rm --name producer_p4 --network vietnam_lake -e KAFKA_BROKER=kafka:29092 vietnam_economic_lakehouse:latest python kafka/producer_index.py

# Terminal 3: Consumer Group A (Bronze)
docker run -it --rm --name consumer_bronze --network vietnam_lake -e KAFKA_BROKER=kafka:29092 -v bronze_data:/data/bronze vietnam_economic_lakehouse:latest python kafka/consumer_bronze_group.py

docker exec -it consumer_bronze python kafka/consumer_bronze_group.py
# Terminal 4: Consumer Group B (InfluxDB)
docker run -it --rm --name consumer_influxdb --network vietnam_lake -e KAFKA_BROKER=kafka:29092 -e INFLUXDB_HOST=influxdb vietnam_economic_lakehouse:latest python kafka/consumer_influxdb_group.py
```

## 3. Verify Data

```bash
# Check Kafka has topics
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Expected output:
# ticker-realtime
# index-world-realtime
# product-realtime
# index-realtime

# Check messages in topic (real-time terminal)
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic ticker-realtime --from-beginning --max-messages 5

# Expected output (JSON format):
# {"symbol":"USDVND=X","timestamp":"2024-01-15T10:30:00Z","price":24150.5}

# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --group bronze_consumer_group --describe

# Expected output:
# GROUP                    TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# bronze_consumer_group    ticker-realtime    0          10              10              0
```

## 4. Check Bronze Data

```bash
# Find bronze files
docker exec consumer_bronze find /data/bronze -type f -name "*.jsonl" | head -10

# Expected:
# /data/bronze/currency/2024-01-15/USDVND_X/data.jsonl
# /data/bronze/world_index/2024-01-15/GSPC/data.jsonl

# View first 5 records in bronze file
docker exec consumer_bronze head -5 /data/bronze/currency/2024-01-15/USDVND_X/data.jsonl

# Expected (JSONL format):
# {"symbol":"USDVND=X","timestamp":"2024-01-15T10:30:00Z","price":24150.5,"source":"yfinance"}
# {"symbol":"USDVND=X","timestamp":"2024-01-15T10:35:00Z","price":24151.0,"source":"yfinance"}
```

## 5. Check InfluxDB Data

```bash
# Connect to InfluxDB
docker exec -it influxdb influx -username admin -password admin123

# Inside influx shell:
> USE vietnam_economic
> SHOW DATABASES
> SHOW MEASUREMENTS
> SELECT * FROM currency LIMIT 5
> SELECT COUNT(*) FROM currency

# Exit
> EXIT
```

## 6. Check Logs

```bash
# Producer unified logs
docker logs producer_unified | tail -20

# Consumer bronze logs
docker logs consumer_bronze | tail -20

# Consumer InfluxDB logs
docker logs consumer_influxdb | tail -20

# Kafka logs (if issues)
docker logs kafka | tail -50
```

## Troubleshooting

### Issues & Solutions

**Problem: "Connection refused" error**
- Solution: Check Kafka is running: `docker ps | grep kafka`
- Wait 30 seconds for Kafka startup
- Check network: `docker network ls` (ensure vietnam_lake exists)

**Problem: "Topic does not exist" error**
- Solution: Create topics using commands in step 1
- Check topics created: `docker exec kafka kafka-topics --list --bootstrap-server kafka:9092`

**Problem: No data in Kafka topics**
- Solution: 
  - Check producer is running: `docker ps | grep producer`
  - Check producer logs: `docker logs producer_unified | tail -20`
  - Check yfinance can fetch data: Test with simple script

**Problem: No files in /data/bronze**
- Solution:
  - Check consumer is running: `docker ps | grep consumer_bronze`
  - Check consumer has lag=0: `docker exec kafka kafka-consumer-groups --describe --group bronze_consumer_group --bootstrap-server kafka:9092`
  - Check directory: `docker exec consumer_bronze ls -la /data/bronze`

**Problem: InfluxDB not recording data**
- Solution:
  - Check InfluxDB is running: `docker ps | grep influxdb`
  - Check InfluxDB connectivity: `docker exec consumer_influxdb ping influxdb`
  - Check consumer InfluxDB logs: `docker logs consumer_influxdb | tail -50`
  - Manually test InfluxDB: `docker exec influxdb influx -username admin -password admin123`

**Problem: Containers exit immediately**
- Solution:
  - Check logs: `docker logs <container_name>`
  - Check exit code: `docker ps -a`
  - Ensure environment variables set: `docker run -e KAFKA_BROKER=kafka:29092 ...`

## Key Commands

```bash
# View all Kafka topics
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# View consumer groups
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list

# View consumer group details
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group bronze_consumer_group --describe

# View messages in topic (real-time)
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic ticker.realtime

# Reset consumer group offset (restart from beginning)
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group bronze_consumer_group --reset-offsets --to-earliest --execute

# View InfluxDB measurements
docker exec influxdb influx -username admin -password admin123 << EOF
USE vietnam_economic
SHOW MEASUREMENTS
SELECT * FROM currency LIMIT 1
EOF

# Monitor topology
docker ps --format "table {{.Names}}\t{{.Status}}"
```

## Data Format

### Currency (P1)
```json
{
  "symbol": "USDVND=X",
  "timestamp": "2024-01-15T10:30:00Z",
  "price": 24150.5,
  "change": 0.05,
  "source": "yfinance"
}
```

### World Index (P2)
```json
{
  "symbol": "^GSPC",
  "name": "S&P 500",
  "timestamp": "2024-01-15T10:30:00Z",
  "price": 4810.25,
  "high": 4820.0,
  "low": 4800.0,
  "change": 0.50,
  "source": "yfinance"
}
```

### Product (P3)
```json
{
  "symbol": "CL=F",
  "name": "WTI Crude Oil",
  "timestamp": "2024-01-15T10:30:00Z",
  "price": 78.45,
  "volume": 50000000,
  "change": 1.2,
  "source": "yfinance"
}
```

### Vietnam Index (P4)
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

## Performance Check

```bash
# Monitor producer throughput (messages/second)
watch -n 5 'docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 --group bronze_consumer_group --describe'

# View lag in real-time (auto-refresh every 5 seconds)
watch -n 5 'docker logs -n 50 consumer_bronze | grep -i "lag\|offset"'

# Check disk usage for bronze data
docker exec consumer_bronze du -h /data/bronze

# Count total messages in topic
docker exec kafka kafka-run-class kafka.tools.JmxTool \
  --object-name kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions \
  --attributes Value --bootstrap-server kafka:9092
```

## Next Steps

1. ✅ Start Kafka & create topics
2. ✅ Run all 4 producers
3. ✅ Run 2 consumer groups
4. ✅ Verify data in Kafka topics
5. ✅ Check bronze JSONL files
6. ✅ Confirm InfluxDB measurements
7. 📋 Process bronze to silver (run 2_silver/* scripts)
8. 📊 Create Grafana dashboards on InfluxDB data
9. 🚀 Optimize throughput (batching, partitioning)
10. 📈 Scale to production (cluster mode, replication)

## More Info

See [KAFKA_ARCHITECTURE.md](KAFKA_ARCHITECTURE.md) for detailed architecture, scaling, and troubleshooting.

See [README.md](README.md) for complete documentation.
