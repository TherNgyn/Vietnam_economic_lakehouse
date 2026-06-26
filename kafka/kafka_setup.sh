#!/bin/bash
# Run complete Kafka system

echo "====== Vietnam Economic Lakehouse - Kafka Setup ======"
echo ""

# 1. Build Docker image
echo "[1/5] Building Docker image..."
docker-compose build

# 2. Start main services (Kafka, InfluxDB, etc.)
echo "[2/5] Starting Kafka & InfluxDB..."
docker-compose up -d kafka influxdb minio

# 3. Setup Kafka topics
echo "[3/5] Setting up Kafka topics..."
sleep 5  # Wait for Kafka to be ready
docker exec -it kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic ticker.realtime \
  --partitions 4 \
  --replication-factor 2 \
  --if-not-exists

docker exec -it kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic index_world_realtime \
  --partitions 4 \
  --replication-factor 2 \
  --if-not-exists

docker exec -it kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic product_realtime \
  --partitions 4 \
  --replication-factor 2 \
  --if-not-exists

docker exec -it kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic index_realtime \
  --partitions 1 \
  --replication-factor 2 \
  --if-not-exists

echo "[3/5] Topics created:"
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list

# 4. Start Producers
echo "[4/5] Starting Producers..."
docker exec -d runner python kafka/producer_unified.py
docker exec -d runner python kafka/producer_index.py

# 5. Start Consumers
echo "[5/5] Starting Consumers..."
docker exec -d runner python kafka/consumer_bronze_group.py
docker exec -d runner python kafka/consumer_influxdb_group.py

echo ""
echo "====== Setup Complete ======"
echo ""
echo "Producer P1-P3 (unified):  docker exec -it runner python kafka/producer_unified.py"
echo "Producer P4 (selenium):    docker exec -it runner python kafka/producer_index.py"
echo "Consumer A (Bronze):       docker exec -it runner python kafka/consumer_bronze_group.py"
echo "Consumer B (InfluxDB):     docker exec -it runner python kafka/consumer_influxdb_group.py"
echo ""
echo "View Kafka topics:  docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list"
echo "View InfluxDB:      docker exec -it influxdb influx -username admin -password admin123"
echo "View Grafana:       http://localhost:3000"
echo ""
