#!/bin/bash
# Setup Kafka Topics
# Run from inside kafka container: docker exec kafka bash /opt/bitnami/kafka/scripts/setup_topics.sh

BOOTSTRAP_SERVER="kafka:9092"

# Note: Using replication-factor 1 since there's only 1 broker
# For production, add more brokers and increase RF to 2 or 3

kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic ticker-realtime --partitions 4 --replication-factor 1 \
  --config retention.ms=604800000 --if-not-exists

kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic index-world-realtime --partitions 4 --replication-factor 1 \
  --config retention.ms=604800000 --if-not-exists

kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic product-realtime --partitions 4 --replication-factor 1 \
  --config retention.ms=604800000 --if-not-exists

kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic index-realtime --partitions 1 --replication-factor 1 \
  --config retention.ms=604800000 --if-not-exists

echo "Topics created successfully:"
kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER

