#!/bin/bash
# Setup Kafka topics for Reddit stream

set -e

KAFKA_CONTAINER="kafka"
KAFKA_BOOTSTRAP="kafka:9092"

echo "Creating Kafka topics for Reddit stream..."

# reddit.posts topic
docker exec -it $KAFKA_CONTAINER kafka-topics.sh \
  --create \
  --if-not-exists \
  --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic reddit.posts \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=172800000 \
  --config compression.type=gzip

# reddit.comments topic
docker exec -it $KAFKA_CONTAINER kafka-topics.sh \
  --create \
  --if-not-exists \
  --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic reddit.comments \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=172800000 \
  --config compression.type=gzip

echo "Verifying topics..."
docker exec -it $KAFKA_CONTAINER kafka-topics.sh \
  --list \
  --bootstrap-server $KAFKA_BOOTSTRAP

echo "✅ Reddit Kafka topics created successfully"
