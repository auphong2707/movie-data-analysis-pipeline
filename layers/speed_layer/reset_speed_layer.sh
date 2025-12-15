#!/bin/bash

################################################################################
# Speed Layer Reset Script
################################################################################
# Purpose: Complete reset of speed layer infrastructure
# - Stops all running processes
# - Clears Kafka topics
# - Drops and recreates Cassandra keyspace/tables
# - Clears Spark checkpoints
# - Restarts all speed layer services
################################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"
SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"

echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}          Speed Layer Complete Reset Script${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""

################################################################################
# Step 1: Stop Running Processes
################################################################################
echo -e "${YELLOW}[1/7] Stopping running speed layer processes...${NC}"

# Stop Spark streaming jobs
echo "  → Stopping Spark streaming jobs..."
pkill -f "reddit_sentiment_stream.py" 2>/dev/null || echo "  ✓ No streaming jobs running"

# Stop Reddit producers
echo "  → Stopping Reddit producers..."
pkill -f "reddit_stream_producer.py" 2>/dev/null || echo "  ✓ No producers running"

# Stop sync connectors
echo "  → Stopping sync connectors..."
pkill -f "reddit_sync.py" 2>/dev/null || echo "  ✓ No sync connectors running"

sleep 2
echo -e "${GREEN}✓ All processes stopped${NC}"
echo ""

################################################################################
# Step 2: Clear Kafka Topics
################################################################################
echo -e "${YELLOW}[2/7] Clearing Kafka topics...${NC}"

# List of Reddit topics
TOPICS=(
    "reddit.posts"
    "reddit.comments"
)

# Check if Kafka is available
if ! docker ps | grep -q kafka; then
    echo -e "${RED}  ✗ Kafka container not running. Start infrastructure first.${NC}"
    exit 1
fi

# Delete and recreate topics
for topic in "${TOPICS[@]}"; do
    echo "  → Deleting topic: $topic"
    docker exec -it kafka kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --delete --topic "$topic" 2>/dev/null || echo "    (topic doesn't exist)"
    
    echo "  → Recreating topic: $topic"
    docker exec -it kafka kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create --topic "$topic" \
        --partitions 3 \
        --replication-factor 1 \
        --config retention.ms=172800000 \
        --config segment.ms=3600000 || echo "    (failed to create)"
done

echo -e "${GREEN}✓ Kafka topics cleared and recreated${NC}"
echo ""

################################################################################
# Step 3: Drop and Recreate Cassandra Keyspace
################################################################################
echo -e "${YELLOW}[3/7] Resetting Cassandra keyspace...${NC}"

# Check if Cassandra is available
if ! docker ps | grep -q cassandra; then
    echo -e "${RED}  ✗ Cassandra container not running. Start infrastructure first.${NC}"
    exit 1
fi

echo "  → Dropping speed_layer keyspace..."
docker exec -it cassandra cqlsh -e "DROP KEYSPACE IF EXISTS speed_layer;" || echo "    (keyspace doesn't exist)"

sleep 2

echo "  → Recreating keyspace and tables from schema..."
docker exec -i cassandra cqlsh < /home/veil/Documents/GitHub/movie-data-analysis-pipeline/layers/speed_layer/cassandra_views/reddit_schema.cql

echo -e "${GREEN}✓ Cassandra keyspace reset${NC}"
echo ""

################################################################################
# Step 4: Clear Spark Checkpoints
################################################################################
echo -e "${YELLOW}[4/7] Clearing Spark checkpoints...${NC}"

CHECKPOINT_DIRS=(
    "/opt/spark/checkpoints/reddit_stream"
    "/opt/spark/checkpoints/reddit_post_metrics"
    "/opt/spark/checkpoints/reddit_comment_metrics"
    "./checkpoints"
)

for dir in "${CHECKPOINT_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        echo "  → Removing checkpoint: $dir"
        rm -rf "$dir"
    fi
done

# Recreate checkpoint directories
mkdir -p ./checkpoints
mkdir -p /opt/spark/checkpoints 2>/dev/null || echo "  (using local checkpoints)"

echo -e "${GREEN}✓ Spark checkpoints cleared${NC}"
echo ""

################################################################################
# Step 5: Clear Local Logs
################################################################################
echo -e "${YELLOW}[5/7] Clearing local logs...${NC}"

if [ -d "./logs" ]; then
    echo "  → Clearing logs directory..."
    rm -rf ./logs/*
    mkdir -p ./logs
fi

echo -e "${GREEN}✓ Logs cleared${NC}"
echo ""

################################################################################
# Step 6: Clear MongoDB Speed Views (Optional)
################################################################################
echo -e "${YELLOW}[6/7] Clearing MongoDB speed_views collection...${NC}"

if docker ps | grep -q mongodb; then
    echo "  → Dropping speed_views collection..."
    docker exec -it mongodb mongosh movie_data_pipeline \
        --eval "db.speed_views.drop()" 2>/dev/null || echo "    (collection doesn't exist)"
    
    echo -e "${GREEN}✓ MongoDB speed_views cleared${NC}"
else
    echo -e "${YELLOW}  ⚠ MongoDB not running, skipping...${NC}"
fi
echo ""

################################################################################
# Step 7: Verification
################################################################################
echo -e "${YELLOW}[7/7] Verifying reset...${NC}"

# Verify Kafka topics
echo "  → Verifying Kafka topics..."
TOPIC_COUNT=$(docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -c "reddit\." || echo "0")
if [ "$TOPIC_COUNT" -eq 2 ]; then
    echo -e "    ${GREEN}✓ 2 Reddit topics created${NC}"
else
    echo -e "    ${YELLOW}⚠ Found $TOPIC_COUNT topics (expected 2)${NC}"
fi

# Verify Cassandra tables
echo "  → Verifying Cassandra tables..."
TABLE_COUNT=$(docker exec cassandra cqlsh -e "DESCRIBE KEYSPACE speed_layer;" 2>/dev/null | grep -c "CREATE TABLE" || echo "0")
if [ "$TABLE_COUNT" -ge 2 ]; then
    echo -e "    ${GREEN}✓ $TABLE_COUNT tables created${NC}"
else
    echo -e "    ${YELLOW}⚠ Found $TABLE_COUNT tables${NC}"
fi

# Verify no running processes
echo "  → Verifying no speed layer processes running..."
PROCESS_COUNT=$(pgrep -f "reddit_.*\.py" | wc -l || echo "0")
if [ "$PROCESS_COUNT" -eq 0 ]; then
    echo -e "    ${GREEN}✓ No processes running${NC}"
else
    echo -e "    ${YELLOW}⚠ Found $PROCESS_COUNT processes still running${NC}"
fi

echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ Speed Layer Reset Complete!${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "  1. Start Reddit producers:"
echo "     $ python reddit_producers/reddit_stream_producer.py"
echo ""
echo "  2. Start Spark streaming job:"
echo "     $ spark-submit streaming_jobs/reddit_sentiment_stream.py"
echo ""
echo "  3. Monitor Kafka topics:"
echo "     $ docker exec -it kafka kafka-console-consumer.sh \\"
echo "       --bootstrap-server localhost:9092 --topic reddit.posts --from-beginning"
echo ""
echo "  4. Query Cassandra:"
echo "     $ docker exec -it cassandra cqlsh"
echo "     cqlsh> SELECT * FROM speed_layer.reddit_post_metrics LIMIT 5;"
echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
