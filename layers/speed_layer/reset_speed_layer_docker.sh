#!/bin/bash

################################################################################
# Speed Layer Reset Script (Docker Compose Version)
################################################################################
# Purpose: Complete reset of speed layer in Docker Compose environment
# - Stops speed layer containers
# - Clears Kafka topics
# - Drops and recreates Cassandra keyspace/tables
# - Removes volumes (optional)
# - Restarts speed layer services
################################################################################

set -e  # Exit on error

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

# Parse arguments
REMOVE_VOLUMES=false
REBUILD_IMAGES=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --remove-volumes)
            REMOVE_VOLUMES=true
            shift
            ;;
        --rebuild)
            REBUILD_IMAGES=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --remove-volumes    Remove persistent volumes (CAUTION: deletes all data)"
            echo "  --rebuild           Rebuild Docker images before starting"
            echo "  -h, --help          Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}      Speed Layer Docker Compose Reset Script${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""

if [ "$REMOVE_VOLUMES" = true ]; then
    echo -e "${RED}⚠ WARNING: Volume removal enabled - ALL DATA WILL BE DELETED${NC}"
    echo -e "${YELLOW}Press Ctrl+C to cancel, or Enter to continue...${NC}"
    read
fi

cd "$PROJECT_ROOT"

################################################################################
# Step 1: Stop Speed Layer Containers
################################################################################
echo -e "${YELLOW}[1/6] Stopping speed layer containers...${NC}"

docker-compose stop speed-reddit-producer speed-reddit-sentiment-stream speed-cassandra-mongo-sync 2>/dev/null || echo "  (containers not running)"
echo -e "${GREEN}✓ Containers stopped${NC}"
echo ""

################################################################################
# Step 2: Clear Kafka Topics
################################################################################
echo -e "${YELLOW}[2/6] Clearing Kafka topics...${NC}"

# Ensure Kafka is running
if ! docker-compose ps speed-kafka-1 | grep -q "Up"; then
    echo "  → Starting Kafka cluster..."
    docker-compose up -d speed-zookeeper speed-kafka-1 speed-kafka-2 speed-kafka-3
    echo "  → Waiting for Kafka to be ready (30s)..."
    sleep 30
fi

TOPICS=("reddit.posts" "reddit.comments")

for topic in "${TOPICS[@]}"; do
    echo "  → Resetting topic: $topic"
    
    # Delete topic
    docker-compose exec -T speed-kafka-1 kafka-topics \
        --bootstrap-server localhost:9092 \
        --delete --topic "$topic" 2>/dev/null || echo "    (doesn't exist)"
    
    sleep 1
    
    # Recreate topic
    docker-compose exec -T speed-kafka-1 kafka-topics \
        --bootstrap-server localhost:9092 \
        --create --topic "$topic" \
        --partitions 3 \
        --replication-factor 3 \
        --config retention.ms=172800000 \
        --config segment.ms=3600000
done

echo -e "${GREEN}✓ Kafka topics reset${NC}"
echo ""

################################################################################
# Step 3: Reset Cassandra Keyspace
################################################################################
echo -e "${YELLOW}[3/6] Resetting Cassandra keyspace...${NC}"

# Ensure Cassandra is running
if ! docker-compose ps speed-cassandra | grep -q "Up"; then
    echo "  → Starting Cassandra..."
    docker-compose up -d speed-cassandra
    echo "  → Waiting for Cassandra to be ready (45s)..."
    sleep 45
fi

# Drop keyspace
echo "  → Dropping speed_layer keyspace..."
docker-compose exec -T speed-cassandra cqlsh -e "DROP KEYSPACE IF EXISTS speed_layer;" 2>/dev/null || echo "    (doesn't exist)"

sleep 2

# Recreate from schema
echo "  → Recreating keyspace and tables..."
docker-compose exec -T speed-cassandra cqlsh < "$SCRIPT_DIR/cassandra_views/reddit_schema.cql"

echo -e "${GREEN}✓ Cassandra keyspace reset${NC}"
echo ""

################################################################################
# Step 4: Clear Checkpoints and Logs
################################################################################
echo -e "${YELLOW}[4/6] Clearing checkpoints and logs...${NC}"

# Clear local checkpoints
if [ -d "$SCRIPT_DIR/checkpoints" ]; then
    echo "  → Clearing local checkpoints..."
    rm -rf "$SCRIPT_DIR/checkpoints"/*
fi

# Clear logs
if [ -d "$SCRIPT_DIR/logs" ]; then
    echo "  → Clearing logs..."
    rm -rf "$SCRIPT_DIR/logs"/*
fi

# Clear container checkpoints (if volume is mounted)
docker-compose exec -T speed-reddit-sentiment-stream bash -c 'rm -rf /opt/spark/checkpoints/* 2>/dev/null' || echo "  (container checkpoints cleared)"

echo -e "${GREEN}✓ Checkpoints and logs cleared${NC}"
echo ""

################################################################################
# Step 5: Clear MongoDB Speed Views
################################################################################
echo -e "${YELLOW}[5/6] Clearing MongoDB speed_views...${NC}"

if docker-compose ps serving-mongodb | grep -q "Up"; then
    echo "  → Dropping speed_views collection..."
    docker-compose exec -T serving-mongodb mongosh movie_data_pipeline \
        --eval "db.speed_views.drop()" 2>/dev/null || echo "    (collection doesn't exist)"
    echo -e "${GREEN}✓ MongoDB speed_views cleared${NC}"
else
    echo -e "${YELLOW}  ⚠ MongoDB not running, skipping...${NC}"
fi
echo ""

################################################################################
# Step 6: Remove Volumes (Optional)
################################################################################
if [ "$REMOVE_VOLUMES" = true ]; then
    echo -e "${YELLOW}[6/6] Removing volumes...${NC}"
    docker-compose down -v
    echo -e "${GREEN}✓ Volumes removed${NC}"
else
    echo -e "${YELLOW}[6/6] Keeping volumes (use --remove-volumes to delete)${NC}"
fi
echo ""

################################################################################
# Rebuild Images (Optional)
################################################################################
if [ "$REBUILD_IMAGES" = true ]; then
    echo -e "${YELLOW}Rebuilding speed layer image...${NC}"
    docker-compose build speed-layer
    echo -e "${GREEN}✓ Image rebuilt${NC}"
    echo ""
fi

################################################################################
# Restart Speed Layer
################################################################################
echo -e "${YELLOW}Restarting speed layer services...${NC}"

docker-compose up -d speed-reddit-producer speed-reddit-sentiment-stream speed-cassandra-mongo-sync

echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ Speed Layer Reset Complete!${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}Verification Commands:${NC}"
echo ""
echo "  1. Check Kafka topics:"
echo "     $ docker-compose exec speed-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list"
echo ""
echo "  2. View Kafka messages:"
echo "     $ docker-compose exec speed-kafka-1 kafka-console-consumer \\"
echo "       --bootstrap-server localhost:9092 --topic reddit.posts --from-beginning"
echo ""
echo "  3. Query Cassandra:"
echo "     $ docker-compose exec speed-cassandra cqlsh"
echo "     cqlsh> SELECT * FROM speed_layer.reddit_post_metrics LIMIT 5;"
echo ""
echo "  4. Check speed layer logs:"
echo "     $ docker-compose logs -f speed-reddit-sentiment-stream"
echo ""
echo "  5. Check producer status:"
echo "     $ docker-compose logs -f speed-reddit-producer"
echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
