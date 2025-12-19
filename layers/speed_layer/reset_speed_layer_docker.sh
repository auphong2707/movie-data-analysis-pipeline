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
echo -e "${YELLOW}What this script does:${NC}"
echo "  • Clears Kafka topics (data lost)"
echo "  • Drops Cassandra keyspace (data lost)"
echo "  • Clears checkpoints and logs"
echo "  • Clears MongoDB speed_views"
if [ "$REMOVE_VOLUMES" = true ]; then
    echo -e "  ${RED}• Removes Docker volumes (ALL historical data lost)${NC}"
else
    echo -e "  ${YELLOW}• Keeps Docker volumes (use --remove-volumes for complete clean)${NC}"
fi
echo ""

if [ "$REMOVE_VOLUMES" = true ]; then
    echo -e "${RED}⚠ WARNING: Volume removal enabled - ALL DATA WILL BE PERMANENTLY DELETED${NC}"
    echo -e "${YELLOW}Press Ctrl+C within 5 seconds to cancel...${NC}"
    sleep 5
else
    echo -e "${YELLOW}⚠ NOTE: Without --remove-volumes, Kafka/Zookeeper may retain stale metadata${NC}"
    echo -e "${YELLOW}   This can cause startup issues (NodeExistsException)${NC}"
    echo -e "${YELLOW}   Recommended: Use --remove-volumes for clean reset${NC}"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to cancel, or Enter to continue...${NC}"
    read
fi

cd "$PROJECT_ROOT"

################################################################################
# Step 1: Stop Speed Layer Containers
################################################################################
echo -e "${YELLOW}[1/6] Stopping speed layer containers...${NC}"

docker compose stop speed-reddit-producer speed-reddit-sentiment-stream speed-cassandra-mongo-sync 2>/dev/null || echo "  (containers not running)"
echo -e "${GREEN}✓ Containers stopped${NC}"
echo ""

################################################################################
# Step 2: Clear Kafka Topics
################################################################################
echo -e "${YELLOW}[2/6] Clearing Kafka topics...${NC}"

# Check if Kafka is running
if ! docker compose ps speed-kafka-1 | grep -q "Up"; then
    echo "  → Starting Kafka cluster..."
    docker compose up -d speed-zookeeper speed-kafka-1 speed-kafka-2 speed-kafka-3
    echo "  → Waiting for Kafka to be ready (60s)..."
    sleep 60
else
    echo "  → Kafka cluster already running"
    # Still wait a bit to ensure it's fully ready
    sleep 5
fi

# Verify Kafka is healthy
echo "  → Verifying Kafka health..."
for i in {1..30}; do
    if docker compose exec -T speed-kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:29092 2>&1 | grep -q "ApiVersion"; then
        echo "  ✓ Kafka is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}  ✗ Kafka did not become ready in time${NC}"
        echo -e "${YELLOW}  Continuing anyway...${NC}"
    fi
    sleep 2
done

TOPICS=("reddit.posts" "reddit.comments")

for topic in "${TOPICS[@]}"; do
    echo "  → Resetting topic: $topic"
    
    # Delete topic (use internal network address)
    docker compose exec -T speed-kafka-1 kafka-topics \
        --bootstrap-server kafka-1:29092 \
        --delete --topic "$topic" 2>/dev/null || echo "    (doesn't exist)"
    
    sleep 2
    
    # Recreate topic (use internal network address)
    docker compose exec -T speed-kafka-1 kafka-topics \
        --bootstrap-server kafka-1:29092 \
        --create --topic "$topic" \
        --partitions 3 \
        --replication-factor 3 \
        --config retention.ms=172800000 \
        --config segment.ms=3600000 || echo "    (failed to create)"
done

echo -e "${GREEN}✓ Kafka topics reset${NC}"
echo ""

################################################################################
# Step 3: Reset Cassandra Keyspace
################################################################################
echo -e "${YELLOW}[3/6] Resetting Cassandra keyspace...${NC}"

# Ensure Cassandra is running
if ! docker compose ps speed-cassandra | grep -q "Up"; then
    echo "  → Starting Cassandra..."
    docker compose up -d speed-cassandra
    echo "  → Waiting for Cassandra to be ready (60s)..."
    sleep 60
fi

# Drop keyspace
echo "  → Dropping speed_layer keyspace..."
docker compose exec -T speed-cassandra cqlsh -e "DROP KEYSPACE IF EXISTS speed_layer;" 2>/dev/null || echo "    (doesn't exist)"

sleep 2

# Recreate from schema
echo "  → Recreating keyspace and tables..."
docker compose exec -T speed-cassandra cqlsh < "$SCRIPT_DIR/cassandra_views/reddit_schema.cql"

echo -e "${GREEN}✓ Cassandra keyspace reset${NC}"
echo ""

################################################################################
# Step 4: Clear Checkpoints and Logs
################################################################################
echo -e "${YELLOW}[4/6] Clearing checkpoints and logs...${NC}"

# Clear local checkpoints
if [ -d "$SCRIPT_DIR/app/checkpoints" ]; then
    echo "  → Clearing local checkpoints..."
    rm -rf "$SCRIPT_DIR/app/checkpoints"/*
fi

# Clear logs
if [ -d "$SCRIPT_DIR/logs" ]; then
    echo "  → Clearing logs..."
    rm -rf "$SCRIPT_DIR/logs"/*
fi

# Clear container checkpoints (if volume is mounted)
docker compose exec -T speed-reddit-sentiment-stream bash -c 'rm -rf /opt/spark/checkpoints/* 2>/dev/null' || echo "  (container checkpoints cleared)"

echo -e "${GREEN}✓ Checkpoints and logs cleared${NC}"
echo ""

################################################################################
# Step 5: Clear MongoDB Speed Views
################################################################################
echo -e "${YELLOW}[5/6] Clearing MongoDB speed_views...${NC}"

if docker compose ps serving-mongodb | grep -q "Up"; then
    echo "  → Dropping speed_views collection..."
    docker compose exec -T serving-mongodb mongosh moviedb \
        --eval "db.speed_views.drop()" 2>/dev/null || echo "    (collection doesn't exist)"
    echo -e "${GREEN}✓ MongoDB speed_views cleared${NC}"
else
    echo -e "${YELLOW}  ⚠ MongoDB not running, skipping...${NC}"
fi
echo ""

################################################################################
# Step 6: Remove Speed Layer Volumes
################################################################################
echo -e "${YELLOW}[6/6] Removing speed layer volumes...${NC}"

if [ "$REMOVE_VOLUMES" = true ]; then
    echo "  → Stopping speed layer containers..."
    docker compose stop speed-reddit-producer speed-reddit-sentiment-stream speed-cassandra-mongo-sync \
        speed-kafka-topics-init speed-schema-registry speed-cassandra speed-kafka-1 speed-kafka-2 speed-kafka-3 speed-zookeeper 2>/dev/null
    
    echo "  → Removing speed layer containers..."
    docker compose rm -f speed-reddit-producer speed-reddit-sentiment-stream speed-cassandra-mongo-sync \
        speed-kafka-topics-init speed-cassandra-init speed-schema-registry \
        speed-cassandra speed-kafka-1 speed-kafka-2 speed-kafka-3 speed-zookeeper 2>/dev/null
    
    # Get the actual volume prefix (project directory name)
    PROJECT_NAME=$(basename "$PROJECT_ROOT")
    
    # Remove specific speed layer volumes
    SPEED_VOLUMES=(
        "${PROJECT_NAME}_speed-zookeeper-data"
        "${PROJECT_NAME}_speed-zookeeper-logs"
        "${PROJECT_NAME}_speed-kafka-broker1-data"
        "${PROJECT_NAME}_speed-kafka-broker2-data"
        "${PROJECT_NAME}_speed-kafka-broker3-data"
        "${PROJECT_NAME}_speed-cassandra-data"
        "${PROJECT_NAME}_speed-application-logs"
        "${PROJECT_NAME}_speed-spark-checkpoints"
    )
    
    for volume in "${SPEED_VOLUMES[@]}"; do
        echo "  → Removing volume: $volume"
        docker volume rm "$volume" 2>/dev/null || echo "    (doesn't exist)"
    done
    
    echo -e "${GREEN}✓ All speed layer volumes removed${NC}"
    echo -e "${RED}⚠ NOTE: This was a COMPLETE reset - all historical data is lost${NC}"
else
    echo -e "${YELLOW}  ℹ Keeping volumes (use --remove-volumes for complete clean)${NC}"
    echo -e "${YELLOW}  ⚠ WARNING: Kafka/Zookeeper may have stale data - recommended to use --remove-volumes${NC}"
fi
echo ""

################################################################################
# Rebuild Images (Optional)
################################################################################
if [ "$REBUILD_IMAGES" = true ]; then
    echo -e "${YELLOW}Rebuilding speed layer image...${NC}"
    docker compose build speed-reddit-producer
    echo -e "${GREEN}✓ Image rebuilt${NC}"
    echo ""
fi

################################################################################
# Restart Speed Layer
################################################################################
echo -e "${YELLOW}Restarting speed layer services...${NC}"

docker compose up -d speed-reddit-producer speed-reddit-sentiment-stream speed-cassandra-mongo-sync

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
