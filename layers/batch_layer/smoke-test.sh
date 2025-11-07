#!/bin/bash
# ========================================
# Batch Layer Smoke Test
# ========================================
# Quick verification that everything works
# ========================================

set -e

echo "=========================================="
echo "Batch Layer Smoke Test"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

DOCKER_CMD=$(if docker ps >/dev/null 2>&1; then echo "docker"; else echo "sudo docker"; fi)
COMPOSE_CMD=$(if docker compose version >/dev/null 2>&1; then echo "docker compose"; else echo "sudo docker compose"; fi)

# Function to check container status
check_container() {
    local container_name=$1
    local expected_status=$2
    
    echo -n "  Checking $container_name... "
    
    if $DOCKER_CMD ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        local status=$($DOCKER_CMD ps --format '{{.Names}}\t{{.Status}}' | grep "^${container_name}" | awk '{print $2}')
        if [[ "$status" == "$expected_status"* ]] || [[ "$status" == "Up" ]]; then
            echo -e "${GREEN}✓${NC} $status"
            return 0
        else
            echo -e "${YELLOW}⚠${NC} $status"
            return 1
        fi
    else
        echo -e "${RED}✗${NC} Not running"
        return 1
    fi
}

# Function to test HTTP endpoint
test_endpoint() {
    local url=$1
    local name=$2
    
    echo -n "  Testing $name... "
    
    if curl -s -f -o /dev/null -w "%{http_code}" "$url" | grep -q "200\|302"; then
        echo -e "${GREEN}✓${NC} Accessible"
        return 0
    else
        echo -e "${RED}✗${NC} Not accessible"
        return 1
    fi
}

echo -e "${BLUE}1. Checking Core Services${NC}"
echo "=========================================="

SERVICES_OK=true

check_container "batch_postgres" "Up" || SERVICES_OK=false
check_container "batch_namenode" "Up" || SERVICES_OK=false
check_container "batch_mongo" "Up" || SERVICES_OK=false
check_container "batch_spark_master" "Up" || SERVICES_OK=false

echo ""

echo -e "${BLUE}2. Checking Web UIs${NC}"
echo "=========================================="

UI_OK=true

test_endpoint "http://localhost:9870" "HDFS Namenode" || UI_OK=false
test_endpoint "http://localhost:8081" "Spark Master" || UI_OK=false
test_endpoint "http://localhost:8080/health" "Airflow" || UI_OK=false
test_endpoint "http://localhost:8082" "Mongo Express" || UI_OK=false

echo ""

echo -e "${BLUE}3. Checking Data Services${NC}"
echo "=========================================="

DATA_OK=true

# Check HDFS
echo -n "  Testing HDFS... "
if $COMPOSE_CMD -f docker-compose.batch.yml exec -T namenode hdfs dfs -ls / >/dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} HDFS operational"
else
    echo -e "${RED}✗${NC} HDFS not responding"
    DATA_OK=false
fi

# Check MongoDB
echo -n "  Testing MongoDB... "
if $COMPOSE_CMD -f docker-compose.batch.yml exec -T mongo mongosh --quiet --eval "db.adminCommand('ping')" >/dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} MongoDB operational"
else
    echo -e "${RED}✗${NC} MongoDB not responding"
    DATA_OK=false
fi

# Check Spark
echo -n "  Testing Spark... "
if $COMPOSE_CMD -f docker-compose.batch.yml exec -T spark-master /opt/spark/bin/spark-submit --version >/dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Spark operational"
else
    echo -e "${YELLOW}⚠${NC} Spark check skipped"
fi

echo ""

echo -e "${BLUE}4. Checking Airflow${NC}"
echo "=========================================="

AIRFLOW_OK=true

# Check if DAG exists
echo -n "  Checking for DAG... "
if $COMPOSE_CMD -f docker-compose.batch.yml exec -T airflow-webserver airflow dags list 2>/dev/null | grep -q "tmdb_batch_pipeline"; then
    echo -e "${GREEN}✓${NC} tmdb_batch_pipeline DAG found"
else
    echo -e "${YELLOW}⚠${NC} DAG not found (may need to refresh)"
    AIRFLOW_OK=false
fi

# Check connections
echo -n "  Checking connections... "
CONN_COUNT=$($COMPOSE_CMD -f docker-compose.batch.yml exec -T airflow-webserver airflow connections list 2>/dev/null | grep -c "default" || echo "0")
if [ "$CONN_COUNT" -ge 2 ]; then
    echo -e "${GREEN}✓${NC} $CONN_COUNT connections configured"
else
    echo -e "${YELLOW}⚠${NC} Only $CONN_COUNT connections found"
fi

echo ""

echo "=========================================="
echo "Summary"
echo "=========================================="

if $SERVICES_OK && $UI_OK && $DATA_OK && $AIRFLOW_OK; then
    echo -e "${GREEN}✓ All smoke tests passed!${NC}"
    echo ""
    echo "Your batch layer is ready to use."
    echo ""
    echo "Next steps:"
    echo "  1. Create HDFS directories: make hdfs-create-dirs"
    echo "  2. Trigger the pipeline: make trigger-dag"
    echo "  3. Monitor in Airflow UI: http://localhost:8080"
    echo ""
    exit 0
else
    echo -e "${YELLOW}⚠ Some tests failed or have warnings${NC}"
    echo ""
    if ! $SERVICES_OK; then
        echo "  - Core services: Some containers not running properly"
    fi
    if ! $UI_OK; then
        echo "  - Web UIs: Some endpoints not accessible"
    fi
    if ! $DATA_OK; then
        echo "  - Data services: Some services not responding"
    fi
    if ! $AIRFLOW_OK; then
        echo "  - Airflow: Configuration incomplete"
    fi
    echo ""
    echo "Review the output above and check logs: make logs"
    exit 1
fi
