#!/bin/bash
# =============================================================================
# Movie Data Analysis Pipeline - Unified Startup Script
# =============================================================================
# This script orchestrates the startup sequence:
# 1. Starts all infrastructure (serving, batch, speed layers)
# 2. Triggers batch layer DAG for cold start (TMDB baseline data)
# 3. Waits for batch processing to complete
# 4. Speed layer automatically processes Reddit data once batch is ready
#
# Usage: ./start.sh
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
AIRFLOW_HOST="localhost"
AIRFLOW_PORT="8088"
AIRFLOW_USER="admin"
AIRFLOW_PASSWORD="admin"
DAG_ID="tmdb_baseline_pipeline"
MAX_WAIT_TIME=600  # 10 minutes max wait for DAG completion

# =============================================================================
# Helper Functions
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_command() {
    if ! command -v $1 &> /dev/null; then
        log_error "$1 is required but not installed."
        exit 1
    fi
}

wait_for_service() {
    local service_name=$1
    local url=$2
    local max_attempts=${3:-30}
    local attempt=1
    
    log_info "Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            log_success "$service_name is ready!"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log_error "$service_name failed to start within timeout"
    return 1
}

get_dag_run_state() {
    local dag_run_id=$1
    
    # Use Airflow CLI to check DAG run state
    docker exec batch-airflow-scheduler airflow dags list-runs -d ${DAG_ID} --output json 2>/dev/null | \
        python3 -c "import sys, json; runs = json.load(sys.stdin); state = next((r['state'] for r in runs if r['run_id'] == '${dag_run_id}'), 'unknown'); print(state)" 2>/dev/null || echo "unknown"
}

# =============================================================================
# Main Execution
# =============================================================================

log_info "Starting Movie Data Analysis Pipeline..."
echo ""

# Check prerequisites
log_info "Checking prerequisites..."
check_command "docker"
check_command "docker-compose"
check_command "curl"
check_command "python3"

# Check if .env file exists
if [ ! -f .env ]; then
    log_warning ".env file not found. Creating from .env.example if available..."
    if [ -f .env.example ]; then
        cp .env.example .env
        log_success "Created .env file from .env.example"
    else
        log_error ".env file is required. Please create one with TMDB_API_KEY."
        exit 1
    fi
fi

# Check if TMDB_API_KEY is set
if ! grep -q "TMDB_API_KEY=.*[^[:space:]]" .env 2>/dev/null; then
    log_error "TMDB_API_KEY is not set in .env file. Please add it and try again."
    exit 1
fi

echo ""
log_info "========================================="
log_info "Phase 1: Starting Infrastructure"
log_info "========================================="
echo ""

# Start all services
log_info "Starting all Docker services..."
docker-compose up -d

echo ""
log_info "========================================="
log_info "Phase 2: Waiting for Core Services"
log_info "========================================="
echo ""

# Wait for MongoDB (serving layer)
wait_for_service "MongoDB" "http://localhost:27017" 60 || exit 1

# Wait for Airflow webserver (batch layer)
log_info "Waiting for Airflow to initialize (this may take 2-3 minutes)..."
wait_for_service "Airflow Webserver" "http://localhost:8088/health" 90 || exit 1

# Give Airflow time to fully initialize (scheduler must be ready to trigger DAGs)
log_info "Allowing Airflow scheduler to initialize and load DAGs..."
log_info "Waiting 30 seconds for scheduler to be ready..."
sleep 30

# Verify scheduler is ready
log_info "Verifying Airflow scheduler is ready..."
SCHEDULER_CHECK=$(docker exec batch-airflow-scheduler airflow dags list 2>&1 | grep -c "${DAG_ID}" || echo "0")

if [ "$SCHEDULER_CHECK" = "0" ]; then
    log_warning "DAG '${DAG_ID}' not found in scheduler yet. Waiting additional 15 seconds..."
    sleep 15
    SCHEDULER_CHECK=$(docker exec batch-airflow-scheduler airflow dags list 2>&1 | grep -c "${DAG_ID}" || echo "0")
    
    if [ "$SCHEDULER_CHECK" = "0" ]; then
        log_error "DAG '${DAG_ID}' still not loaded"
        log_warning "You may need to manually trigger the DAG from Airflow UI"
    else
        log_success "Airflow scheduler has loaded the DAG!"
    fi
else
    log_success "Airflow scheduler is ready with DAG loaded!"
fi

echo ""
log_info "========================================="
log_info "Phase 3: Batch Layer Cold Start"
log_info "========================================="
echo ""

log_info "Triggering batch DAG: ${DAG_ID}"

# Use Airflow CLI to trigger DAG (more reliable than REST API with session auth)
TRIGGER_OUTPUT=$(docker exec batch-airflow-scheduler airflow dags trigger ${DAG_ID} 2>&1)

if echo "$TRIGGER_OUTPUT" | grep -q "Created.*DagRun"; then
    # Extract the dag_run_id from the output
    DAG_RUN_ID=$(echo "$TRIGGER_OUTPUT" | grep -oP 'manual__\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}' | head -1)
    if [ -z "$DAG_RUN_ID" ]; then
        # Fallback: get the latest run
        DAG_RUN_ID=$(docker exec batch-airflow-scheduler airflow dags list-runs -d ${DAG_ID} --state running --output json 2>/dev/null | python3 -c "import sys, json; runs = json.load(sys.stdin); print(runs[0]['run_id'] if runs else '')" 2>/dev/null || echo "")
    fi
elif echo "$TRIGGER_OUTPUT" | grep -qi "error\|not found"; then
    log_error "Failed to trigger DAG. Error: $TRIGGER_OUTPUT"
    log_warning "The DAG may not be loaded yet. Please manually trigger it at:"
    log_warning "http://localhost:8088/dags/${DAG_ID}/grid"
    log_warning "Continuing with startup..."
    DAG_RUN_ID=""
else
    # Try to get the run ID from recent runs
    DAG_RUN_ID=$(docker exec batch-airflow-scheduler airflow dags list-runs -d ${DAG_ID} --state running --output json 2>/dev/null | python3 -c "import sys, json; runs = json.load(sys.stdin); print(runs[0]['run_id'] if runs else '')" 2>/dev/null || echo "")
fi

if [ -z "$DAG_RUN_ID" ]; then
    log_warning "Could not automatically trigger DAG."
    log_warning "You can manually trigger it from Airflow UI at http://localhost:8088"
    log_warning "Continuing with startup..."
else
    log_success "DAG triggered successfully! Run ID: ${DAG_RUN_ID}"
    
    # Wait for DAG to complete
    log_info "Waiting for batch processing to complete (this may take several minutes)..."
    log_info "Monitor progress at: http://localhost:8088/dags/${DAG_ID}/grid"
    
    elapsed=0
    while [ $elapsed -lt $MAX_WAIT_TIME ]; do
        STATE=$(get_dag_run_state "$DAG_RUN_ID")
        
        case "$STATE" in
            "success")
                log_success "Batch processing completed successfully!"
                break
                ;;
            "failed")
                log_error "Batch processing failed. Check logs at http://localhost:8088"
                log_warning "Continuing with startup, but batch data may be incomplete..."
                break
                ;;
            "running")
                echo -n "."
                sleep 5
                elapsed=$((elapsed + 5))
                ;;
            *)
                echo -n "."
                sleep 5
                elapsed=$((elapsed + 5))
                ;;
        esac
    done
    
    if [ $elapsed -ge $MAX_WAIT_TIME ]; then
        log_warning "Batch processing timeout. DAG may still be running."
        log_warning "Check status at: http://localhost:8088"
    fi
fi

echo ""
log_info "========================================="
log_info "Phase 4: Speed Layer Verification"
log_info "========================================="
echo ""

log_info "Verifying speed layer services..."

# Check Kafka
if docker ps | grep -q "speed-kafka-1.*healthy"; then
    log_success "Kafka cluster is healthy"
else
    log_warning "Kafka cluster is still starting..."
fi

# Check Cassandra
if docker ps | grep -q "speed-cassandra.*healthy"; then
    log_success "Cassandra is healthy"
else
    log_warning "Cassandra is still starting..."
fi

# Check speed layer applications
sleep 5
if docker ps | grep -q "speed-reddit-producer"; then
    log_success "Reddit producer is running"
else
    log_warning "Reddit producer is not yet running"
fi

if docker ps | grep -q "speed-reddit-sentiment-stream"; then
    log_success "Reddit sentiment stream is running"
else
    log_warning "Reddit sentiment stream is not yet running"
fi

echo ""
log_info "========================================="
log_info "Pipeline Startup Complete!"
log_info "========================================="
echo ""

log_success "All layers are starting. Here are the access points:"
echo ""
echo "  📊 Batch Layer:"
echo "     - Airflow UI:          http://localhost:8088 (admin/admin)"
echo "     - MinIO Console:       http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "  ⚡ Speed Layer:"
echo "     - Kafka Brokers:       localhost:9092, 9093, 9094"
echo "     - Schema Registry:     http://localhost:8081"
echo "     - Cassandra CQL:       localhost:9042"
echo ""
echo "  🎯 Serving Layer:"
echo "     - API:                 http://localhost:8000"
echo "     - API Docs:            http://localhost:8000/docs"
echo "     - MongoDB:             localhost:27017 (admin/password)"
echo "     - Mongo Express:       http://localhost:8082 (admin/admin)"
echo "     - Redis:               localhost:6379"
echo ""
echo "  📈 Monitoring:"
echo "     - Prometheus:          http://localhost:9090"
echo "     - Grafana:             http://localhost:3001 (admin/admin)"
echo ""
echo "  💡 Next Steps:"
echo "     1. Monitor batch processing: http://localhost:8088/dags/${DAG_ID}/grid"
echo "     2. Check MongoDB data:       http://localhost:8082"
echo "     3. View API endpoints:       http://localhost:8000/docs"
echo "     4. Watch logs:               docker-compose logs -f speed-reddit-producer"
echo ""
echo "  🛑 To stop the pipeline:       docker-compose down"
echo "  🔄 To restart services:        docker-compose restart <service-name>"
echo "  📋 To view service status:     docker-compose ps"
echo ""

log_info "Pipeline is ready for queries! 🚀"
