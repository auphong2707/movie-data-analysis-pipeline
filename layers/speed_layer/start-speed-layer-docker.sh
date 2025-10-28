#!/bin/bash
# Speed Layer Management Script
# Unified script to manage the fully containerized Speed Layer
# Usage: ./start-speed-layer-docker.sh [start|stop|restart|status|logs|cleanup|purge]

set -e

# Get the script directory (project root)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

COMPOSE_FILE="docker-compose.speed.yml"

# Function to show usage
show_usage() {
    echo "Speed Layer Management Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start     - Start all Speed Layer services (default, smart start)"
    echo "  rebuild   - Force rebuild of all images and start services"
    echo "  stop      - Stop all services without removing containers"
    echo "  restart   - Restart all services"
    echo "  status    - Show status of all services"
    echo "  logs      - Follow logs from all services"
    echo "  cleanup   - Stop and remove all containers"
    echo "  purge     - Complete cleanup including volumes (deletes all data!)"
    echo ""
    echo "Examples:"
    echo "  $0           # Smart start (detects if already running)"
    echo "  $0 start     # Same as above"
    echo "  $0 rebuild   # Force rebuild after code changes"
    echo "  $0 status"
    echo "  $0 logs"
    echo ""
}

# Function to check Docker
check_docker() {
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ docker-compose is not installed or not in PATH"
        echo "Please install Docker Desktop or docker-compose"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        echo "❌ Docker is not running"
        echo "Please start Docker Desktop"
        exit 1
    fi
}

# Function to validate environment
validate_environment() {
    if [ ! -f ".env" ]; then
        echo "⚠️  No .env file found. Creating from .env.example..."
        if [ -f ".env.example" ]; then
            cp .env.example .env
            echo "📝 Please edit .env and add your TMDB_API_KEY"
            echo ""
        fi
    fi

    # Load environment variables
    if [ -f ".env" ]; then
        export $(grep -v '^#' .env | xargs)
    fi

    # Validate TMDB_API_KEY
    if [ -z "$TMDB_API_KEY" ] || [ "$TMDB_API_KEY" == "your_tmdb_api_key_here" ]; then
        echo "❌ TMDB_API_KEY is not set or is still the default value"
        echo "📝 Please edit .env and add your TMDB_API_KEY"
        echo ""
        echo "You can get an API key from: https://www.themoviedb.org/settings/api"
        exit 1
    fi
}

# Function to check if services are running
check_services_running() {
    local running_count=$(docker-compose -f $COMPOSE_FILE ps -q 2>/dev/null | wc -l)
    echo $running_count
}

# Function to start services
start_services() {
    echo "🐳 Starting Fully Containerized Speed Layer..."
    echo ""
    
    check_docker
    validate_environment
    
    echo "✅ Environment validated"
    echo ""
    
    # Check if services are already running
    local running=$(check_services_running)
    
    if [ "$running" -gt 0 ]; then
        echo "ℹ️  Detected $running running containers"
        echo ""
        docker-compose -f $COMPOSE_FILE ps
        echo ""
        read -p "Services are already running. Restart them? (yes/no): " -r
        if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
            echo "✅ Using existing containers"
            show_useful_commands
            return
        fi
        echo "🔄 Restarting services..."
        docker-compose -f $COMPOSE_FILE restart
        echo "✅ Services restarted!"
        show_useful_commands
        return
    fi
    
    # Check if images exist (to decide if we need to build)
    local need_build=false
    if ! docker images | grep -q "speed_layer.*tmdb-producer"; then
        need_build=true
        echo "📦 No existing images found - will build from scratch"
    else
        echo "✅ Found existing images - will use them"
    fi
    
    # Pull base images only if needed
    if [ "$need_build" = true ]; then
        echo ""
        echo "📥 Pulling base Docker images (this may take a few minutes on first run)..."
        docker-compose -f $COMPOSE_FILE pull
    fi
    
    echo ""
    echo "🚀 Starting all Speed Layer services..."
    echo ""
    echo "Services being started:"
    echo "  Infrastructure:"
    echo "    • Zookeeper (port 2181)"
    echo "    • Kafka (port 9092)"
    echo "    • Schema Registry (port 8081)"
    echo "    • Cassandra (port 9042)"
    echo "    • MongoDB (port 27017)"
    echo ""
    echo "  Speed Layer Applications:"
    echo "    • TMDB Producer"
    echo "    • Event Producer"
    echo "    • Sentiment Stream Processor"
    echo "    • Aggregation Stream Processor"
    echo "    • Trending Detection Processor"
    echo ""
    
    # Start infrastructure services first
    echo "📦 Starting infrastructure services..."
    docker-compose -f $COMPOSE_FILE up -d zookeeper kafka schema-registry cassandra mongodb
    
    echo ""
    echo "⏳ Waiting for infrastructure to be ready (30s)..."
    sleep 30
    
    # Initialize Cassandra schema
    echo ""
    echo "🗄️  Initializing Cassandra schema..."
    docker-compose -f $COMPOSE_FILE up cassandra-init
    
    # Start speed layer applications (only build if needed)
    echo ""
    if [ "$need_build" = true ]; then
        echo "� Building and starting Speed Layer applications..."
        docker-compose -f $COMPOSE_FILE up -d --build tmdb-producer event-producer sentiment-stream aggregation-stream trending-stream
    else
        echo "🚀 Starting Speed Layer applications (using existing images)..."
        docker-compose -f $COMPOSE_FILE up -d tmdb-producer event-producer sentiment-stream aggregation-stream trending-stream
    fi
    
    echo ""
    echo "⏳ Waiting for services to start..."
    sleep 10
    
    show_status
    
    echo ""
    echo "✅ Speed Layer started successfully!"
    echo ""
    show_useful_commands
}

# Function to rebuild services
rebuild_services() {
    echo "🔨 Rebuilding Speed Layer (force rebuild all images)..."
    echo ""
    
    check_docker
    validate_environment
    
    echo "✅ Environment validated"
    echo ""
    
    # Stop existing services
    echo "🛑 Stopping existing services..."
    docker-compose -f $COMPOSE_FILE stop
    
    echo ""
    echo "📥 Pulling latest base images..."
    docker-compose -f $COMPOSE_FILE pull
    
    echo ""
    echo "🔨 Rebuilding all Speed Layer images..."
    docker-compose -f $COMPOSE_FILE build --no-cache
    
    echo ""
    echo "🚀 Starting all services with new images..."
    
    # Start infrastructure services first
    echo "📦 Starting infrastructure services..."
    docker-compose -f $COMPOSE_FILE up -d zookeeper kafka schema-registry cassandra mongodb
    
    echo ""
    echo "⏳ Waiting for infrastructure to be ready (30s)..."
    sleep 30
    
    # Initialize Cassandra schema
    echo ""
    echo "🗄️  Initializing Cassandra schema..."
    docker-compose -f $COMPOSE_FILE up cassandra-init
    
    # Start speed layer applications
    echo ""
    echo "🚀 Starting Speed Layer applications..."
    docker-compose -f $COMPOSE_FILE up -d tmdb-producer event-producer sentiment-stream aggregation-stream trending-stream
    
    echo ""
    echo "⏳ Waiting for services to start..."
    sleep 10
    
    show_status
    
    echo ""
    echo "✅ Speed Layer rebuilt and started successfully!"
    echo ""
    show_useful_commands
}

# Function to stop services
stop_services() {
    echo "🛑 Stopping Speed Layer services..."
    check_docker
    docker-compose -f $COMPOSE_FILE stop
    echo "✅ All services stopped"
}

# Function to restart services
restart_services() {
    echo "🔄 Restarting Speed Layer services..."
    stop_services
    echo ""
    start_services
}

# Function to show status
show_status() {
    check_docker
    echo "📊 Service Status:"
    docker-compose -f $COMPOSE_FILE ps
}

# Function to show logs
show_logs() {
    check_docker
    echo "📋 Following logs (Ctrl+C to exit)..."
    docker-compose -f $COMPOSE_FILE logs -f
}

# Function to cleanup (stop and remove containers)
cleanup() {
    echo "🧹 Cleaning up Speed Layer (removing containers)..."
    check_docker
    docker-compose -f $COMPOSE_FILE down
    echo "✅ Cleanup complete - containers removed"
}

# Function to purge (complete cleanup including volumes)
purge() {
    echo "⚠️  WARNING: This will delete ALL data including:"
    echo "   - Kafka messages"
    echo "   - Cassandra data"
    echo "   - MongoDB data"
    echo "   - All logs and checkpoints"
    echo ""
    read -p "Are you sure? (yes/no): " -r
    if [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        echo "🗑️  Purging Speed Layer (removing containers and volumes)..."
        check_docker
        docker-compose -f $COMPOSE_FILE down -v
        echo "✅ Complete purge - all data deleted"
    else
        echo "❌ Purge cancelled"
    fi
}

# Function to show useful commands
show_useful_commands() {
    echo "📖 Useful Commands:"
    echo ""
    echo "  • View all logs:"
    echo "    docker-compose -f $COMPOSE_FILE logs -f"
    echo ""
    echo "  • View specific service logs:"
    echo "    docker-compose -f $COMPOSE_FILE logs -f tmdb-producer"
    echo "    docker-compose -f $COMPOSE_FILE logs -f sentiment-stream"
    echo ""
    echo "  • Check Kafka topics:"
    echo "    docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092"
    echo ""
    echo "  • Query Cassandra:"
    echo "    docker exec -it cassandra cqlsh -e \"SELECT * FROM speed_layer.movie_aggregations LIMIT 10;\""
    echo ""
}

# Main script logic
COMMAND=${1:-start}

case "$COMMAND" in
    start)
        start_services
        ;;
    rebuild)
        rebuild_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    cleanup)
        cleanup
        ;;
    purge)
        purge
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        echo "❌ Unknown command: $COMMAND"
        echo ""
        show_usage
        exit 1
        ;;
esac