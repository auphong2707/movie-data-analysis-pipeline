#!/bin/bash
# Speed Layer Startup Script
# Automates the startup process for the Speed Layer components

set -e

echo "🚀 Starting Speed Layer..."

# Function to check if a service is running
check_service() {
    local service_name=$1
    local host=$2
    local port=$3
    
    if nc -z $host $port 2>/dev/null; then
        echo "✅ $service_name is running"
        return 0
    else
        echo "❌ $service_name is not running"
        return 1
    fi
}

# Function to wait for service
wait_for_service() {
    local service_name=$1
    local host=$2
    local port=$3
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Waiting for $service_name to start..."
    
    while [ $attempt -le $max_attempts ]; do
        if check_service "$service_name" $host $port; then
            return 0
        fi
        echo "Attempt $attempt/$max_attempts failed, retrying in 5 seconds..."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    echo "❌ $service_name failed to start after $max_attempts attempts"
    return 1
}

# Check if dependencies are installed
echo "🔍 Checking Python dependencies..."
python speed_layer_checker.py --verbose || {
    echo "❌ Dependency check failed. Please install missing packages:"
    echo "pip install confluent-kafka kafka-python avro-python3 pyspark cassandra-driver vaderSentiment"
    exit 1
}

# Start external services with Docker Compose
echo "🐳 Starting external services with Docker Compose..."
docker-compose -f ../../docker-compose.yml up -d kafka zookeeper schema-registry cassandra mongodb

# Wait for services to be ready
wait_for_service "Kafka" localhost 9092
wait_for_service "Schema Registry" localhost 8081
wait_for_service "Cassandra" localhost 9042
wait_for_service "MongoDB" localhost 27017

# Initialize Cassandra schema
echo "🗄️ Initializing Cassandra schema..."
timeout 60 bash -c 'until cqlsh localhost 9042 -f cassandra_views/schema.cql; do sleep 5; done'

# Create log directory
mkdir -p logs

# Start Speed Layer components in background
echo "🚀 Starting Speed Layer components..."

# Start TMDB stream producer
echo "Starting TMDB Stream Producer..."
nohup python kafka_producers/tmdb_stream_producer.py > logs/tmdb_producer.log 2>&1 &
TMDB_PRODUCER_PID=$!
echo "TMDB Producer started with PID: $TMDB_PRODUCER_PID"

# Wait a bit for producer to initialize
sleep 10

# Start streaming jobs
echo "Starting Review Sentiment Stream..."
nohup python streaming_jobs/review_sentiment_stream.py > logs/sentiment_stream.log 2>&1 &
SENTIMENT_PID=$!
echo "Sentiment Stream started with PID: $SENTIMENT_PID"

echo "Starting Movie Aggregation Stream..."
nohup python streaming_jobs/movie_aggregation_stream.py > logs/aggregation_stream.log 2>&1 &
AGGREGATION_PID=$!
echo "Aggregation Stream started with PID: $AGGREGATION_PID"

echo "Starting Trending Detection Stream..."
nohup python streaming_jobs/trending_detection_stream.py > logs/trending_stream.log 2>&1 &
TRENDING_PID=$!
echo "Trending Stream started with PID: $TRENDING_PID"

# Save PIDs for easy stopping
echo "$TMDB_PRODUCER_PID" > logs/tmdb_producer.pid
echo "$SENTIMENT_PID" > logs/sentiment_stream.pid
echo "$AGGREGATION_PID" > logs/aggregation_stream.pid
echo "$TRENDING_PID" > logs/trending_stream.pid

echo "✅ Speed Layer startup complete!"
echo ""
echo "📊 Component Status:"
echo "  • TMDB Producer: PID $TMDB_PRODUCER_PID (logs/tmdb_producer.log)"
echo "  • Sentiment Stream: PID $SENTIMENT_PID (logs/sentiment_stream.log)"
echo "  • Aggregation Stream: PID $AGGREGATION_PID (logs/aggregation_stream.log)"
echo "  • Trending Stream: PID $TRENDING_PID (logs/trending_stream.log)"
echo ""
echo "📝 Useful commands:"
echo "  • Check logs: tail -f logs/*.log"
echo "  • Stop all: ./stop_speed_layer.sh"
echo "  • Monitor data: cqlsh -e 'SELECT COUNT(*) FROM speed_layer.review_sentiments;'"
echo ""
echo "🌐 Service URLs:"
echo "  • Kafka: localhost:9092"
echo "  • Schema Registry: http://localhost:8081"
echo "  • Cassandra: localhost:9042"
echo "  • MongoDB: localhost:27017"