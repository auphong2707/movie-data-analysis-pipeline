#!/bin/bash
# Speed Layer Stop Script
# Gracefully stops all Speed Layer components

echo "🛑 Stopping Speed Layer components..."

# Function to stop process by PID file
stop_process() {
    local service_name=$1
    local pid_file=$2
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping $service_name (PID: $pid)..."
            kill -TERM "$pid"
            
            # Wait up to 30 seconds for graceful shutdown
            local attempts=30
            while kill -0 "$pid" 2>/dev/null && [ $attempts -gt 0 ]; do
                sleep 1
                attempts=$((attempts - 1))
            done
            
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                echo "Force killing $service_name..."
                kill -KILL "$pid"
            fi
            
            echo "✅ $service_name stopped"
        else
            echo "⚠️  $service_name PID $pid not running"
        fi
        rm -f "$pid_file"
    else
        echo "⚠️  No PID file found for $service_name"
    fi
}

# Stop all Speed Layer processes
if [ -d "logs" ]; then
    stop_process "TMDB Producer" "logs/tmdb_producer.pid"
    stop_process "Sentiment Stream" "logs/sentiment_stream.pid"
    stop_process "Aggregation Stream" "logs/aggregation_stream.pid"
    stop_process "Trending Stream" "logs/trending_stream.pid"
else
    echo "⚠️  Logs directory not found, attempting to kill by process name..."
    pkill -f "python.*tmdb_stream_producer.py" && echo "✅ TMDB Producer stopped"
    pkill -f "python.*review_sentiment_stream.py" && echo "✅ Sentiment Stream stopped"
    pkill -f "python.*movie_aggregation_stream.py" && echo "✅ Aggregation Stream stopped"
    pkill -f "python.*trending_detection_stream.py" && echo "✅ Trending Stream stopped"
fi

# Optionally stop external services
read -p "🤔 Stop external services (Kafka, Cassandra, MongoDB)? [y/N]: " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🐳 Stopping external services..."
    docker-compose -f ../../docker-compose.yml stop kafka schema-registry cassandra mongodb zookeeper
    echo "✅ External services stopped"
fi

echo "🏁 Speed Layer shutdown complete!"