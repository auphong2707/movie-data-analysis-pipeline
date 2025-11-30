#!/bin/bash

echo "🚀 Starting Grafana..."

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if API is already running
if ! curl -s http://localhost:8000/api/v1/health > /dev/null 2>&1; then
    echo "⚠️  Warning: API is not running. Please run start-all.sh first."
    echo "   Continuing anyway..."
fi

# Start Grafana
echo "📦 Starting Grafana..."
docker-compose -f docker-compose.grafana.yml up -d

# Wait for Grafana
echo "⏳ Waiting for Grafana to start..."
sleep 5

for i in {1..30}; do
    if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
        echo "✅ Grafana is ready!"
        break
    fi
    echo "   Waiting for Grafana... ($i/30)"
    sleep 2
done

echo ""
echo "✨ Grafana is running!"
echo ""
echo "📊 Access Grafana:"
echo "   URL: http://localhost:3000"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "🛑 To stop: ./stop-grafana.sh"
echo ""

# Open browser automatically (macOS)
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "🌐 Opening Grafana in browser..."
    open http://localhost:3000
fi
