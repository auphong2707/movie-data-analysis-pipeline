#!/bin/bash

echo "🛑 Stopping Grafana Visualization Stack..."

docker-compose -f docker-compose.grafana.yml down

echo "✅ All services stopped"
echo ""
echo "💡 To remove data volumes: docker-compose -f docker-compose.grafana.yml down -v"
