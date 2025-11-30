#!/bin/bash

# Script dừng toàn bộ Serving Layer

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Stopping all Serving Layer services...${NC}"
echo ""

# Dừng API + Redis
echo -e "${YELLOW}Stopping API and Redis...${NC}"
docker-compose -f docker-compose.serving.yml down

# Dừng Grafana
echo -e "${YELLOW}Stopping Grafana...${NC}"
cd visualization/grafana
docker-compose -f docker-compose.grafana.yml down
cd ../..

echo ""
echo -e "${GREEN}✓ All services stopped${NC}"
