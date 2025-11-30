#!/bin/bash

# Script khởi động toàn bộ Serving Layer (API + Grafana)

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Movie Analytics - Serving Layer${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Bước 1: Kiểm tra network
echo -e "${YELLOW}[1/5] Checking network...${NC}"
if ! docker network ls | grep -q movie-pipeline; then
    echo -e "${YELLOW}Creating network movie-pipeline...${NC}"
    docker network create movie-pipeline
fi
echo -e "${GREEN}✓ Network ready${NC}"
echo ""

# Bước 2: Kiểm tra .env
echo -e "${YELLOW}[2/5] Checking environment configuration...${NC}"
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env from template...${NC}"
    cp .env.example .env
    echo -e "${RED}⚠ Please edit .env file with your MongoDB URI${NC}"
    echo -e "${YELLOW}Press Enter when ready...${NC}"
    read
fi
echo -e "${GREEN}✓ Environment configured${NC}"
echo ""

# Bước 3: Khởi động API + Redis
echo -e "${YELLOW}[3/5] Starting API and Redis...${NC}"
docker-compose -f docker-compose.serving.yml up -d
echo -e "${GREEN}✓ API and Redis started${NC}"
echo ""

# Đợi API khởi động
echo -e "${YELLOW}Waiting for API to be ready...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:8000/api/v1/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓ API is ready${NC}"
        break
    fi
    echo -n "."
    sleep 2
done
echo ""

# Bước 4: Khởi động Grafana
echo -e "${YELLOW}[4/5] Starting Grafana...${NC}"
cd visualization/grafana
docker-compose -f docker-compose.grafana.yml up -d
cd ../..
echo -e "${GREEN}✓ Grafana started${NC}"
echo ""

# Đợi Grafana khởi động
echo -e "${YELLOW}Waiting for Grafana to be ready...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Grafana is ready${NC}"
        break
    fi
    echo -n "."
    sleep 2
done
echo ""

# Bước 5: Kiểm tra trạng thái
echo -e "${YELLOW}[5/5] Checking services status...${NC}"
echo ""
docker ps --filter "name=serving-api" --filter "name=serving-redis" --filter "name=grafana" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

# Tóm tắt
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  All Services Started Successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${GREEN}📡 API Documentation:${NC}"
echo -e "   http://localhost:8000/docs"
echo ""
echo -e "${GREEN}📊 Grafana Dashboards:${NC}"
echo -e "   http://localhost:3000"
echo -e "   Username: ${YELLOW}admin${NC}"
echo -e "   Password: ${YELLOW}admin${NC}"
echo ""
echo -e "${GREEN}🔍 Health Check:${NC}"
echo -e "   curl http://localhost:8000/api/v1/health"
echo ""
echo -e "${GREEN}📝 View Logs:${NC}"
echo -e "   docker logs serving-api -f"
echo -e "   docker logs grafana -f"
echo ""
echo -e "${GREEN}🛑 Stop All:${NC}"
echo -e "   ./stop-all.sh"
echo ""
