#!/bin/bash

# Script kiểm tra Grafana auto-provisioning

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Grafana Auto-Setup Verification${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Check if Grafana is running
echo -e "${YELLOW}1. Checking Grafana container...${NC}"
if docker ps | grep -q grafana; then
    echo -e "${GREEN}✓ Grafana is running${NC}"
else
    echo -e "${RED}✗ Grafana is not running${NC}"
    echo -e "${YELLOW}Start with: docker-compose -f docker-compose.grafana.yml up -d${NC}"
    exit 1
fi

# Check provisioning files
echo -e "\n${YELLOW}2. Checking provisioning files...${NC}"
if docker exec grafana ls /etc/grafana/provisioning/datasources/infinity.yml > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Datasource provisioning file exists${NC}"
else
    echo -e "${RED}✗ Datasource provisioning file missing${NC}"
fi

if docker exec grafana ls /etc/grafana/provisioning/dashboards/dashboards.yml > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Dashboard provisioning file exists${NC}"
else
    echo -e "${RED}✗ Dashboard provisioning file missing${NC}"
fi

# Check dashboard files
echo -e "\n${YELLOW}3. Checking dashboard files...${NC}"
DASHBOARD_COUNT=$(docker exec grafana ls /var/lib/grafana/dashboards/*.json 2>/dev/null | wc -l)
echo -e "${GREEN}✓ Found ${DASHBOARD_COUNT} dashboard files${NC}"

# Check Infinity plugin
echo -e "\n${YELLOW}4. Checking Infinity plugin...${NC}"
if docker exec grafana grafana-cli plugins ls 2>/dev/null | grep -q "yesoreyeram-infinity-datasource"; then
    echo -e "${GREEN}✓ Infinity plugin is installed${NC}"
else
    echo -e "${YELLOW}⚠ Infinity plugin not found. Installing...${NC}"
    docker exec grafana grafana-cli plugins install yesoreyeram-infinity-datasource
    echo -e "${YELLOW}Restarting Grafana...${NC}"
    docker restart grafana
    sleep 10
    echo -e "${GREEN}✓ Infinity plugin installed${NC}"
fi

# Check Grafana API
echo -e "\n${YELLOW}5. Checking Grafana API...${NC}"
sleep 2
if curl -s http://localhost:3000/api/health | grep -q "ok"; then
    echo -e "${GREEN}✓ Grafana API is responding${NC}"
else
    echo -e "${RED}✗ Grafana API not responding${NC}"
fi

# Summary
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "Access Grafana at: ${GREEN}http://localhost:3000${NC}"
echo -e "Username: ${GREEN}admin${NC}"
echo -e "Password: ${GREEN}admin${NC}"
echo ""
echo -e "Dashboards will be automatically available!"
echo ""
