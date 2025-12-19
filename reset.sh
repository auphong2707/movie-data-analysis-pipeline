#!/bin/bash
# =============================================================================
# Movie Data Analysis Pipeline - Complete Reset Script
# =============================================================================
# This script stops and removes ALL containers, volumes, and networks.
# WARNING: This will DELETE all data permanently!
#
# Usage: ./reset.sh
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${RED}=========================================${NC}"
echo -e "${RED}WARNING: Complete Pipeline Reset${NC}"
echo -e "${RED}=========================================${NC}"
echo ""
echo -e "${YELLOW}This will permanently delete:${NC}"
echo "  • All Docker containers"
echo "  • All Docker volumes (MongoDB data, Kafka data, etc.)"
echo "  • All Docker networks"
echo "  • All pipeline data"
echo ""
echo -e "${RED}This action CANNOT be undone!${NC}"
echo ""
read -p "Are you sure you want to continue? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Reset cancelled."
    exit 0
fi

echo ""
echo -e "${YELLOW}Stopping and removing all containers...${NC}"
docker-compose down

echo ""
echo -e "${YELLOW}Removing all volumes...${NC}"
docker-compose down -v

echo ""
echo -e "${YELLOW}Removing orphaned containers...${NC}"
docker-compose down --remove-orphans

echo ""
echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}Pipeline reset complete!${NC}"
echo -e "${GREEN}=========================================${NC}"
echo ""
echo "All containers, volumes, and networks have been removed."
echo ""
echo "To start fresh, run: ./start.sh"
