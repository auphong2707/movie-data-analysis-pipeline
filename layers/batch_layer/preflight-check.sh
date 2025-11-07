#!/bin/bash
# ========================================
# Batch Layer Pre-Flight Check
# ========================================
# Verifies prerequisites before starting
# ========================================

set -e

echo "=========================================="
echo "Batch Layer Pre-Flight Check"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

ERRORS=0
WARNINGS=0

# Check Docker
echo -n "Checking Docker... "
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version 2>/dev/null || sudo docker --version 2>/dev/null || echo "unknown")
    echo -e "${GREEN}✓${NC} $DOCKER_VERSION"
else
    echo -e "${RED}✗${NC} Docker not found"
    ERRORS=$((ERRORS+1))
fi

# Check Docker Compose
echo -n "Checking Docker Compose... "
if docker compose version &> /dev/null || sudo docker compose version &> /dev/null; then
    COMPOSE_VERSION=$(docker compose version 2>/dev/null || sudo docker compose version 2>/dev/null)
    echo -e "${GREEN}✓${NC} $COMPOSE_VERSION"
else
    echo -e "${RED}✗${NC} Docker Compose not found"
    ERRORS=$((ERRORS+1))
fi

# Check Docker daemon
echo -n "Checking Docker daemon... "
if docker ps &> /dev/null; then
    echo -e "${GREEN}✓${NC} Docker daemon running (no sudo needed)"
elif sudo docker ps &> /dev/null; then
    echo -e "${YELLOW}⚠${NC} Docker daemon running (requires sudo)"
    WARNINGS=$((WARNINGS+1))
else
    echo -e "${RED}✗${NC} Docker daemon not running"
    ERRORS=$((ERRORS+1))
fi

# Check disk space (need at least 20GB free)
echo -n "Checking disk space... "
AVAILABLE_GB=$(df -BG . | awk 'NR==2 {print $4}' | sed 's/G//')
if [ "$AVAILABLE_GB" -ge 20 ]; then
    echo -e "${GREEN}✓${NC} ${AVAILABLE_GB}GB available"
else
    echo -e "${YELLOW}⚠${NC} ${AVAILABLE_GB}GB available (recommend 20GB+)"
    WARNINGS=$((WARNINGS+1))
fi

# Check required files
echo -n "Checking configuration files... "
if [ -f "docker-compose.batch.yml" ] && [ -f ".env" ] && [ -f "Makefile" ]; then
    echo -e "${GREEN}✓${NC} All files present"
else
    echo -e "${RED}✗${NC} Missing files"
    ERRORS=$((ERRORS+1))
fi

# Check Docker files
echo -n "Checking Docker files... "
if [ -f "docker/spark.Dockerfile" ] && [ -f "docker/airflow.Dockerfile" ]; then
    echo -e "${GREEN}✓${NC} Dockerfiles present"
else
    echo -e "${RED}✗${NC} Missing Dockerfiles"
    ERRORS=$((ERRORS+1))
fi

# Check init scripts
echo -n "Checking init scripts... "
if [ -f "docker/airflow-init.sh" ] && [ -f "docker/mongo-init.js" ]; then
    if [ -x "docker/airflow-init.sh" ]; then
        echo -e "${GREEN}✓${NC} Init scripts present and executable"
    else
        echo -e "${YELLOW}⚠${NC} airflow-init.sh not executable"
        chmod +x docker/airflow-init.sh
        echo -e "${GREEN}✓${NC} Fixed permissions"
    fi
else
    echo -e "${RED}✗${NC} Missing init scripts"
    ERRORS=$((ERRORS+1))
fi

# Check TMDB API key
echo -n "Checking TMDB API key... "
if grep -q "TMDB_API_KEY=36bdc639ae379da0a89bfb9c556e2136" .env 2>/dev/null; then
    echo -e "${GREEN}✓${NC} API key configured"
else
    echo -e "${YELLOW}⚠${NC} API key not found in .env"
    WARNINGS=$((WARNINGS+1))
fi

# Check for port conflicts
echo -n "Checking port availability... "
PORTS_IN_USE=""
for port in 8080 8081 9870 27017 8082; do
    if sudo lsof -i :$port &> /dev/null; then
        PORTS_IN_USE="$PORTS_IN_USE $port"
    fi
done

if [ -z "$PORTS_IN_USE" ]; then
    echo -e "${GREEN}✓${NC} All ports available"
else
    echo -e "${YELLOW}⚠${NC} Ports in use:$PORTS_IN_USE"
    echo "    You may need to stop conflicting services or change ports"
    WARNINGS=$((WARNINGS+1))
fi

# Summary
echo ""
echo "=========================================="
echo "Pre-Flight Check Summary"
echo "=========================================="

if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed!${NC}"
    echo ""
    echo "Ready to start. Run:"
    echo "  make up"
    exit 0
elif [ $ERRORS -eq 0 ]; then
    echo -e "${YELLOW}⚠ $WARNINGS warning(s) found${NC}"
    echo ""
    echo "You can proceed, but review the warnings above."
    echo "Run: make up"
    exit 0
else
    echo -e "${RED}✗ $ERRORS error(s) found${NC}"
    if [ $WARNINGS -gt 0 ]; then
        echo -e "${YELLOW}⚠ $WARNINGS warning(s) found${NC}"
    fi
    echo ""
    echo "Please fix the errors before starting."
    exit 1
fi
