#!/bin/bash
# Quick HA cluster status check

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║           ZeroFS HA Cluster Status Check                ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# Check if containers are running
echo -e "${BLUE}📦 Container Status:${NC}"
docker compose -f examples/ha-docker-compose.yml ps 2>&1 | grep -v "attribute.*version.*obsolete" | grep -E "(zerofs-node|s3-proxy)" | while read line; do
    if echo "$line" | grep -q "Up.*healthy"; then
        echo -e "  ${GREEN}✓${NC} $(echo $line | awk '{print $1}')"
    elif echo "$line" | grep -q "Up"; then
        echo -e "  ${YELLOW}⚠${NC} $(echo $line | awk '{print $1}') (starting...)"
    else
        echo -e "  ${RED}✗${NC} $(echo $line | awk '{print $1}')"
    fi
done

echo ""
echo -e "${BLUE}🎯 Node Status:${NC}"

# Check each node
active_count=0
for i in 1 2 3; do
    port=$((8080 + i))
    status=$(curl -s http://localhost:$port/ready 2>/dev/null)
    http_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/ready 2>/dev/null)
    
    echo -n "  Node $i (port $port): "
    if [ "$http_code" = "200" ]; then
        echo -e "${GREEN}🟢 ACTIVE${NC}"
        active_count=$((active_count + 1))
    elif [ "$http_code" = "503" ]; then
        echo -e "${YELLOW}🟡 STANDBY${NC}"
    else
        echo -e "${RED}🔴 ERROR${NC} (HTTP $http_code)"
    fi
done

echo ""
echo -e "${BLUE}🔍 Cluster Health:${NC}"

if [ $active_count -eq 1 ]; then
    echo -e "  ${GREEN}✓${NC} Exactly one active node (healthy)"
elif [ $active_count -eq 0 ]; then
    echo -e "  ${RED}✗${NC} No active node (cluster down!)"
else
    echo -e "  ${RED}✗${NC} Multiple active nodes (SPLIT-BRAIN!)"
fi

# Check recent logs for errors
echo ""
echo -e "${BLUE}📝 Recent Activity:${NC}"
for node in node1 node2 node3; do
    service="zerofs-${node}"
    
    # Check for lease acquisition
    acquired=$(docker compose -f examples/ha-docker-compose.yml logs --tail=20 $service 2>&1 | \
        grep -v "attribute.*version.*obsolete" | \
        grep -c "Successfully acquired lease" || echo "0")
    
    # Check for promotion
    promoted=$(docker compose -f examples/ha-docker-compose.yml logs --tail=20 $service 2>&1 | \
        grep -v "attribute.*version.*obsolete" | \
        grep -c "Promotion to ACTIVE complete" || echo "0")
    
    # Check for errors
    errors=$(docker compose -f examples/ha-docker-compose.yml logs --tail=50 $service 2>&1 | \
        grep -v "attribute.*version.*obsolete" | \
        grep -ci "error\|panic\|fatal" || echo "0")
    
    if [ "$acquired" != "0" ] || [ "$promoted" != "0" ]; then
        echo -e "  ${GREEN}✓${NC} $node: Recent activity (acquired=$acquired, promoted=$promoted)"
    fi
    
    if [ "$errors" != "0" ]; then
        echo -e "  ${YELLOW}⚠${NC} $node: $errors errors in recent logs"
    fi
done

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Use './test_ha.sh' to run comprehensive tests          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

