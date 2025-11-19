#!/bin/bash
# Script to check ZeroFS HA cluster status

echo "=========================================="
echo "ZeroFS HA Cluster Status Check"
echo "=========================================="
echo ""

echo "=== Container Status ==="
cd "$(dirname "$0")"
docker compose -f ha-docker-compose.yml ps
echo ""

echo "=== Health Check (all should return OK) ==="
for port in 8081 8082 8083; do
    node_num=$((port - 8080))
    health=$(curl -s http://localhost:$port/health 2>/dev/null || echo "FAILED")
    echo "Node $node_num (port $port): $health"
done
echo ""

echo "=== Readiness Check (Active/Standby Status) ==="
active_count=0
standby_count=0
for port in 8081 8082 8083; do
    node_num=$((port - 8080))
    status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/ready 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        echo "Node $node_num (port $port): ✓ ACTIVE (ready for writes)"
        active_count=$((active_count + 1))
    elif [ "$status" = "503" ]; then
        echo "Node $node_num (port $port): ○ STANDBY (read-only)"
        standby_count=$((standby_count + 1))
    else
        echo "Node $node_num (port $port): ✗ Error (status: $status)"
    fi
done
echo ""

if [ "$active_count" -eq 1 ] && [ "$standby_count" -eq 2 ]; then
    echo "✅ SUCCESS: HA cluster is working correctly!"
    echo "   - 1 Active node (handling writes)"
    echo "   - 2 Standby nodes (ready for failover)"
elif [ "$active_count" -eq 0 ]; then
    echo "⚠️  WARNING: No active node found"
    echo "   Checking logs..."
    echo ""
    echo "=== Recent Logs from Node 1 ==="
    docker compose -f ha-docker-compose.yml logs --tail=20 zerofs-node1 | grep -E "lease|Lease|Active|Standby|promoting|coordinator|Health server" || docker compose -f ha-docker-compose.yml logs --tail=10 zerofs-node1
else
    echo "⚠️  WARNING: Unexpected state"
    echo "   Active nodes: $active_count"
    echo "   Standby nodes: $standby_count"
fi
echo ""

echo "=== Lease Coordinator Logs (Node 1) ==="
docker compose -f ha-docker-compose.yml logs --tail=30 zerofs-node1 | grep -i "lease\|coordinator\|active\|standby" | tail -10
echo ""

echo "=== Useful Commands ==="
echo "View all logs:"
echo "  docker compose -f ha-docker-compose.yml logs -f"
echo ""
echo "View specific node logs:"
echo "  docker compose -f ha-docker-compose.yml logs -f zerofs-node1"
echo ""
echo "Test failover (stop active node):"
echo "  docker compose -f ha-docker-compose.yml stop zerofs-node1"
echo "  # Wait 30-40 seconds, then run this script again"


