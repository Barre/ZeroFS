#!/bin/bash
# Test script for ZeroFS HA setup

set -e

echo "=== ZeroFS HA Test Script ==="
echo ""

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose not found. Please install docker-compose."
    exit 1
fi

# Check if we're in the examples directory
if [ ! -f "ha-docker-compose.yml" ]; then
    echo "Error: ha-docker-compose.yml not found. Please run this script from the examples/ directory."
    exit 1
fi

echo "1. Building Docker image..."
cd ..
docker build -t zerofs:latest -f Dockerfile .

echo ""
echo "2. Starting HA cluster..."
cd examples
docker-compose -f ha-docker-compose.yml up -d

echo ""
echo "3. Waiting for nodes to start (10 seconds)..."
sleep 10

echo ""
echo "4. Checking health endpoints..."
echo ""

for port in 8081 8082 8083; do
    echo -n "Node on port $port: "
    status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/health 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        echo "✓ Health check OK"
    else
        echo "✗ Health check failed (status: $status)"
    fi
done

echo ""
echo "5. Checking readiness endpoints (which node is active)..."
echo ""

active_node=""
for port in 8081 8082 8083; do
    echo -n "Node on port $port: "
    status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/ready 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        echo "✓ ACTIVE (ready for writes)"
        active_node=$port
    elif [ "$status" = "503" ]; then
        echo "○ STANDBY (read-only)"
    else
        echo "✗ Error (status: $status)"
    fi
done

echo ""
if [ -n "$active_node" ]; then
    echo "✓ Active node found on port $active_node"
    echo ""
    echo "6. Testing failover..."
    echo "   Stopping active node..."
    
    case $active_node in
        8081) docker-compose -f ha-docker-compose.yml stop zerofs-node1 ;;
        8082) docker-compose -f ha-docker-compose.yml stop zerofs-node2 ;;
        8083) docker-compose -f ha-docker-compose.yml stop zerofs-node3 ;;
    esac
    
    echo "   Waiting for failover (30 seconds)..."
    sleep 30
    
    echo ""
    echo "   Checking for new active node..."
    new_active=""
    for port in 8081 8082 8083; do
        status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/ready 2>/dev/null || echo "000")
        if [ "$status" = "200" ]; then
            new_active=$port
            echo "   ✓ New active node on port $port"
        fi
    done
    
    if [ -n "$new_active" ]; then
        echo ""
        echo "✓ Failover successful!"
    else
        echo ""
        echo "✗ Failover failed - no active node found"
    fi
else
    echo "✗ No active node found - HA may not be working correctly"
fi

echo ""
echo "=== Test Complete ==="
echo ""
echo "To view logs:"
echo "  docker-compose -f ha-docker-compose.yml logs -f"
echo ""
echo "To stop cluster:"
echo "  docker-compose -f ha-docker-compose.yml down"
echo ""


