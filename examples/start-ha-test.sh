#!/bin/bash
# Quick start script for ZeroFS HA testing with Wasabi

set -e

echo "=== ZeroFS HA Test - Wasabi Configuration ==="
echo ""

cd "$(dirname "$0")"

# Check if Docker image exists
if ! docker image inspect zerofs:latest &>/dev/null; then
    echo "Building Docker image..."
    cd ..
    docker build -t zerofs:latest -f Dockerfile .
    cd examples
    echo "✓ Docker image built"
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "Creating .env file with Wasabi credentials..."
    cat > .env <<EOF
# ZeroFS HA Test Configuration for Wasabi
S3_URL=s3://sb-zerofile-ha/zerofs-data
ZEROFS_PASSWORD=test-password-123
AWS_ACCESS_KEY_ID=X7SMWFBIMHK761MZDCM4
AWS_SECRET_ACCESS_KEY=HeCjI9zsWe6lemh42fmCCugfyF06f7zXlyb9VY0G
AWS_DEFAULT_REGION=us-east-1
WASABI_ENDPOINT=https://s3.wasabisys.com
EOF
    echo "✓ .env file created"
    echo ""
    echo "⚠️  WARNING: Using default password 'test-password-123'"
    echo "   Please change ZEROFS_PASSWORD in .env for production use!"
    echo ""
fi

echo "Starting ZeroFS HA cluster..."
docker-compose -f ha-docker-compose.yml --env-file .env up -d

echo ""
echo "Waiting for nodes to start (10 seconds)..."
sleep 10

echo ""
echo "=== Health Check ==="
echo ""

for port in 8081 8082 8083; do
    node_num=$((port - 8080))
    echo -n "Node $node_num (port $port): "
    status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/health 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        echo "✓ Health OK"
    else
        echo "✗ Health check failed (status: $status)"
    fi
done

echo ""
echo "=== Readiness Check (Active/Standby Status) ==="
echo ""

active_node=""
for port in 8081 8082 8083; do
    node_num=$((port - 8080))
    echo -n "Node $node_num (port $port): "
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
    echo "=== Cluster Status: READY ==="
else
    echo "⚠️  No active node found yet - cluster may still be initializing"
    echo "   Check logs: docker-compose -f ha-docker-compose.yml logs"
fi

echo ""
echo "=== Useful Commands ==="
echo ""
echo "View logs:"
echo "  docker-compose -f ha-docker-compose.yml logs -f"
echo ""
echo "Check specific node:"
echo "  docker-compose -f ha-docker-compose.yml logs -f zerofs-node1"
echo ""
echo "Stop cluster:"
echo "  docker-compose -f ha-docker-compose.yml down"
echo ""
echo "Test failover (stop active node):"
echo "  docker-compose -f ha-docker-compose.yml stop zerofs-node1"
echo "  # Wait 30 seconds, then check which node became active"
echo ""


