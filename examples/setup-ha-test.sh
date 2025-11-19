#!/bin/bash
# Setup script for ZeroFS HA testing

set -e

echo "=== ZeroFS HA Test Setup ==="
echo ""

# Check if we're in the examples directory
if [ ! -f "ha-docker-compose.yml" ]; then
    echo "Error: Please run this script from the examples/ directory"
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    echo ""
    echo "Please provide the following information:"
    echo ""
    
    read -p "S3 Bucket URL (e.g., s3://my-bucket/zerofs-data): " S3_URL
    read -sp "ZeroFS Encryption Password: " ZEROFS_PASSWORD
    echo ""
    read -p "AWS Access Key ID: " AWS_ACCESS_KEY_ID
    read -sp "AWS Secret Access Key: " AWS_SECRET_ACCESS_KEY
    echo ""
    read -p "AWS Region [us-east-1]: " AWS_DEFAULT_REGION
    AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}
    
    # Create .env file
    cat > .env <<EOF
# ZeroFS HA Test Configuration
S3_URL=${S3_URL}
ZEROFS_PASSWORD=${ZEROFS_PASSWORD}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}
EOF
    
    echo ""
    echo "✓ .env file created"
else
    echo "✓ .env file already exists"
    source .env
fi

# Update config file with S3 URL
if [ -n "$S3_URL" ]; then
    echo ""
    echo "Updating ha-config-lease.toml with S3 URL..."
    # Use sed to replace the S3 URL in the config file
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s|url = \"s3://.*\"|url = \"${S3_URL}\"|" ha-config-lease.toml
        sed -i '' "s|default_region = \".*\"|default_region = \"${AWS_DEFAULT_REGION}\"|" ha-config-lease.toml
    else
        # Linux
        sed -i "s|url = \"s3://.*\"|url = \"${S3_URL}\"|" ha-config-lease.toml
        sed -i "s|default_region = \".*\"|default_region = \"${AWS_DEFAULT_REGION}\"|" ha-config-lease.toml
    fi
    echo "✓ Config file updated"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "1. Build Docker image:"
echo "   cd .. && docker build -t zerofs:latest -f Dockerfile ."
echo ""
echo "2. Start the cluster:"
echo "   cd examples"
echo "   docker-compose -f ha-docker-compose.yml --env-file .env up -d"
echo ""
echo "3. Check health endpoints:"
echo "   curl http://localhost:8081/ready  # Node 1"
echo "   curl http://localhost:8082/ready  # Node 2"
echo "   curl http://localhost:8083/ready  # Node 3"
echo ""
echo "4. View logs:"
echo "   docker-compose -f ha-docker-compose.yml logs -f"
echo ""


