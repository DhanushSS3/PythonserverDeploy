#!/bin/bash

# Exit on error
set -e

echo "=== FastAPI Trading App Deployment Script ==="
echo "This script will deploy the application to your Hostinger VPS"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Installing Docker..."
    sudo apt update
    sudo apt install -y docker.io docker-compose
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo usermod -aG docker $USER
    echo "Docker installed successfully. Please log out and log back in for group changes to take effect."
    echo "Then run this script again."
    exit 0
fi

echo "Docker is installed. Proceeding with deployment..."

# Create necessary directories
mkdir -p logs uploads

# Check if redis.conf exists
if [ ! -f "redis.conf" ]; then
    echo "redis.conf not found. Please make sure it exists in the current directory."
    exit 1
fi

# Check if .env file exists, create if not
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cat > .env << EOF
# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# JWT Configuration
SECRET_KEY=$(openssl rand -hex 32)
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# Firebase Configuration
FIREBASE_CREDENTIALS_FILE=path/to/your/firebase-credentials.json
FIREBASE_DATABASE_URL=https://your-project.firebaseio.com
FIREBASE_DATA_PATH=datafeeds
EOF
    echo ".env file created. Please edit it with your specific configuration."
    exit 0
fi

# Build and start the containers
echo "Building and starting containers..."
docker-compose up -d --build

# Check if containers are running
echo "Checking container status..."
docker-compose ps

echo "=== Deployment Complete ==="
echo "Your FastAPI application is now running at http://your-server-ip:8000"
echo "Redis is running with AOF persistence enabled"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f api"
echo ""
echo "To stop the application:"
echo "  docker-compose down"
echo ""
echo "To restart the application:"
echo "  docker-compose restart" 