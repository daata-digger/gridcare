#!/bin/bash

# GridCARE Live Dashboard Deployment Script
# Author: GridCARE.ai
# Description: Automated deployment script for GridCARE Live Dashboard

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DASHBOARD_PORT=${DASHBOARD_PORT:-8080}
API_URL=${API_URL:-http://api:8000}
CONTAINER_NAME="gridcare-dashboard"
IMAGE_NAME="gridcare-dashboard:latest"

echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════════════════╗"
echo "║        GridCARE Live Dashboard Deployment Script          ║"
echo "║                    Version 1.0                             ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Check if Docker is installed
print_message "$YELLOW" "→ Checking prerequisites..."
if ! command -v docker &> /dev/null; then
    print_message "$RED" "✗ Docker is not installed. Please install Docker first."
    exit 1
fi
print_message "$GREEN" "✓ Docker is installed"

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    print_message "$RED" "✗ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi
print_message "$GREEN" "✓ Docker Compose is installed"

# Stop existing container if running
print_message "$YELLOW" "\n→ Checking for existing containers..."
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    print_message "$YELLOW" "✓ Found existing container. Stopping and removing..."
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    print_message "$GREEN" "✓ Existing container removed"
else
    print_message "$GREEN" "✓ No existing container found"
fi

# Build the Docker image
print_message "$YELLOW" "\n→ Building Docker image..."
docker build -t $IMAGE_NAME . || {
    print_message "$RED" "✗ Failed to build Docker image"
    exit 1
}
print_message "$GREEN" "✓ Docker image built successfully"

# Start the container
print_message "$YELLOW" "\n→ Starting GridCARE Dashboard..."
docker run -d \
    --name $CONTAINER_NAME \
    -p $DASHBOARD_PORT:8080 \
    -e API_URL=$API_URL \
    --restart unless-stopped \
    $IMAGE_NAME || {
    print_message "$RED" "✗ Failed to start container"
    exit 1
}

# Wait for container to be ready
print_message "$YELLOW" "\n→ Waiting for dashboard to be ready..."
sleep 5

# Check if container is running
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    print_message "$GREEN" "✓ Dashboard is running"
    
    # Display container status
    echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}✓ Deployment successful!${NC}\n"
    echo -e "${BLUE}Dashboard URL:${NC} http://localhost:$DASHBOARD_PORT"
    echo -e "${BLUE}API URL:${NC} $API_URL"
    echo -e "${BLUE}Container Name:${NC} $CONTAINER_NAME"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
    
    # Show useful commands
    echo -e "${YELLOW}Useful commands:${NC}"
    echo -e "  ${BLUE}View logs:${NC}       docker logs -f $CONTAINER_NAME"
    echo -e "  ${BLUE}Stop dashboard:${NC}  docker stop $CONTAINER_NAME"
    echo -e "  ${BLUE}Start dashboard:${NC} docker start $CONTAINER_NAME"
    echo -e "  ${BLUE}Remove dashboard:${NC} docker rm -f $CONTAINER_NAME"
    echo -e "  ${BLUE}Health check:${NC}    curl http://localhost:$DASHBOARD_PORT/health"
    echo ""
    
    # Offer to open browser
    if command -v open &> /dev/null; then
        read -p "Open dashboard in browser? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            open "http://localhost:$DASHBOARD_PORT"
        fi
    fi
else
    print_message "$RED" "✗ Container failed to start"
    print_message "$YELLOW" "Showing container logs:"
    docker logs $CONTAINER_NAME
    exit 1
fi

print_message "$GREEN" "\n✓ Deployment complete! Dashboard is accessible at http://localhost:$DASHBOARD_PORT"