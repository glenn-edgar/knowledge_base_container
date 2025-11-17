#!/bin/bash

# ML Container Build and Run Script
# This script builds and runs the ML container without docker-compose

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
IMAGE_NAME="nanodatacenter/python-ml"
CONTAINER_NAME="python-ml"
PORT="8000"

echo -e "${BLUE}🐳 ML Container Build and Run Script${NC}"
echo "=================================="

# Function to check if container is running
check_container() {
    docker ps -q -f name=$CONTAINER_NAME
}

# Function to check if container exists (running or stopped)
check_container_exists() {
    docker ps -aq -f name=$CONTAINER_NAME
}

# Function to build the image
build_image() {
    echo -e "${YELLOW}🔨 Building Docker image...${NC}"
    docker build -t $IMAGE_NAME .
    echo -e "${GREEN}✅ Image built successfully!${NC}"
}

# Function to stop and remove existing container
cleanup_container() {
    if [ "$(check_container_exists)" ]; then
        echo -e "${YELLOW}🧹 Stopping and removing existing container...${NC}"
        docker stop $CONTAINER_NAME 2>/dev/null || true
        docker rm $CONTAINER_NAME 2>/dev/null || true
        echo -e "${GREEN}✅ Container cleaned up!${NC}"
    fi
}

# Function to run the container
run_container() {
    echo -e "${YELLOW}🚀 Starting container...${NC}"
    
    # Create data and models directories if they don't exist
    mkdir -p data models
    
    # Run the container with volume mounts
    # Disable Docker logging AND suppress terminal output
    docker run -d \
        --name $CONTAINER_NAME \
        --rm \
        --log-driver=none \
        -p $PORT:8000 \
        -v "$(pwd)/data:/app/data" \
        -v "$(pwd)/models:/app/models" \
        -e PYTHONPATH=/app \
        $IMAGE_NAME > /dev/null 2>&1
    
    echo -e "${GREEN}✅ Container started successfully!${NC}"
    echo -e "${BLUE}🌐 Application is running at: http://localhost:$PORT${NC}"
}

# Function to show container status
show_status() {
    echo -e "${BLUE}📊 Container Status:${NC}"
    if [ "$(check_container)" ]; then
        echo -e "${GREEN}✅ Container is running${NC}"
        echo -e "${BLUE}🌐 Access the app at: http://localhost:$PORT${NC}"
        echo -e "${BLUE}📖 API endpoints:${NC}"
        echo "  • GET  / - Home page"
        echo "  • GET  /test - Test all libraries"
        echo "  • GET  /health - Health check"
        echo "  • POST /predict - Make predictions"
        echo "  • GET  /plot - Generate sample plot"
    else
        echo -e "${RED}❌ Container is not running${NC}"
    fi
}

# Function to show logs
show_logs() {
    if [ "$(check_container_exists)" ]; then
        echo -e "${BLUE}📋 Container logs:${NC}"
        docker logs $CONTAINER_NAME
    else
        echo -e "${RED}❌ Container does not exist${NC}"
    fi
}

# Function to follow logs
follow_logs() {
    if [ "$(check_container)" ]; then
        echo -e "${BLUE}📋 Following container logs (Ctrl+C to stop):${NC}"
        docker logs -f $CONTAINER_NAME
    else
        echo -e "${RED}❌ Container is not running${NC}"
    fi
}

# Function to stop container
stop_container() {
    if [ "$(check_container)" ]; then
        echo -e "${YELLOW}⏹️  Stopping container...${NC}"
        docker stop $CONTAINER_NAME
        echo -e "${GREEN}✅ Container stopped${NC}"
    else
        echo -e "${YELLOW}ℹ️  Container is not running${NC}"
    fi
}

# Function to start existing container
start_container() {
    if [ "$(check_container_exists)" ] && [ ! "$(check_container)" ]; then
        echo -e "${YELLOW}▶️  Starting existing container...${NC}"
        docker start $CONTAINER_NAME
        echo -e "${GREEN}✅ Container started${NC}"
        echo -e "${BLUE}🌐 Application is running at: http://localhost:$PORT${NC}"
    elif [ "$(check_container)" ]; then
        echo -e "${YELLOW}ℹ️  Container is already running${NC}"
    else
        echo -e "${RED}❌ No existing container found. Use 'build' or 'run' first.${NC}"
    fi
}

# Function to enter container shell
enter_container() {
    if [ "$(check_container)" ]; then
        echo -e "${BLUE}🐚 Entering container shell...${NC}"
        docker exec -it $CONTAINER_NAME bash
    else
        echo -e "${RED}❌ Container is not running${NC}"
    fi
}

# Function to test the API
test_api() {
    if [ "$(check_container)" ]; then
        echo -e "${BLUE}🧪 Testing API endpoints...${NC}"
        
        echo -e "${YELLOW}Testing health endpoint...${NC}"
        curl -s http://localhost:$PORT/health | python3 -m json.tool
        
        echo -e "\n${YELLOW}Testing library tests...${NC}"
        curl -s http://localhost:$PORT/test | python3 -m json.tool
        
    else
        echo -e "${RED}❌ Container is not running${NC}"
    fi
}

# Main script logic
case "${1:-help}" in
    "build")
        build_image
        ;;
    "build-only")
        build_image
        ;;
    "run")
        cleanup_container
        run_container
        show_status
        ;;
    "start")
        start_container
        ;;
    "stop")
        stop_container
        ;;
    "restart")
        stop_container
        start_container
        ;;
    "status")
        show_status
        ;;
    "logs")
        show_logs
        ;;
    "follow")
        follow_logs
        ;;
    "shell")
        enter_container
        ;;
    "test")
        test_api
        ;;
    "clean")
        cleanup_container
        echo -e "${YELLOW}🧹 Removing Docker image...${NC}"
        docker rmi $IMAGE_NAME 2>/dev/null || true
        echo -e "${GREEN}✅ Cleanup complete!${NC}"
        ;;
    "help"|*)
        echo -e "${BLUE}Usage: $0 {command}${NC}"
        echo ""
        echo "Commands:"
        echo "  build       - Build the Docker image only"
        echo "  build-only  - Build the Docker image only (alias for build)"
        echo "  run         - Run the container (assumes image is built)"
        echo "  start       - Start existing container"
        echo "  stop        - Stop the container"
        echo "  restart     - Restart the container"
        echo "  status      - Show container status"
        echo "  logs        - Show container logs"
        echo "  follow      - Follow container logs"
        echo "  shell       - Enter container shell"
        echo "  test        - Test API endpoints"
        echo "  clean       - Stop container and remove image"
        echo "  help        - Show this help message"
        echo ""
        echo "Example workflow:"
        echo "  ./build.sh build    # Build the image"
        echo "  ./build.sh run      # Run the container"
        ;;
esac
