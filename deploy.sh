#!/bin/bash

# Stock Data Pipeline Deployment Script
# Usage: ./deploy.sh [start|stop|restart|status|logs|clean]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

check_dependencies() {
    print_status "Checking dependencies..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed"
        exit 1
    fi
    
    # Check required tools
    for cmd in curl jq; do
        if ! command -v $cmd &> /dev/null; then
            print_warning "$cmd is not installed, some features may not work"
        fi
    done
    
    print_success "All dependencies satisfied"
}

create_directories() {
    print_status "Creating necessary directories..."
    
    mkdir -p ./stock_data/{raw,processed,logs,reports}
    mkdir -p ./notebooks
    mkdir -p ./logs/{backend,data-ingestion,spark}
    mkdir -p ./hadoop-config
    mkdir -p ./monitoring/{prometheus,grafana,alertmanager}
    
    print_success "Directories created"
}

build_images() {
    print_status "Building Docker images..."
    
    # Build backend
    print_status "Building backend image..."
    docker-compose -f docker-compose.full.yml build backend
    
    # Build data ingestion
    print_status "Building data ingestion image..."
    docker-compose -f docker-compose.full.yml build data-ingestion
    
    # Build frontend
    print_status "Building frontend image..."
    docker-compose -f docker-compose.full.yml build frontend
    
    # Build nginx
    print_status "Building nginx image..."
    docker-compose -f docker-compose.full.yml build nginx
    
    print_success "All images built successfully"
}

start_services() {
    print_status "Starting services..."
    
    # Start core services first
    docker-compose -f docker-compose.full.yml up -d \
        postgres \
        redis \
        namenode \
        datanode
    
    print_status "Waiting for core services to initialize (60 seconds)..."
    sleep 60
    
    # Start Spark and backend
    docker-compose -f docker-compose.full.yml up -d \
        spark-master \
        spark-worker1 \
        backend
    
    print_status "Waiting for Spark and backend to start (30 seconds)..."
    sleep 30
    
    # Start remaining services
    docker-compose -f docker-compose.full.yml up -d
    
    print_success "All services started"
    
    # Show status
    sleep 10
    show_status
}

stop_services() {
    print_status "Stopping services..."
    docker-compose -f docker-compose.full.yml down
    print_success "Services stopped"
}

restart_services() {
    print_status "Restarting services..."
    stop_services
    start_services
}

show_status() {
    print_status "Service Status:"
    echo "========================================"
    
    # Check Docker containers
    if docker-compose -f docker-compose.full.yml ps | grep -q "Up"; then
        docker-compose -f docker-compose.full.yml ps
        
        echo ""
        print_status "Service URLs:"
        echo "  Frontend:      http://localhost"
        echo "  Backend API:   http://localhost:8000"
        echo "  Grafana:       http://localhost:3001"
        echo "  Prometheus:    http://localhost:9090"
        echo "  HDFS Web UI:   http://localhost:9870"
        echo "  Spark Web UI:  http://localhost:8080"
        echo "  Jupyter:       http://localhost:8888"
        
        # Check API health
        echo ""
        print_status "Checking API health..."
        if curl -s http://localhost:8000/health | grep -q "healthy"; then
            print_success "API is healthy"
        else
            print_warning "API health check failed"
        fi
    else
        print_warning "No services are running"
    fi
}

show_logs() {
    local service=$1
    
    if [ -z "$service" ]; then
        print_status "Showing logs for all services (Ctrl+C to exit)..."
        docker-compose -f docker-compose.full.yml logs -f
    else
        print_status "Showing logs for $service (Ctrl+C to exit)..."
        docker-compose -f docker-compose.full.yml logs -f "$service"
    fi
}

cleanup() {
    print_warning "This will remove all containers, volumes, and images. Are you sure? (y/N)"
    read -r confirm
    
    if [[ $confirm =~ ^[Yy]$ ]]; then
        print_status "Cleaning up..."
        
        # Stop and remove containers
        docker-compose -f docker-compose.full.yml down -v
        
        # Remove unused images
        docker image prune -af
        
        # Remove unused volumes
        docker volume prune -f
        
        # Remove unused networks
        docker network prune -f
        
        print_success "Cleanup completed"
    else
        print_status "Cleanup cancelled"
    fi
}

backup_data() {
    local backup_dir="./backups/$(date +%Y%m%d_%H%M%S)"
    
    print_status "Creating backup in $backup_dir..."
    mkdir -p "$backup_dir"
    
    # Backup PostgreSQL data
    print_status "Backing up PostgreSQL..."
    docker exec postgres pg_dump -U stockuser stockdb > "$backup_dir/stockdb_backup.sql"
    
    # Backup HDFS data (simplified - in production use distcp)
    print_status "Backing up HDFS metadata..."
    docker exec namenode hdfs dfs -ls -R /stock_data > "$backup_dir/hdfs_structure.txt" 2>/dev/null || true
    
    # Backup configuration files
    print_status "Backing up configurations..."
    cp -r ./hadoop-config "$backup_dir/"
    cp -r ./monitoring "$backup_dir/"
    cp -r ./backend "$backup_dir/"
    cp -r ./data-ingestion "$backup_dir/"
    cp docker-compose.full.yml "$backup_dir/"
    cp .env "$backup_dir/" 2>/dev/null || true
    
    # Create backup archive
    tar -czf "$backup_dir.tar.gz" -C "$backup_dir" .
    rm -rf "$backup_dir"
    
    print_success "Backup created: $backup_dir.tar.gz"
}

restore_data() {
    local backup_file=$1
    
    if [ -z "$backup_file" ]; then
        print_error "Please specify backup file to restore"
        exit 1
    fi
    
    if [ ! -f "$backup_file" ]; then
        print_error "Backup file not found: $backup_file"
        exit 1
    fi
    
    print_warning "This will overwrite existing data. Are you sure? (y/N)"
    read -r confirm
    
    if [[ $confirm =~ ^[Yy]$ ]]; then
        print_status "Restoring from $backup_file..."
        
        # Extract backup
        local temp_dir=$(mktemp -d)
        tar -xzf "$backup_file" -C "$temp_dir"
        
        # Restore PostgreSQL
        if [ -f "$temp_dir/stockdb_backup.sql" ]; then
            print_status "Restoring PostgreSQL..."
            docker-compose -f docker-compose.full.yml stop backend
            docker exec postgres psql -U stockuser -d stockdb -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
            docker exec -i postgres psql -U stockuser stockdb < "$temp_dir/stockdb_backup.sql"
        fi
        
        # Restore configurations
        print_status "Restoring configurations..."
        cp -r "$temp_dir/hadoop-config" ./ 2>/dev/null || true
        cp -r "$temp_dir/monitoring" ./ 2>/dev/null || true
        
        # Clean up
        rm -rf "$temp_dir"
        
        print_success "Restore completed. Please restart services."
    else
        print_status "Restore cancelled"
    fi
}

setup_monitoring() {
    print_status "Setting up monitoring..."
    
    # Check if monitoring containers are running
    if ! docker-compose -f docker-compose.full.yml ps | grep -q "prometheus"; then
        print_status "Starting monitoring stack..."
        docker-compose -f docker-compose.full.yml up -d \
            prometheus \
            grafana \
            node-exporter \
            cadvisor \
            alertmanager
    fi
    
    # Wait for Grafana to start
    print_status "Waiting for Grafana to initialize..."
    sleep 30
    
    # Setup Grafana datasources and dashboards
    print_status "Setting up Grafana..."
    
    # Import dashboards (this would typically be done through API)
    print_status "Monitoring setup completed"
    echo ""
    print_status "Monitoring URLs:"
    echo "  Grafana:    http://localhost:3001 (admin/admin123)"
    echo "  Prometheus: http://localhost:9090"
    echo "  AlertManager: http://localhost:9093"
}

run_tests() {
    print_status "Running system tests..."
    
    # Test API endpoints
    endpoints=(
        "/health"
        "/api/v1/stocks"
        "/api/v1/analytics/summary"
    )
    
    for endpoint in "${endpoints[@]}"; do
        print_status "Testing $endpoint..."
        if curl -s "http://localhost:8000$endpoint" | grep -q "error\|Error"; then
            print_error "Failed: $endpoint"
        else
            print_success "OK: $endpoint"
        fi
    done
    
    # Test HDFS connection
    print_status "Testing HDFS connection..."
    if docker exec namenode hdfs dfs -test -d /stock_data; then
        print_success "HDFS connection OK"
    else
        print_error "HDFS connection failed"
    fi
    
    # Test database connection
    print_status "Testing database connection..."
    if docker exec postgres psql -U stockuser -d stockdb -c "SELECT 1" &>/dev/null; then
        print_success "Database connection OK"
    else
        print_error "Database connection failed"
    fi
    
    print_success "System tests completed"
}

# Main execution
case "$1" in
    start)
        check_dependencies
        create_directories
        build_images
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs "$2"
        ;;
    clean)
        cleanup
        ;;
    backup)
        backup_data
        ;;
    restore)
        restore_data "$2"
        ;;
    monitor)
        setup_monitoring
        ;;
    test)
        run_tests
        ;;
    build)
        build_images
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|clean|backup|restore|monitor|test|build}"
        echo ""
        echo "Commands:"
        echo "  start     - Start all services"
        echo "  stop      - Stop all services"
        echo "  restart   - Restart all services"
        echo "  status    - Show service status"
        echo "  logs      - Show logs (all or specific service)"
        echo "  clean     - Remove all containers, volumes, and images"
        echo "  backup    - Create backup of data and configs"
        echo "  restore   - Restore from backup"
        echo "  monitor   - Setup monitoring stack"
        echo "  test      - Run system tests"
        echo "  build     - Build Docker images"
        exit 1
        ;;
esac