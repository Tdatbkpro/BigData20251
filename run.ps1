# run.ps1 - Script đơn giản nhất
Write-Host "=== Starting Big Data System ===" -ForegroundColor Cyan

# 1. Dừng và xóa
Write-Host "1. Cleaning up..." -ForegroundColor Yellow
docker-compose down -v 2>$null

# 2. Khởi động Hadoop trước
Write-Host "2. Starting Hadoop..." -ForegroundColor Yellow
docker-compose up -d namenode datanode

# 3. Chờ namenode
Write-Host "3. Waiting for Hadoop..." -ForegroundColor Yellow
Start-Sleep -Seconds 20

# 4. Format HDFS (luôn format)
Write-Host "4. Formatting HDFS..." -ForegroundColor Yellow
docker exec namenode hdfs namenode -format -nonInteractive -force 2>$null

# 5. Khởi động lại
Write-Host "5. Restarting Hadoop..." -ForegroundColor Yellow
docker-compose restart namenode datanode
Start-Sleep -Seconds 15

# 6. Kiểm tra HDFS
Write-Host "6. Checking HDFS..." -ForegroundColor Yellow
$hdfsReady = $false
for ($i = 1; $i -le 5; $i++) {
    Write-Host "  Attempt $i/5..."
    $result = docker exec namenode hdfs dfsadmin -report 2>&1
    if ($result -like "*Live datanodes*") {
        $hdfsReady = $true
        Write-Host "  ✓ HDFS is ready!" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 10
}

if (-not $hdfsReady) {
    Write-Host "  ✗ HDFS failed to start" -ForegroundColor Red
    exit 1
}

# 7. Tạo directories
Write-Host "7. Creating HDFS directories..." -ForegroundColor Yellow
docker exec namenode hdfs dfs -mkdir -p /stock_data 2>$null
docker exec namenode hdfs dfs -mkdir -p /stock_data/raw 2>$null
docker exec namenode hdfs dfs -mkdir -p /stock_data/processed 2>$null
docker exec namenode hdfs dfs -chmod -R 777 /stock_data 2>$null

# 8. Khởi động tất cả services
Write-Host "8. Starting all services..." -ForegroundColor Yellow
docker-compose up -d

# 9. Kiểm tra
Write-Host "9. Checking services..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host "`n=== System Ready ===" -ForegroundColor Green
Write-Host "HDFS Web UI: http://localhost:9870"
Write-Host "Spark UI:    http://localhost:8080"
Write-Host "Jupyter:     http://localhost:8888"
Write-Host "Backend:     http://localhost:8000"
Write-Host "Frontend:    http://localhost:3000"
Write-Host "Redis:       localhost:6379"
Write-Host "PostgreSQL:  localhost:5432"
Write-Host "`nTo stop: docker-compose down" -ForegroundColor Yellow