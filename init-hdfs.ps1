# init-hdfs.ps1 - PowerShell script for Windows

Write-Host "=== Initializing HDFS ===" -ForegroundColor Cyan

# 1. Khởi động namenode và datanode trước
Write-Host "1. Starting Hadoop containers..." -ForegroundColor Yellow
docker-compose up -d namenode datanode

Write-Host "2. Waiting for namenode to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# 3. Kiểm tra namenode logs
Write-Host "3. Checking namenode logs..." -ForegroundColor Yellow
docker-compose logs namenode

# 4. Format HDFS nếu cần
Write-Host "4. Formatting HDFS if needed..." -ForegroundColor Yellow
docker-compose exec namenode bash -c "
if [ ! -d /hadoop/dfs/name/current ]; then
  echo 'Formatting HDFS...'
  hdfs namenode -format -nonInteractive -force
  echo 'Format completed'
else
  echo 'HDFS already formatted'
fi
"

# 5. Restart services
Write-Host "5. Restarting Hadoop services..." -ForegroundColor Yellow
docker-compose restart namenode
Start-Sleep -Seconds 10
docker-compose restart datanode
Start-Sleep -Seconds 20

# 6. Kiểm tra HDFS status
Write-Host "6. Checking HDFS status..." -ForegroundColor Yellow
$attempts = 0
$maxAttempts = 10

do {
    $attempts++
    Write-Host "Attempt $attempts/$maxAttempts to connect to HDFS..."
    
    $result = docker-compose exec namenode hdfs dfsadmin -report 2>&1
    if ($result -like "*Live datanodes*") {
        Write-Host "✓ HDFS is ready!" -ForegroundColor Green
        break
    }
    
    if ($attempts -lt $maxAttempts) {
        Write-Host "HDFS not ready yet, waiting 10 seconds..." -ForegroundColor Yellow
        Start-Sleep -Seconds 10
    } else {
        Write-Host "✗ HDFS failed to start after $maxAttempts attempts" -ForegroundColor Red
        exit 1
    }
} while ($attempts -lt $maxAttempts)

# 7. Tạo directories
Write-Host "7. Creating HDFS directories..." -ForegroundColor Yellow
docker-compose exec namenode hdfs dfs -mkdir -p /stock_data
docker-compose exec namenode hdfs dfs -mkdir -p /stock_data/raw
docker-compose exec namenode hdfs dfs -mkdir -p /stock_data/processed
docker-compose exec namenode hdfs dfs -mkdir -p /stock_data/analytics
docker-compose exec namenode hdfs dfs -chmod -R 777 /stock_data

# 8. Kiểm tra kết quả
Write-Host "8. Final HDFS status..." -ForegroundColor Yellow
docker-compose exec namenode hdfs dfsadmin -report
docker-compose exec namenode hdfs dfs -ls /

Write-Host "`n=== HDFS Initialization Complete ===" -ForegroundColor Green
Write-Host "HDFS Web UI: http://localhost:9870" -ForegroundColor Cyan
Write-Host "Spark UI: http://localhost:8080" -ForegroundColor Cyan
Write-Host "Jupyter: http://localhost:8888" -ForegroundColor Cyan