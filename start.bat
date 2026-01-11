@echo off
echo ================================================
echo BIG DATA STOCK SYSTEM - STARTING
echo ================================================

echo.
echo 1. Stopping any running containers...
docker-compose down -v 2>nul

echo.
echo 2. Cleaning up...
docker system prune -f 2>nul

echo.
echo 3. Starting Hadoop and Redis...
docker-compose up -d namenode datanode redis

echo.
echo 4. Waiting for Hadoop to start (30 seconds)...
timeout /t 30 /nobreak >nul

echo.
echo 5. Initializing HDFS directories...
docker exec namenode hdfs dfs -mkdir -p /stock_data 2>nul
docker exec namenode hdfs dfs -mkdir -p /stock_data/raw 2>nul
docker exec namenode hdfs dfs -mkdir -p /stock_data/raw/daily 2>nul
docker exec namenode hdfs dfs -chmod -R 777 /stock_data 2>nul

echo.
echo 6. Starting data ingestion and Jupyter...
docker-compose up -d data-ingestion jupyter

echo.
echo 7. Waiting for services...
timeout /t 10 /nobreak >nul

echo.
echo ================================================
echo SYSTEM READY!
echo ================================================
echo.
echo Services:
echo ---------
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo.
echo URLs:
echo -----
echo Hadoop Web UI:  http://localhost:9870
echo Jupyter Lab:    http://localhost:8888
echo.
echo Data Collection: Running in container 'data-ingestion'
echo Check logs:      docker-compose logs data-ingestion
echo.
echo ================================================