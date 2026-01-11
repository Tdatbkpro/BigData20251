#!/bin/bash

# Batch job script for Spark processing

echo "=========================================="
echo "STOCK DATA PROCESSING BATCH JOB"
echo "Start time: $(date)"
echo "=========================================="

# Set environment variables
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Step 1: Run data processing
echo ""
echo "[STEP 1] Running data processing..."
python /opt/spark-apps/data_processor.py

if [ $? -eq 0 ]; then
    echo "✓ Data processing completed"
else
    echo "✗ Data processing failed"
    exit 1
fi

# Step 2: Run stock analysis
echo ""
echo "[STEP 2] Running stock analysis..."
python /opt/spark-apps/stock_analysis.py

if [ $? -eq 0 ]; then
    echo "✓ Stock analysis completed"
else
    echo "✗ Stock analysis failed"
    exit 1
fi

echo ""
echo "=========================================="
echo "BATCH JOB COMPLETED SUCCESSFULLY"
echo "End time: $(date)"
echo "=========================================="

exit 0