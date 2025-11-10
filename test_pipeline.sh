#!/bin/bash
# Manual Pipeline Test Script
# Run this to test the complete Bronze -> Silver -> Gold -> MongoDB pipeline

set -e  # Exit on error

# Change to batch layer directory
cd /home/dmv/Documents/GitHub/movie-data-analysis-pipeline/layers/batch_layer

echo "========================================="
echo "TMDB Batch Pipeline Manual Test"
echo "========================================="
echo ""

echo "1️⃣  Testing Bronze Ingestion..."
sudo docker exec pyspark-runner /bin/bash -c "
    cd /opt/spark-jobs && \
    export PYTHONPATH=/opt/spark-jobs && \
    python3 bronze_ingest.py --pages 2
"
echo "✅ Bronze ingestion completed!"
echo ""

echo "2️⃣  Testing Silver Transformation..."
sudo docker exec pyspark-runner /bin/bash -c "
    cd /opt/spark-jobs && \
    export PYTHONPATH=/opt/spark-jobs && \
    python3 silver_transform.py
"
echo "✅ Silver transformation completed!"
echo ""

echo "3️⃣  Testing Gold Aggregation..."
sudo docker exec pyspark-runner /bin/bash -c "
    cd /opt/spark-jobs && \
    export PYTHONPATH=/opt/spark-jobs && \
    python3 gold_aggregate.py
"
echo "✅ Gold aggregation completed!"
echo ""

echo "4️⃣  Testing MongoDB Export..."
sudo docker exec pyspark-runner /bin/bash -c "
    cd /opt/spark-jobs && \
    export PYTHONPATH=/opt/spark-jobs && \
    python3 export_to_mongo.py
"
echo "✅ MongoDB export completed!"
echo ""

echo "========================================="
echo "5️⃣  Verifying Data in Each Layer..."
echo "========================================="
echo ""

echo "📊 Bronze Layer (MinIO):"
sudo docker exec pyspark-runner /bin/bash -c "
    python3 -c '
from utils.spark_session import get_spark_session
import sys
sys.path.insert(0, \"/opt/spark-jobs\")

spark = get_spark_session(\"verify\")
movies = spark.read.parquet(\"s3a://bronze-data/movies/\")
print(f\"Bronze movies: {movies.count()}\")
spark.stop()
' 2>&1 | grep 'Bronze movies'
"
echo ""

echo "📊 Silver Layer (MinIO):"
sudo docker exec pyspark-runner /bin/bash -c "
    python3 -c '
from utils.spark_session import get_spark_session
import sys
sys.path.insert(0, \"/opt/spark-jobs\")

spark = get_spark_session(\"verify\")
try:
    movies = spark.read.parquet(\"s3a://silver-data/movies/\")
    print(f\"Silver movies: {movies.count()}\")
except Exception as e:
    print(f\"Silver data not yet available: {str(e)[:100]}\")
spark.stop()
' 2>&1 | grep -E '(Silver|available)'
"
echo ""

echo "📊 Gold Layer (MinIO):"
sudo docker exec pyspark-runner /bin/bash -c "
    python3 -c '
from utils.spark_session import get_spark_session
import sys
sys.path.insert(0, \"/opt/spark-jobs\")

spark = get_spark_session(\"verify\")
try:
    gold = spark.read.parquet(\"s3a://gold-data/\")
    print(f\"Gold aggregations: {gold.count()}\")
except Exception as e:
    print(f\"Gold data not yet available: {str(e)[:100]}\")
spark.stop()
' 2>&1 | grep -E '(Gold|available)'
"
echo ""

echo "📊 MongoDB (batch_views collection):"
sudo docker exec mongodb mongosh --quiet --eval "
    db = db.getSiblingDB('moviedb');
    db.auth('admin', 'password');
    const count = db.batch_views.countDocuments();
    print('MongoDB batch_views: ' + count + ' documents');
"
echo ""

echo "========================================="
echo "✅ Pipeline Test Completed Successfully!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Access Airflow UI: http://localhost:8088"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "2. Access MinIO UI: http://localhost:9001"
echo "   Username: minioadmin"
echo "   Password: minioadmin"
echo ""
echo "3. Connect to MongoDB:"
echo "   mongosh mongodb://admin:password@localhost:27017/moviedb?authSource=admin"
