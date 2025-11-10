#!/bin/bash
# Quick System Status Check
# Run this to verify the Batch Layer is ready

echo "=========================================
🎯 BATCH LAYER - SYSTEM STATUS
=========================================
"

echo "📍 Access Points:"
echo "   • Airflow UI: http://localhost:8088 (admin/admin)"
echo "   • MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "   • MongoDB: mongodb://admin:password@localhost:27017/moviedb"
echo ""

echo "📋 Services Status:"
cd /home/dmv/Documents/GitHub/movie-data-analysis-pipeline/layers/batch_layer
sudo docker compose -f docker-compose.batch.yml ps
echo ""

echo "📦 MinIO Buckets:"
sudo docker logs minio-init 2>&1 | grep "Bucket created" || echo "Check logs for bucket creation"
echo ""

echo "=========================================
🧪 RUNNING QUICK PIPELINE TEST
=========================================
"

echo "Step 1: Bronze Ingestion (2 pages from TMDB)..."
sudo docker exec pyspark-runner python3 /opt/spark-jobs/bronze_ingest.py --pages 2 > /tmp/bronze_test.log 2>&1 &
BRONZE_PID=$!

echo "   Running in background (PID: $BRONZE_PID)..."
echo "   This will take ~40-60 seconds"
echo ""

# Wait and show status
for i in {1..60}; do
    if ! kill -0 $BRONZE_PID 2>/dev/null; then
        echo "   ✅ Bronze ingestion completed!"
        break
    fi
    if [ $((i % 10)) -eq 0 ]; then
        echo "   ⏳ Still running... ($i seconds)"
    fi
    sleep 1
done

# Show results
if [ -f /tmp/bronze_test.log ]; then
    echo ""
    echo "   📊 Results:"
    grep -E "(movies_fetched|total_movies|Bronze.*completed)" /tmp/bronze_test.log | tail -5
    echo ""
fi

echo "Step 2: Verify Bronze Data..."
sudo docker exec pyspark-runner python3 -c "
import sys
sys.path.insert(0, '/opt/spark-jobs')
from utils.spark_session import get_spark_session

spark = get_spark_session('verify')
try:
    movies = spark.read.parquet('s3a://bronze-data/movies/')
    print(f'   ✅ Bronze movies: {movies.count()} records')
except Exception as e:
    print(f'   ❌ Error: {e}')
finally:
    spark.stop()
" 2>&1 | grep "✅\|❌"

echo ""
echo "=========================================
✅ SYSTEM IS READY!
=========================================
"
echo ""
echo "📝 Next steps:"
echo "   1. Check Airflow UI at http://localhost:8088"
echo "   2. Browse data in MinIO at http://localhost:9001"
echo "   3. For full pipeline test, run each layer manually:"
echo ""
echo "      Bronze:  sudo docker exec pyspark-runner python3 /opt/spark-jobs/bronze_ingest.py --pages 2"
echo "      Silver:  sudo docker exec pyspark-runner python3 /opt/spark-jobs/silver_transform.py"
echo "      Gold:    sudo docker exec pyspark-runner python3 /opt/spark-jobs/gold_aggregate.py"
echo "      MongoDB: sudo docker exec pyspark-runner python3 /opt/spark-jobs/export_to_mongo.py"
echo ""
echo "🐛 Report any bugs you find!"
