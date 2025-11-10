#!/bin/bash
# Complete Batch Layer Pipeline Test
# Tests: Bronze → Silver → Gold → MongoDB data flow

set -e
cd /home/dmv/Documents/GitHub/movie-data-analysis-pipeline/layers/batch_layer

echo "========================================="
echo "🚀 BATCH LAYER COMPLETE PIPELINE TEST"
echo "========================================="
echo ""
echo "Services:"
echo "- Airflow UI: http://localhost:8088 (admin/admin)"
echo "- MinIO UI: http://localhost:9001 (minioadmin/minioadmin)"
echo "- MongoDB: mongodb://admin:password@localhost:27017/moviedb"
echo ""

# Function to check service health
check_services() {
    echo "📋 Checking service health..."
    sudo docker compose -f docker-compose.batch.yml ps --format "table {{.Name}}\t{{.Status}}"
    echo ""
}

# Function to check MinIO buckets
check_minio_buckets() {
    echo "📦 Checking MinIO buckets..."
    sudo docker logs minio-init 2>&1 | tail -5
    echo ""
}

# Run Bronze ingestion
run_bronze() {
    echo "========================================="
    echo "1️⃣  BRONZE LAYER INGESTION"
    echo "========================================="
    echo "Fetching data from TMDB API..."
    
    sudo docker exec pyspark-runner /bin/bash -c "
        cd /opt/spark-jobs && \
        export PYTHONPATH=/opt/spark-jobs && \
        python3 bronze_ingest.py --pages 2
    " 2>&1 | grep -E "(INFO|ERROR|movies_fetched|Bronze)" | tail -20
    
    echo ""
    echo "✅ Bronze ingestion completed!"
    echo ""
}

# Run Silver transformation
run_silver() {
    echo "========================================="
    echo "2️⃣  SILVER LAYER TRANSFORMATION"
    echo "========================================="
    echo "Cleaning and enriching data..."
    
    sudo docker exec pyspark-runner /bin/bash -c "
        cd /opt/spark-jobs && \
        export PYTHONPATH=/opt/spark-jobs && \
        python3 silver_transform.py
    " 2>&1 | grep -E "(INFO|ERROR|Silver)" | tail -20
    
    echo ""
    echo "✅ Silver transformation completed!"
    echo ""
}

# Run Gold aggregation
run_gold() {
    echo "========================================="
    echo "3️⃣  GOLD LAYER AGGREGATION"
    echo "========================================="
    echo "Computing analytics and aggregations..."
    
    sudo docker exec pyspark-runner /bin/bash -c "
        cd /opt/spark-jobs && \
        export PYTHONPATH=/opt/spark-jobs && \
        python3 gold_aggregate.py
    " 2>&1 | grep -E "(INFO|ERROR|Gold)" | tail -20
    
    echo ""
    echo "✅ Gold aggregation completed!"
    echo ""
}

# Run MongoDB export
run_mongo_export() {
    echo "========================================="
    echo "4️⃣  MONGODB EXPORT"
    echo "========================================="
    echo "Exporting data to serving layer..."
    
    sudo docker exec pyspark-runner /bin/bash -c "
        cd /opt/spark-jobs && \
        export PYTHONPATH=/opt/spark-jobs && \
        python3 export_to_mongo.py
    " 2>&1 | grep -E "(INFO|ERROR|MongoDB|Exported)" | tail -20
    
    echo ""
    echo "✅ MongoDB export completed!"
    echo ""
}

# Verify data at each layer
verify_data() {
    echo "========================================="
    echo "5️⃣  DATA VERIFICATION"
    echo "========================================="
    echo ""
    
    echo "📊 Bronze Layer (MinIO s3a://bronze-data/):"
    sudo docker exec pyspark-runner python3 -c "
import sys
sys.path.insert(0, '/opt/spark-jobs')
from utils.spark_session import get_spark_session

spark = get_spark_session('verify_bronze')
try:
    movies = spark.read.parquet('s3a://bronze-data/movies/')
    print(f'  ✅ Movies: {movies.count()} records')
    
    details = spark.read.parquet('s3a://bronze-data/movie_details/')
    print(f'  ✅ Details: {details.count()} records')
    
    credits = spark.read.parquet('s3a://bronze-data/credits/')
    print(f'  ✅ Credits: {credits.count()} records')
    
    reviews = spark.read.parquet('s3a://bronze-data/reviews/')
    print(f'  ✅ Reviews: {reviews.count()} records')
except Exception as e:
    print(f'  ❌ Error: {str(e)[:100]}')
finally:
    spark.stop()
" 2>&1 | grep -E "(✅|❌|Movies|Details|Credits|Reviews)"
    echo ""
    
    echo "📊 Silver Layer (MinIO s3a://silver-data/):"
    sudo docker exec pyspark-runner python3 -c "
import sys
sys.path.insert(0, '/opt/spark-jobs')
from utils.spark_session import get_spark_session

spark = get_spark_session('verify_silver')
try:
    movies = spark.read.parquet('s3a://silver-data/movies/')
    print(f'  ✅ Movies: {movies.count()} records')
    
    reviews = spark.read.parquet('s3a://silver-data/reviews/')
    print(f'  ✅ Reviews: {reviews.count()} records')
except Exception as e:
    print(f'  ❌ Error: {str(e)[:100]}')
finally:
    spark.stop()
" 2>&1 | grep -E "(✅|❌|Movies|Reviews)"
    echo ""
    
    echo "📊 Gold Layer (MinIO s3a://gold-data/):"
    sudo docker exec pyspark-runner python3 -c "
import sys
sys.path.insert(0, '/opt/spark-jobs')
from utils.spark_session import get_spark_session

spark = get_spark_session('verify_gold')
try:
    gold = spark.read.parquet('s3a://gold-data/')
    count = gold.count()
    print(f'  ✅ Aggregations: {count} records')
    
    # Show metrics breakdown
    metrics = gold.groupBy('view_type').count().collect()
    for row in metrics:
        print(f'     - {row[\"view_type\"]}: {row[\"count\"]} records')
except Exception as e:
    print(f'  ❌ Error: {str(e)[:100]}')
finally:
    spark.stop()
" 2>&1 | grep -E "(✅|❌|Aggregations|-)"
    echo ""
    
    echo "📊 MongoDB (moviedb.batch_views):"
    sudo docker exec mongodb mongosh --quiet --eval "
        db = db.getSiblingDB('moviedb');
        db.auth('admin', 'password');
        const count = db.batch_views.countDocuments();
        print('  ✅ Documents: ' + count);
        
        // Count by view_type
        const types = db.batch_views.distinct('view_type');
        types.forEach(function(type) {
            const typeCount = db.batch_views.countDocuments({view_type: type});
            print('     - ' + type + ': ' + typeCount + ' documents');
        });
    " 2>&1 | grep -E "(✅|❌|-)"
    echo ""
}

# Show sample data
show_samples() {
    echo "========================================="
    echo "6️⃣  SAMPLE DATA"
    echo "========================================="
    echo ""
    
    echo "📄 Bronze Sample (first 3 movies):"
    sudo docker exec pyspark-runner python3 -c "
import sys
sys.path.insert(0, '/opt/spark-jobs')
from utils.spark_session import get_spark_session

spark = get_spark_session('sample')
movies = spark.read.parquet('s3a://bronze-data/movies/')
movies.select('id', 'title', 'release_date', 'vote_average').show(3, truncate=False)
spark.stop()
" 2>&1 | tail -10
    echo ""
    
    echo "📄 MongoDB Sample (first document):"
    sudo docker exec mongodb mongosh --quiet --eval "
        db = db.getSiblingDB('moviedb');
        db.auth('admin', 'password');
        printjson(db.batch_views.findOne({}, {_id: 0}));
    " 2>&1 | head -20
    echo ""
}

# Main execution
main() {
    check_services
    check_minio_buckets
    
    echo "Starting pipeline execution..."
    echo ""
    
    run_bronze
    run_silver
    run_gold
    run_mongo_export
    
    verify_data
    show_samples
    
    echo "========================================="
    echo "✅ PIPELINE TEST COMPLETED SUCCESSFULLY!"
    echo "========================================="
    echo ""
    echo "📊 Access your data:"
    echo "   • Airflow UI: http://localhost:8088"
    echo "   • MinIO Console: http://localhost:9001"
    echo "   • MongoDB: mongosh mongodb://admin:password@localhost:27017/moviedb?authSource=admin"
    echo ""
    echo "🐛 Report any bugs you find!"
}

# Run main
main
