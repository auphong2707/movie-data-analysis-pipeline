# ✅ Batch Layer - System Ready for Testing

## 🎯 Current Status: ALL SERVICES RUNNING

The Batch Layer has been completely reorganized and is ready for your testing!

---

## 📂 File Organization

```
layers/batch_layer/
├── docker-compose.batch.yml     ← Main docker-compose file (RENAMED)
├── spark_jobs/                  ← All Spark jobs
│   ├── bronze_ingest.py
│   ├── silver_transform.py
│   ├── gold_aggregate.py
│   ├── export_to_mongo.py
│   └── utils/
├── airflow_dags/                ← Airflow DAGs
│   ├── tmdb_batch_dag.py
│   └── tmdb_batch_dag_simple.py
├── BATCH_SETUP.md              ← Setup documentation
├── TESTING_GUIDE.md            ← Testing guide
├── test_full_pipeline.sh       ← Complete pipeline test
└── quick_test.sh               ← Quick status check
```

---

## 🚀 Quick Start Commands

### 1. Start All Services
```bash
cd /home/dmv/Documents/GitHub/movie-data-analysis-pipeline/layers/batch_layer
sudo docker compose -f docker-compose.batch.yml up -d
```

### 2. Check Services Status
```bash
sudo docker compose -f docker-compose.batch.yml ps
```

### 3. Stop All Services
```bash
sudo docker compose -f docker-compose.batch.yml down -v
```

---

## 🧪 Test the Complete Data Flow

Run each step **one at a time** and check for bugs:

### Step 1: Bronze Ingestion (TMDB API → Bronze Layer)
```bash
sudo docker exec pyspark-runner python3 /opt/spark-jobs/bronze_ingest.py --pages 2
```

**Expected**: Fetches ~40 movies from TMDB API, writes to s3a://bronze-data/

**Check for errors**: Look for any ERROR messages in output

### Step 2: Verify Bronze Data
```bash
sudo docker exec pyspark-runner python3 -c "
import sys
sys.path.insert(0, '/opt/spark-jobs')
from utils.spark_session import get_spark_session

spark = get_spark_session('verify')
movies = spark.read.parquet('s3a://bronze-data/movies/')
print(f'Bronze movies: {movies.count()}')
movies.show(5)
spark.stop()
"
```

**Expected**: Shows count and 5 sample movies

### Step 3: Silver Transformation (Bronze → Silver)
```bash
sudo docker exec pyspark-runner python3 /opt/spark-jobs/silver_transform.py
```

**Expected**: Cleans data, adds sentiment analysis, writes to s3a://silver-data/

### Step 4: Verify Silver Data
```bash
sudo docker exec pyspark-runner python3 -c "
import sys
sys.path.insert(0, '/opt/spark-jobs')
from utils.spark_session import get_spark_session

spark = get_spark_session('verify')
movies = spark.read.parquet('s3a://silver-data/movies/')
print(f'Silver movies: {movies.count()}')
movies.printSchema()
spark.stop()
"
```

### Step 5: Gold Aggregation (Silver → Gold)
```bash
sudo docker exec pyspark-runner python3 /opt/spark-jobs/gold_aggregate.py
```

**Expected**: Computes genre analytics, trending scores, writes to s3a://gold-data/

### Step 6: Verify Gold Data
```bash
sudo docker exec pyspark-runner python3 -c "
import sys
sys.path.insert(0, '/opt/spark-jobs')
from utils.spark_session import get_spark_session

spark = get_spark_session('verify')
gold = spark.read.parquet('s3a://gold-data/')
print(f'Gold aggregations: {gold.count()}')
gold.groupBy('view_type').count().show()
spark.stop()
"
```

### Step 7: MongoDB Export (Gold → MongoDB)
```bash
sudo docker exec pyspark-runner python3 /opt/spark-jobs/export_to_mongo.py
```

**Expected**: Exports aggregations to MongoDB moviedb.batch_views collection

### Step 8: Verify MongoDB Data
```bash
sudo docker exec mongodb mongosh --quiet --eval "
    db = db.getSiblingDB('moviedb');
    db.auth('admin', 'password');
    print('Total documents:', db.batch_views.countDocuments());
    print('Sample:');
    printjson(db.batch_views.findOne({}, {_id: 0}));
"
```

---

## 📍 Access URLs

### Airflow Web UI
- **URL**: http://localhost:8088
- **Username**: `admin`
- **Password**: `admin`
- **DAG**: `tmdb_batch_pipeline_simple`

### MinIO Console
- **URL**: http://localhost:9001
- **Username**: `minioadmin`
- **Password**: `minioadmin`
- **Buckets**: bronze-data, silver-data, gold-data

### MongoDB
- **Connection**: `mongodb://admin:password@localhost:27017/moviedb?authSource=admin`
- **Connect via CLI**:
  ```bash
  sudo docker exec -it mongodb mongosh mongodb://admin:password@mongodb:27017/moviedb?authSource=admin
  ```

---

## 🐛 Common Issues & Solutions

### Issue: "Cannot connect to Docker daemon"
```bash
# Solution: Add sudo
sudo docker compose -f docker-compose.batch.yml ps
```

### Issue: "No such file or directory: s3a://bronze-data"
```bash
# Solution: Run Bronze ingestion first
sudo docker exec pyspark-runner python3 /opt/spark-jobs/bronze_ingest.py --pages 2
```

### Issue: Containers not healthy
```bash
# Check logs
sudo docker logs pyspark-runner
sudo docker logs airflow-webserver
sudo docker logs mongodb

# Restart services
sudo docker compose -f docker-compose.batch.yml restart
```

### Issue: Port already in use
```bash
# Stop conflicting containers
sudo docker ps -a
sudo docker stop <container_name>

# Or change ports in docker-compose.batch.yml
```

---

## 📊 Expected Data Flow

```
TMDB API (4 req/s rate limit)
    ↓
Bronze Layer (s3a://bronze-data/)
  • Raw Parquet files
  • Partitioned by: year/month/day/hour
  • ~40 movies, 40 details, 30 credits, 40 reviews
    ↓
Silver Layer (s3a://silver-data/)
  • Deduplicated by movie_id
  • Genre enrichment (ID → name)
  • Sentiment analysis on reviews
  • Partitioned by: year/month/genre
    ↓
Gold Layer (s3a://gold-data/)
  • Genre analytics (avg rating, revenue, sentiment)
  • Trending scores (7d/30d/90d windows)
  • Temporal analysis (YoY trends)
  • Partitioned by: metric_type/year/month
    ↓
MongoDB (moviedb.batch_views)
  • Indexed by view_type
  • Ready for API queries
  • Documents with view_type: genre_analytics, trending_scores, etc.
```

---

## ✅ What's Fixed

1. ✅ **Docker Compose**: Renamed to `docker-compose.batch.yml` in batch_layer/
2. ✅ **File Paths**: All paths updated to work from batch_layer/ directory
3. ✅ **Spark Mode**: Using local[*] instead of cluster mode
4. ✅ **S3A JARs**: Automatically downloaded to pyspark-runner container
5. ✅ **Dependencies**: pyspark, requests, pymongo, boto3 installed
6. ✅ **urllib3 Fix**: Changed method_whitelist to allowed_methods
7. ✅ **Services**: All containers healthy (MinIO, MongoDB, Airflow, PySpark)

---

## 🎯 Your Testing Checklist

Please test each step and report:

- [ ] Services start successfully
- [ ] Bronze ingestion fetches data from TMDB
- [ ] Data appears in MinIO buckets
- [ ] Silver transformation works
- [ ] Gold aggregation computes correctly
- [ ] MongoDB export succeeds
- [ ] Data is queryable in MongoDB
- [ ] Airflow UI loads properly
- [ ] MinIO Console shows buckets

**For each bug you find, please share:**
1. Which command/step failed
2. Error message (full text)
3. Container logs if available

---

## 📝 Manual Commands Reference

```bash
# Start services
cd layers/batch_layer
sudo docker compose -f docker-compose.batch.yml up -d

# Check status
sudo docker compose -f docker-compose.batch.yml ps

# Run Bronze
sudo docker exec pyspark-runner python3 /opt/spark-jobs/bronze_ingest.py --pages 2

# Run Silver
sudo docker exec pyspark-runner python3 /opt/spark-jobs/silver_transform.py

# Run Gold
sudo docker exec pyspark-runner python3 /opt/spark-jobs/gold_aggregate.py

# Run MongoDB Export
sudo docker exec pyspark-runner python3 /opt/spark-jobs/export_to_mongo.py

# Check Bronze data
sudo docker exec pyspark-runner python3 -c "
from utils.spark_session import get_spark_session
import sys; sys.path.insert(0, '/opt/spark-jobs')
spark = get_spark_session('check')
print(spark.read.parquet('s3a://bronze-data/movies/').count())
spark.stop()
"

# Check MongoDB data
sudo docker exec mongodb mongosh --quiet --eval "
db = db.getSiblingDB('moviedb');
db.auth('admin', 'password');
print(db.batch_views.countDocuments());
"

# Stop everything
sudo docker compose -f docker-compose.batch.yml down -v
```

---

## 🚀 Ready to Test!

The system is **fully operational** and waiting for your testing. Please run through the steps above and report any bugs you encounter!
