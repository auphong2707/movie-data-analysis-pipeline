# TMDB Batch Layer - Baseline Calculation Pipeline

> **📢 IMPORTANT**: This layer is now part of the unified setup at project root.  
> **See the root [README.md](../../README.md) for the recommended way to run the complete Lambda Architecture.**  
> The instructions below are for running the batch layer in isolation (development/testing only).

**One-command deployment**: Calculate historical baselines from TMDB metadata for comparison with real-time Reddit data using Apache Spark, MinIO, and MongoDB.

---

## 🚀 Quick Start

```bash
./start.sh
```

**That's it!** The system will:
- ✅ Build custom Airflow image with PySpark
- ✅ Start MinIO, MongoDB, Airflow, PostgreSQL
- ✅ Initialize database and wait for health checks

**Prerequisites:** Docker and Docker Compose installed

**Time:** ~5-10 minutes first run, ~2-3 minutes subsequent runs

---

## 📦 What This Does

```
TMDB API → Bronze (Metadata) → Silver (Baselines) → Gold (Export) → MongoDB
              ↓                    ↓                     ↓
           MinIO                MinIO                 MinIO
```

### Pipeline Flow
1. **Bronze**: Fetch ~2000 movies metadata from TMDB API → Store raw JSON in MinIO
2. **Silver**: Calculate genre-level baselines (sentiment, vote thresholds) → Store Parquet in MinIO  
3. **Gold**: Add metadata and prepare for export → Store Parquet
4. **Export**: Load baseline data into MongoDB for serving layer comparison with Reddit data

### Services Running
- **Airflow Web UI**: http://localhost:8088 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **MongoDB**: mongodb://localhost:27017
- **PostgreSQL**: Internal metadata store
- **PySpark Runner**: Executes Spark jobs

---

## ✅ Verify It Works

### 1. Trigger the Pipeline
- Open http://localhost:8088
- Login: `admin` / `admin`
- Find DAG: `tmdb_baseline_pipeline`
- Click "Play" button → "Trigger DAG"
- Wait 5-8 minutes for all tasks to turn green

### 2. Check Data in MinIO
- Open http://localhost:9001
- Login: `minioadmin` / `minioadmin`
- Browse buckets:
  - `bronze/tmdb_movies/` → Raw movie metadata JSON files
  - `bronze/tmdb_genres/` → Genre list JSON
  - `silver/genre_baselines/` → Calculated baseline Parquet files  
  - `gold/baselines/` → Final baseline Parquet files

### 3. Query MongoDB Results
```bash
# Count baseline documents (expect ~19-20 genres)
docker exec -it serving-mongodb mongosh --eval "use tmdb_analytics; db.batch_views.countDocuments()"

# View Action genre baseline
docker exec -it serving-mongodb mongosh --eval "
  use tmdb_analytics;
  db.batch_views.find(
    {genre: 'Action'}, 
    {genre: 1, avg_sentiment: 1, viral_threshold: 1, type: 1}
  ).pretty()
"
```

**Expected Output:**
```json
{
  "genre": "Action",
  "avg_sentiment": 0.65,
  "sentiment_stddev": 0.12,
  "viral_threshold": 5000,
  "type": "baseline",
  "updated_at": "2025-12-03T02:00:00Z",
  "source": "tmdb_batch"
}
```

---

## 🛠️ Troubleshooting

### Services Won't Start
```bash
# Check status
docker ps

# View logs
docker compose -f docker-compose.batch.yml logs airflow-scheduler
docker compose -f docker-compose.batch.yml logs airflow-webserver

# Restart if needed
docker compose -f docker-compose.batch.yml restart
```

### Airflow UI Not Accessible
- **Wait 2-3 minutes** for initialization
- Check health: `docker ps | grep airflow-webserver`
- Restart: `docker compose -f docker-compose.batch.yml restart airflow-webserver`

### DAG Not Showing
```bash
# Verify DAG file exists
docker exec airflow-scheduler ls -la /opt/airflow/dags/

# Restart scheduler
docker compose -f docker-compose.batch.yml restart airflow-scheduler
```

### Pipeline Task Fails
```bash
# Check scheduler logs
docker compose -f docker-compose.batch.yml logs airflow-scheduler | tail -100

# Check PySpark logs  
docker compose -f docker-compose.batch.yml logs pyspark-runner | tail -100

# Test MinIO connectivity
docker exec airflow-scheduler curl -I http://minio:9000/minio/health/live
```

### Slow Build (Network Timeout)
Already includes retry logic (pip: 1000s timeout, curl: 5 retries). If still failing:

Edit `Dockerfile.airflow` line 18:
```dockerfile
RUN pip install --default-timeout=2000 --retries=10 \
    pyspark==3.5.4 pymongo==4.10.1 requests==2.32.4 boto3==1.37.38
```

Rebuild:
```bash
docker compose -f docker-compose.batch.yml build --no-cache
```

---

## 🗂️ Project Structure

```
layers/batch_layer/
├── docker-compose.batch.yml       # Orchestrates all services
├── Dockerfile.airflow              # Custom Airflow + PySpark image
├── .env.example                    # Environment variables template
├── airflow_dags/
│   └── tmdb_baseline_pipeline.py  # Airflow DAG (baseline calculation)
├── spark_jobs/
│   ├── bronze_ingest.py           # Bronze: Fetch TMDB metadata
│   ├── silver_transform.py        # Silver: Calculate baselines
│   ├── gold_aggregate.py          # Gold: Prepare baseline export
│   ├── export_to_mongo.py         # Export: Load to MongoDB
│   └── utils/                     # Shared Spark utilities
└── tests/
    └── test_integration.py        # Integration tests
```

---

## 🔄 Stop & Restart

```bash
# Stop services (keep data)
docker compose -f docker-compose.batch.yml down

# Stop and delete all data (fresh start)
docker compose -f docker-compose.batch.yml down -v

# Restart after code changes
docker compose -f docker-compose.batch.yml down
docker compose -f docker-compose.batch.yml build --no-cache
docker compose -f docker-compose.batch.yml up -d
```

---

## 📝 Configuration

### Change TMDB API Key
Edit `.env`:
```env
TMDB_API_KEY=your_api_key_here
```

Rebuild:
```bash
docker compose -f docker-compose.batch.yml down
docker compose -f docker-compose.batch.yml build
docker compose -f docker-compose.batch.yml up -d
```

### Change Pipeline Schedule
Edit `airflow_dags/tmdb_baseline_pipeline.py`:
```python
dag = DAG(
    'tmdb_baseline_pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM (default)
    # Options: '@daily', '@weekly', '0 0 * * *', None
)
```

### Increase Movie Count
Edit `spark_jobs/bronze_ingest.py`:
```python
# In TMDBBaselineIngestion class
def fetch_movies(self):
    for page in range(1, 100):  # Change to 200 for ~4000 movies
```

---

## 💡 Key Features

- **Dockerized**: No Python dependencies on host machine
- **Portable**: Includes API key in `.env.example` template
- **Resilient**: Retry logic for network timeouts (pip, curl)
- **Observable**: Airflow UI shows real-time progress
- **Validated**: Data quality checks at each stage
- **Production-Ready**: Uses industry-standard tools (Spark, Airflow, MinIO)
- **Baseline-Focused**: Calculates historical baselines for Reddit comparison

---

## 📊 Sample MongoDB Output

After successful run:

```json
{
  "_id": ObjectId("..."),
  "genre": "Action",
  "avg_sentiment": 0.65,
  "sentiment_stddev": 0.12,
  "viral_threshold": 5000,
  "type": "baseline",
  "updated_at": "2025-12-03T02:00:00Z",
  "source": "tmdb_batch"
}
```

---

## 🤝 Sharing With Others

This repo is ready to share:
1. Push to GitHub
2. Friend clones repo
3. Friend runs `./start.sh`
4. Done!

**No configuration needed** - API key and all settings are included.

---

## 📄 License

See LICENSE file in repository root.
