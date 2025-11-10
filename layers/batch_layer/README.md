# TMDB Batch Layer - Movie Data Pipeline

> **📢 IMPORTANT**: This layer is now part of the unified setup at project root.  
> **See the root [README.md](../../README.md) for the recommended way to run the complete Lambda Architecture.**  
> The instructions below are for running the batch layer in isolation (development/testing only).

**One-command deployment**: Fetch, transform, and analyze movie data from TMDB API using Apache Spark, MinIO, and MongoDB.

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
TMDB API → Bronze (JSON) → Silver (Parquet) → Gold (Aggregated) → MongoDB
              ↓                ↓                    ↓
           MinIO            MinIO                MinIO
```

### Pipeline Flow
1. **Bronze**: Fetch 80 movies from TMDB API → Store raw JSON in MinIO
2. **Silver**: Clean, deduplicate, validate → Store Parquet in MinIO  
3. **Gold**: Aggregate by genre (avg rating, revenue, count) → Store Parquet
4. **Export**: Load analytical results into MongoDB for serving

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
- Find DAG: `tmdb_batch_pipeline`
- Click "Play" button → "Trigger DAG"
- Wait 5-8 minutes for all tasks to turn green

### 2. Check Data in MinIO
- Open http://localhost:9001
- Login: `minioadmin` / `minioadmin`
- Browse buckets:
  - `bronze/movies/` → Raw JSON files
  - `silver/movies/` → Cleaned Parquet files  
  - `gold/movies_by_genre/` → Aggregated Parquet files

### 3. Query MongoDB Results
```bash
# Count genre documents (expect ~19-20)
docker exec -it serving-mongodb mongosh --eval "use tmdb_analytics; db.movies_by_genre.countDocuments()"

# View Drama genre statistics
docker exec -it serving-mongodb mongosh --eval "
  use tmdb_analytics;
  db.movies_by_genre.find(
    {genre: 'Drama'}, 
    {genre: 1, avg_vote_average: 1, total_movies: 1}
  ).pretty()
"
```

**Expected Output:**
```json
{
  "genre": "Drama",
  "total_movies": 25,
  "avg_vote_average": 7.2,
  "avg_popularity": 45.3,
  "total_revenue": 1500000000
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
├── .env                            # TMDB API key (committed)
├── start.sh                        # One-click startup
├── dags/
│   └── tmdb_batch_pipeline.py     # Airflow DAG (12 tasks)
├── master_dataset/
│   └── ingestion.py               # Bronze: Fetch from TMDB API
├── spark_jobs/
│   ├── silver_transformation.py   # Silver: Clean & validate
│   ├── gold_aggregation.py        # Gold: Aggregate by genre
│   ├── export_to_mongo.py         # Export: Load to MongoDB
│   └── utils/                     # Shared Spark utilities
└── config/
    └── expectations/               # Data quality rules
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
Edit `dags/tmdb_batch_pipeline.py`:
```python
default_args = {
    'schedule_interval': '@daily',  # Options: '@hourly', '0 0 * * *', None
}
```

### Increase Movie Count
Edit `master_dataset/ingestion.py`:
```python
MAX_PAGES = 4  # Change to 10 for 200 movies, 20 for 400 movies
```

---

## 💡 Key Features

- **Dockerized**: No Python dependencies on host machine
- **Portable**: Includes API key in `.env` (committed to repo)
- **Resilient**: Retry logic for network timeouts (pip, curl)
- **Observable**: Airflow UI shows real-time progress
- **Validated**: Data quality checks at each stage
- **Production-Ready**: Uses industry-standard tools (Spark, Airflow, MinIO)

---

## 📊 Sample MongoDB Output

After successful run:

```json
{
  "_id": ObjectId("..."),
  "genre": "Action",
  "total_movies": 18,
  "avg_vote_average": 6.8,
  "avg_popularity": 52.1,
  "total_revenue": 2500000000,
  "avg_revenue_per_movie": 138888888,
  "last_updated": "2024-11-10T16:45:00Z"
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
