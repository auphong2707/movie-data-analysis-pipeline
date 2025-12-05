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
- **Airflow Web UI**: http://localhost:8080 (admin/admin)
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

**Step 1: Connect to MongoDB**
```bash
docker exec -it serving-mongodb mongosh --username admin --authenticationDatabase admin moviedb
```

**Step 2: Run queries inside the authenticated shell**
```js
// Count total documents (expect ~3000-4000)
db.batch_views.countDocuments()

// Count by view type
db.batch_views.aggregate([
  { $group: { _id: "$view_type", count: { $sum: 1 } } }
])

// View Action genre sentiment baseline
db.batch_views.findOne(
  { view_type: 'sentiment_baseline', genre: 'Action' }, 
  { genre: 1, avg_sentiment: 1, sentiment_stddev: 1, movie_count: 1, review_count: 1, _id: 0 }
)

// View viral threshold for Action blockbusters in summer
db.batch_views.findOne(
  { view_type: 'viral_threshold', genre: 'Action', budget_tier: 'blockbuster', season: 'summer' },
  { genre: 1, budget_tier: 1, season: 1, viral_threshold: 1, avg_popularity: 1, _id: 0 }
)

// View individual movie intelligence
db.batch_views.findOne(
  { view_type: 'movie_intelligence', title: { $exists: true } },
  { movie_id: 1, title: 1, genre: 1, vote_average: 1, avg_sentiment: 1, _id: 0 }
)

// Exit mongosh when done
exit
```

**Expected Output:**
```
3979

[
  { _id: 'movie_intelligence', count: 3295 },
  { _id: 'sentiment_baseline', count: 658 },
  { _id: 'viral_threshold', count: 26 }
]

{
  genre: 'Action',
  avg_sentiment: 0.0021077661263748473,
  sentiment_stddev: 0.016247547088559453,
  movie_count: 393,
  review_count: 33
}

{
  genre: 'Action',
  budget_tier: 'blockbuster',
  season: 'summer',
  viral_threshold: 29058,
  avg_popularity: 15.234
}

{
  movie_id: 914,
  title: 'The Great Dictator',
  genre: 'Comedy',
  vote_average: 8.3,
  avg_sentiment: 0
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

After successful run, the `batch_views` collection contains **3 view types** in a unified schema:

### 1. Sentiment Baseline (Genre/Franchise/Year Aggregations)

```json
{
  "_id": ObjectId("..."),
  "view_type": "sentiment_baseline",
  "genre": "Action",
  "franchise": null,
  "year": null,
  "avg_sentiment": 0.0021077661263748473,
  "sentiment_stddev": 0.016247547088559453,
  "movie_count": 393,
  "review_count": 33,
  "batch_run_timestamp": "2025-12-05T17:27:13.987915Z",
  "aggregation_granularity": "all_time",
  "data_period_start": "1900-01-01",
  "data_period_end": "2025-12-05",
  "updated_at": "2025-12-05T17:27:00.196Z"
}
```

**Other Genre Examples:**
```json
{ "genre": "Science Fiction", "avg_sentiment": 0.084, "movie_count": 375 }
{ "genre": "Comedy", "avg_sentiment": 0.114, "movie_count": 989 }
{ "genre": "Horror", "avg_sentiment": 0.004, "movie_count": 363 }
```

### 2. Viral Threshold (Genre×Budget×Season Thresholds)

```json
{
  "_id": ObjectId("..."),
  "view_type": "viral_threshold",
  "genre": "Action",
  "budget_tier": "blockbuster",
  "season": "summer",
  "viral_threshold": 29058,
  "avg_popularity": 6.973233333333333,
  "movie_count": 3,
  "batch_run_timestamp": "2025-12-05T17:27:13.987915Z",
  "aggregation_granularity": "all_time",
  "updated_at": "2025-12-05T17:27:03.808Z"
}
```

### 3. Movie Intelligence (Individual Movie Data)

```json
{
  "_id": ObjectId("..."),
  "view_type": "movie_intelligence",
  "movie_id": 914,
  "title": "The Great Dictator",
  "director": "Charlie Chaplin",
  "genre": "Comedy",
  "budget": 2000000,
  "budget_tier": "indie",
  "runtime": 125,
  "release_date": "1940-10-15",
  "release_year": 1940,
  "vote_average": 8.3,
  "vote_count": 3566,
  "popularity": 2.5774,
  "avg_sentiment": 0,
  "review_count": 0,
  "batch_run_timestamp": "2025-12-05T17:27:13.987915Z",
  "updated_at": "2025-12-05T17:27:05.500Z"
}
```

**Collection Stats**:
- **Total documents**: ~3,979
- **Movie Intelligence**: ~3,295 (individual movies)
- **Sentiment Baselines**: ~658 (genre/franchise/year aggregations)
- **Viral Thresholds**: ~26 (genre×budget×season combinations)
- **Genres covered**: 19 (Action, Adventure, Animation, Comedy, Crime, Documentary, Drama, Family, Fantasy, History, Horror, Music, Mystery, Romance, Science Fiction, TV Movie, Thriller, War, Western)

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
