# TMDB Movie Data Analysis Pipeline - Batch Layer

## Overview

The Batch Layer is the cornerstone of our Lambda Architecture implementation for TMDB (The Movie Database) historical data processing. This production-ready system processes massive volumes of movie data through a robust Bronze → Silver → Gold pipeline, delivering comprehensive analytics and insights for movie trends, sentiment analysis, and industry intelligence.

## Architecture

### Data Flow Architecture
```
TMDB API → Bronze Layer → Silver Layer → Gold Layer → MongoDB Views
    ↓           ↓             ↓            ↓
Raw Data → Cleaned Data → Enriched Data → Analytics → Serving Layer
```
│  • Genre/cast joins                   │
│  • Historical sentiment analysis      │
│  • Partition: /year/month/genre       │
└────────────────┬──────────────────────┘
                 ↓ (Spark Aggregations)
### Layer Responsibilities

#### Bronze Layer (Raw Data Lake)
- **Purpose**: Immutable storage of raw TMDB data
- **Format**: JSON documents stored as Parquet files in HDFS
- **Partitioning**: By ingestion date (`/year=2023/month=06/day=15/`)
- **Retention**: 2 years of historical data
- **Compression**: Snappy compression for optimal I/O performance

#### Silver Layer (Cleaned & Validated)
- **Purpose**: Cleaned, validated, and structured movie data
- **Schema**: Enforced schema with data quality validation
- **Enrichments**: Genre normalization, date parsing, sentiment analysis
- **Deduplication**: Movie-level deduplication based on TMDB ID
- **Quality Gates**: 95% completeness, 98% consistency thresholds

#### Gold Layer (Analytics-Ready)
- **Purpose**: Pre-aggregated analytics and business metrics
- **Views**: Genre trends, temporal analysis, trending scores, actor networks
- **Refresh**: Every 4 hours with incremental processing
- **Export**: MongoDB batch views for serving layer consumption

## Quick Start

### Prerequisites
```bash
# Required versions
- Apache Spark 3.4.x
- Apache Airflow 2.6.x
- Python 3.9+
- MongoDB 6.0+
- HDFS 3.3.x
```

### Installation
```bash
# Clone repository
git clone <repository-url>
cd movie-data-analysis-pipeline

# Install Python dependencies
pip install -r requirements.txt

# Set up configuration
cp config/batch_layer.yaml.template config/batch_layer.yaml
# Edit configuration with your environment details

# Initialize HDFS directories
hdfs dfs -mkdir -p /tmdb/bronze /tmdb/silver /tmdb/gold

# Set up MongoDB collections and indexes
python scripts/setup_mongodb.py
```

### Configuration

#### Batch Layer Configuration (`config/batch_layer.yaml`)
```yaml
spark:
  app_name: "TMDB-Batch-Processing"
  driver_memory: "4g"
  executor_memory: "8g"
  executor_cores: 4
  max_executors: 20
  
storage:
  hdfs_base_path: "hdfs://namenode:9000/tmdb"
  checkpoint_location: "hdfs://namenode:9000/tmdb/checkpoints"
  compression: "snappy"
  
quality:
  min_completeness: 0.95
  min_consistency: 0.98
  max_processing_delay: "2h"
  
mongodb:
  uri: "mongodb://localhost:27017"
  database: "tmdb_analytics"
  batch_size: 1000
```

### Running the Pipeline

#### Manual Execution
```bash
# Run complete pipeline
python -m layers.batch_layer.spark_jobs.bronze_to_silver_job \
  --input-path hdfs://namenode:9000/tmdb/bronze/2023/06/15 \
  --output-path hdfs://namenode:9000/tmdb/silver/2023/06/15 \
  --config-file config/batch_layer.yaml

# Run specific transformation
spark-submit \
  --class layers.batch_layer.spark_jobs.silver_to_gold_job \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8g \
  --num-executors 10 \
  layers/batch_layer/spark_jobs/silver_to_gold_job.py
```

#### Airflow Orchestration
```bash
# Start Airflow
airflow webserver --port 8080
airflow scheduler

# Trigger batch processing DAG
airflow dags trigger tmdb_batch_transform_dag

# Monitor pipeline
airflow dags list
airflow tasks list tmdb_batch_transform_dag
```

## Data Schemas

### Bronze Layer Schema
```json
{
  "movie_id": "string",
  "title": "string",
  "overview": "string",
  "release_date": "string",
  "genres": "array<string>",
  "vote_average": "double",
  "vote_count": "integer",
  "popularity": "double",
  "revenue": "long",
  "budget": "long",
  "runtime": "integer",
  "production_companies": "array<struct>",
  "production_countries": "array<struct>",
  "spoken_languages": "array<struct>",
  "cast": "array<struct>",
  "crew": "array<struct>",
  "keywords": "array<string>",
  "ingestion_timestamp": "timestamp"
}
```

### Silver Layer Schema
```json
{
  "movie_id": "string",
  "title": "string",
  "clean_title": "string",
  "overview": "string",
  "release_date": "date",
  "release_year": "integer",
  "release_month": "integer",
  "genres": "array<string>",
  "primary_genre": "string",
  "rating": "double",
  "vote_count": "integer",
  "popularity_score": "double",
  "revenue": "long",
  "budget": "long",
  "profit": "long",
  "roi": "double",
  "runtime": "integer",
  "runtime_category": "string",
  "sentiment_score": "double",
  "sentiment_category": "string",
  "production_companies": "array<string>",
  "production_countries": "array<string>",
  "spoken_languages": "array<string>",
  "director": "string",
  "top_cast": "array<string>",
  "keywords": "array<string>",
  "is_recent": "boolean",
  "decade": "string",
  "processing_timestamp": "timestamp"
}
```

### Gold Layer Schemas

#### Genre Analytics
```json
{
  "genre": "string",
  "year": "integer",
  "month": "integer",
  "total_movies": "long",
  "avg_rating": "double",
  "total_revenue": "long",
  "avg_budget": "long",
  "avg_sentiment": "double",
  "top_movies": "array<struct>",
  "top_directors": "array<struct>",
  "computed_date": "timestamp"
}
```

#### Trending Scores
```json
{
  "movie_id": "string",
  "window": "string",
  "trend_score": "double",
  "velocity": "double",
  "popularity_change": "double",
  "rating_momentum": "double",
  "social_buzz": "double",
  "computed_date": "timestamp"
}
```
│  • Updated every 4 hours              │
│  • Indexed for fast queries           │
└───────────────────────────────────────┘
```

---

## Key Characteristics

| Property | Value | Rationale |
|----------|-------|-----------|
| **Schedule** | Every 4 hours | Balances freshness vs cost |
| **Accuracy** | 100% | No approximations allowed |
| **Latency** | Hours | Acceptable for historical data |
| **Reprocessing** | Full history | Can recompute from scratch |
| **Storage** | HDFS | Distributed, fault-tolerant |
| **Retention** | Bronze: 90d, Silver: 2y, Gold: 5y | Cost vs compliance |

---

## Directory Structure

```
batch_layer/
├── README.md                    # This file
│
├── airflow_dags/               # Orchestration
│   ├── __init__.py
│   ├── batch_ingestion_dag.py  # Bronze layer ingestion
│   ├── batch_transform_dag.py  # Silver layer processing
│   ├── batch_aggregate_dag.py  # Gold layer aggregations
│   └── README.md               # DAG documentation
│
├── spark_jobs/                 # Batch processing
│   ├── __init__.py
│   ├── bronze_to_silver.py     # Cleaning & enrichment
│   ├── silver_to_gold.py       # Aggregations
│   ├── sentiment_batch.py      # Historical sentiment
│   ├── actor_networks.py       # Graph analytics
│   └── README.md               # Job documentation
│
├── master_dataset/             # Immutable raw data
│   ├── __init__.py
│   ├── ingestion.py            # TMDB → HDFS pipeline
│   ├── schema.py               # Data schemas
│   ├── partitioning.py         # Partition strategies
│   └── README.md               # Dataset documentation
│
├── batch_views/                # Pre-computed views
│   ├── __init__.py
│   ├── movie_analytics.py      # Movie-level aggregations
│   ├── genre_trends.py         # Genre-based analytics
│   ├── temporal_analysis.py    # Time-series views
│   ├── export_to_mongo.py      # MongoDB integration
│   └── README.md               # Views documentation
│
├── config/                     # Configuration
│   ├── spark_config.yaml       # Spark settings
│   ├── hdfs_config.yaml        # HDFS settings
│   └── airflow_config.yaml     # DAG settings
│
└── tests/                      # Unit tests
    ├── test_transformations.py
    ├── test_aggregations.py
    └── test_data_quality.py
```

---

## Data Layers

### Bronze Layer (Raw Data)

**Purpose**: Store immutable, raw data exactly as received from TMDB API

**Schema**:
```python
# Parquet format
bronze_schema = {
    "movie_id": "int",
    "raw_json": "string",  # Complete API response
    "api_endpoint": "string",  # movies/people/reviews
    "extraction_timestamp": "timestamp",
    "partition_year": "int",
    "partition_month": "int",
    "partition_day": "int",
    "partition_hour": "int"
}
```

**Partitioning**:
```
/bronze/movies/year=2025/month=10/day=17/hour=14/
/bronze/reviews/year=2025/month=10/day=17/hour=14/
/bronze/people/year=2025/month=10/day=17/hour=14/
```

**Retention**: 90 days (configurable)

**Implementation**: `master_dataset/ingestion.py`

---

### Silver Layer (Cleaned Data)

**Purpose**: Cleaned, validated, and enriched data ready for analytics

**Transformations**:
1. **Deduplication**: Remove duplicate movie_id entries
2. **Schema Validation**: Ensure data quality
3. **Enrichment**: 
   - Join genre names from genre_ids
   - Join cast/crew information
   - Extract keywords and production companies
4. **Sentiment Analysis**: Historical review sentiment
5. **Data Type Casting**: Proper types for all fields

**Schema**:
```python
silver_schema = {
    "movie_id": "int",
    "title": "string",
    "release_date": "date",
    "genres": "array<string>",
    "vote_average": "double",
    "vote_count": "int",
    "popularity": "double",
    "budget": "long",
    "revenue": "long",
    "runtime": "int",
    "cast": "array<struct<name:string, character:string>>",
    "crew": "array<struct<name:string, job:string>>",
    "sentiment_score": "double",  # -1 to 1
    "sentiment_label": "string",  # positive/neutral/negative
    "quality_flag": "string",  # OK/WARNING/ERROR
    "processed_timestamp": "timestamp",
    "partition_year": "int",
    "partition_month": "int",
    "partition_genre": "string"
}
```

**Partitioning**:
```
/silver/movies/year=2025/month=10/genre=action/
/silver/reviews/year=2025/month=10/genre=action/
```

**Retention**: 2 years

**Implementation**: `spark_jobs/bronze_to_silver.py`

---

### Gold Layer (Aggregated Data)

**Purpose**: Business-ready aggregations and analytics

**Aggregations**:
1. **Movie Popularity Trends**: 7-day, 30-day, 90-day rolling windows
2. **Genre Analytics**: Average ratings, revenue by genre
3. **Temporal Analysis**: Year-over-year comparisons
4. **Actor Networks**: Collaboration graphs
5. **Sentiment Trends**: Sentiment changes over time

**Views**:
```python
# Genre aggregations
genre_analytics = {
    "genre": "string",
    "year": "int",
    "month": "int",
    "total_movies": "int",
    "avg_rating": "double",
    "total_revenue": "long",
    "avg_sentiment": "double",
    "top_movies": "array<string>"
}

# Trending scores
trending_scores = {
    "movie_id": "int",
    "window": "string",  # 7d/30d/90d
    "trend_score": "double",
    "velocity": "double",  # rate of popularity change
    "computed_date": "date"
}
```

**Partitioning**:
```
/gold/genre_analytics/year=2025/month=10/
/gold/trending_scores/window=7d/year=2025/month=10/
/gold/actor_networks/year=2025/month=10/
```

**Retention**: 5 years

**Implementation**: `spark_jobs/silver_to_gold.py`

---

## Airflow DAGs

### 1. Batch Ingestion DAG (`batch_ingestion_dag.py`)

**Schedule**: Every 4 hours  
**Purpose**: Extract data from TMDB API and store in Bronze layer

```python
# DAG Structure
[start] 
  → [check_api_health]
  → [extract_movies] 
  → [extract_reviews]
  → [extract_people]
  → [validate_extraction]
  → [store_to_hdfs_bronze]
  → [update_metadata]
  → [end]
```

**Tasks**:
- `check_api_health`: Verify TMDB API availability
- `extract_*`: Parallel extraction from different endpoints
- `validate_extraction`: Check data completeness
- `store_to_hdfs_bronze`: Write Parquet to HDFS
- `update_metadata`: Update extraction logs

---

### 2. Batch Transform DAG (`batch_transform_dag.py`)

**Schedule**: 30 minutes after ingestion  
**Purpose**: Transform Bronze → Silver

```python
# DAG Structure
[start]
  → [wait_for_bronze]
  → [spark_deduplicate]
  → [spark_validate_schema]
  → [spark_enrich_data]
  → [spark_sentiment_analysis]
  → [write_to_silver]
  → [data_quality_checks]
  → [end]
```

**Spark Job**: `spark_jobs/bronze_to_silver.py`

---

### 3. Batch Aggregate DAG (`batch_aggregate_dag.py`)

**Schedule**: 1 hour after transformation  
**Purpose**: Create Gold layer views and export to MongoDB

```python
# DAG Structure
[start]
  → [wait_for_silver]
  → [spark_genre_aggregations]
  → [spark_trending_scores]
  → [spark_actor_networks]
  → [write_to_gold]
  → [export_to_mongodb]
  → [update_serving_indexes]
  → [end]
```

**Spark Jobs**: `spark_jobs/silver_to_gold.py`, `batch_views/export_to_mongo.py`

---

## Spark Jobs

### Bronze to Silver (`spark_jobs/bronze_to_silver.py`)

**Input**: HDFS Bronze layer Parquet files  
**Output**: HDFS Silver layer Parquet files

**Processing Steps**:
1. Read Bronze Parquet with pushdown filters
2. Parse raw JSON and extract fields
3. Deduplicate by movie_id (keep latest)
4. Validate schema and data types
5. Enrich with genre names, cast info
6. Run sentiment analysis on reviews
7. Add quality flags
8. Write to Silver with partitioning

**Key Optimizations**:
- Broadcast small lookup tables (genres)
- Partition by year/month/genre
- Use bucketing for frequent joins
- Z-ordering on movie_id

**Template**: See `spark_jobs/bronze_to_silver.py`

---

### Silver to Gold (`spark_jobs/silver_to_gold.py`)

**Input**: HDFS Silver layer Parquet files  
**Output**: HDFS Gold layer aggregated views

**Aggregations**:
1. **Genre Analytics**: GROUP BY genre, year, month
2. **Trending Scores**: Window functions for moving averages
3. **Actor Networks**: GraphX for collaboration graphs
4. **Temporal Analysis**: Year-over-year comparisons

**Window Functions**:
```python
# 7-day rolling average popularity
window_7d = Window.partitionBy("movie_id").orderBy("date").rowsBetween(-6, 0)
df = df.withColumn("popularity_7d_avg", avg("popularity").over(window_7d))
```

**Template**: See `spark_jobs/silver_to_gold.py`

---

## Batch Views (MongoDB Export)

### Collections in MongoDB

```javascript
// batch_views collection
{
  "_id": ObjectId("..."),
  "movie_id": 12345,
  "view_type": "genre_analytics",  // genre/trending/temporal
  "data": {
    "genre": "Action",
    "year": 2025,
    "month": 10,
    "avg_rating": 7.5,
    "total_movies": 150,
    "total_revenue": 5000000000
  },
  "computed_at": ISODate("2025-10-17T14:00:00Z"),
  "batch_run_id": "batch_2025_10_17_14"
}
```

**Indexes**:
```javascript
db.batch_views.createIndex({ "movie_id": 1, "view_type": 1 })
db.batch_views.createIndex({ "view_type": 1, "data.genre": 1, "data.year": 1 })
db.batch_views.createIndex({ "computed_at": -1 })
```

**Export Process**:
1. Read Gold layer aggregations
2. Transform to MongoDB document format
3. Bulk upsert to `batch_views` collection
4. Update indexes
5. Log export metrics

**Template**: See `batch_views/export_to_mongo.py`

---

## Configuration

### Spark Configuration (`config/spark_config.yaml`)

```yaml
spark:
  app_name: "batch_layer_processing"
  master: "spark://spark-master:7077"
  
  executor:
    memory: "4g"
    cores: 2
    instances: 10
  
  driver:
    memory: "2g"
    cores: 1
  
  sql:
    adaptive_enabled: true
    coalesce_partitions_enabled: true
    broadcast_timeout: 300
  
  serializer: "org.apache.spark.serializer.KryoSerializer"
```

### HDFS Configuration (`config/hdfs_config.yaml`)

```yaml
hdfs:
  namenode: "hdfs://namenode:9000"
  replication_factor: 3
  block_size: "128m"
  
  paths:
    bronze: "/data/bronze"
    silver: "/data/silver"
    gold: "/data/gold"
  
  retention:
    bronze_days: 90
    silver_days: 730
    gold_days: 1825
```

---

## Next Phase Implementation Tasks

### Phase 2A: Master Dataset Ingestion (Week 3)
- [ ] Implement `master_dataset/ingestion.py`
  - TMDB API client with rate limiting
  - Batch extraction for movies, reviews, people
  - Write raw JSON to HDFS Bronze layer
  - Implement retry logic and error handling

- [ ] Create `master_dataset/schema.py`
  - Define Bronze layer Parquet schemas
  - Validation rules
  - Partition strategies

- [ ] Build `airflow_dags/batch_ingestion_dag.py`
  - 4-hour schedule
  - Parallel extraction tasks
  - Health checks and monitoring
  - Slack/email alerts

### Phase 2B: Silver Layer Transformations (Week 4)
- [ ] Implement `spark_jobs/bronze_to_silver.py`
  - Read Bronze Parquet
  - Deduplication logic
  - Schema validation
  - Data enrichment (genres, cast)

- [ ] Create `spark_jobs/sentiment_batch.py`
  - Historical sentiment analysis
  - VADER or transformer model
  - Batch processing of reviews

- [ ] Build `airflow_dags/batch_transform_dag.py`
  - Trigger after ingestion
  - Spark job orchestration
  - Data quality checks

### Phase 2C: Gold Layer Aggregations (Week 5)
- [ ] Implement `spark_jobs/silver_to_gold.py`
  - Genre aggregations
  - Trending scores with window functions
  - Temporal analysis

- [ ] Create `spark_jobs/actor_networks.py`
  - GraphX-based network analysis
  - Collaboration strength calculations
  - Community detection

- [ ] Build `batch_views/export_to_mongo.py`
  - Gold → MongoDB pipeline
  - Bulk upsert logic
  - Index maintenance

- [ ] Create `airflow_dags/batch_aggregate_dag.py`
  - Trigger after transformations
  - Gold layer processing
  - MongoDB export

### Phase 2D: Testing & Monitoring (Week 5)
- [ ] Write unit tests
  - Test deduplication logic
  - Test aggregation accuracy
  - Test MongoDB export

- [ ] Set up data quality checks
  - Row count validation
  - Schema validation
  - Completeness checks

- [ ] Configure monitoring
  - DAG success/failure rates
  - Spark job duration
  - HDFS storage utilization

---

## Testing Strategy

### Unit Tests
```python
# tests/test_transformations.py
def test_deduplication():
    # Create sample DataFrame with duplicates
    # Run deduplication
    # Assert no duplicates remain
    pass

def test_sentiment_analysis():
    # Sample reviews with known sentiments
    # Run sentiment analysis
    # Assert accuracy > 80%
    pass
```

### Integration Tests
```python
# tests/test_end_to_end.py
def test_bronze_to_silver_pipeline():
    # Write sample Bronze data
    # Run Spark job
    # Validate Silver output
    pass
```

### Data Quality Tests
```python
# tests/test_data_quality.py
def test_completeness():
    # Check for null values in required fields
    pass

def test_schema_compliance():
    # Validate schema matches expected
    pass
```

---

## Monitoring & Alerts

### Key Metrics
- **Batch Job Success Rate**: Target > 99%
- **Processing Time**: < 2 hours per batch
- **Data Quality Score**: > 95% rows passing validation
- **HDFS Storage Growth**: Track and predict capacity

### Alerts
- DAG failure → Slack channel + email
- Data quality below threshold → PagerDuty
- HDFS capacity > 80% → Ops team notification
- Job duration > 3 hours → Warning alert

---

## Troubleshooting

### Common Issues

**Issue**: Spark job OOM errors  
**Solution**: Increase executor memory, reduce partition size

**Issue**: HDFS write failures  
**Solution**: Check namenode health, verify replication factor

**Issue**: Duplicate records in Silver  
**Solution**: Review deduplication logic, check for partition key issues

**Issue**: Slow MongoDB export  
**Solution**: Use bulk writes, create appropriate indexes

---

## References

- [Apache Spark Best Practices](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Airflow DAG Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Lambda Architecture - Batch Layer](http://nathanmarz.com/blog/how-to-beat-the-cap-theorem.html)

---

**Next Step**: Proceed to [Speed Layer README](../speed_layer/README.md)
