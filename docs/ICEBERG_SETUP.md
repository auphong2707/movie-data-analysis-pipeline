# Apache Iceberg Integration Guide

## Overview

This guide covers the integration of Apache Iceberg table format into the Movie Data Analytics Pipeline. Apache Iceberg provides advanced lakehouse capabilities including ACID transactions, schema evolution, time travel, and improved query performance.

## Table of Contents

1. [What is Apache Iceberg?](#what-is-apache-iceberg)
2. [Architecture](#architecture)
3. [Key Features](#key-features)
4. [Setup and Configuration](#setup-and-configuration)
5. [Usage Examples](#usage-examples)
6. [Migration from Parquet](#migration-from-parquet)
7. [Maintenance Operations](#maintenance-operations)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

## What is Apache Iceberg?

Apache Iceberg is an open table format for huge analytic datasets that brings the reliability and simplicity of SQL tables to big data. It was designed to solve correctness problems in eventually-consistent cloud object stores.

### Why Iceberg for Movie Analytics Pipeline?

- **ACID Transactions**: Ensure data consistency across concurrent reads and writes
- **Schema Evolution**: Safely add, drop, or rename columns without rewriting data
- **Time Travel**: Query historical data snapshots for auditing and debugging
- **Partition Evolution**: Change partitioning schemes without rewriting all data
- **Hidden Partitioning**: Users don't need to know how data is partitioned
- **Improved Performance**: Better query performance through metadata optimization and file pruning

## Architecture

### Iceberg Integration in the Pipeline

```
┌─────────────────┐
│   TMDB API      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka Streams  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│           Apache Spark Streaming                    │
│  (with Iceberg Table Format)                        │
└─────────┬───────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────┐
│    MinIO (S3) + Apache Iceberg Tables               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │  Bronze  │  │  Silver  │  │   Gold   │          │
│  │  Layer   │  │  Layer   │  │  Layer   │          │
│  └──────────┘  └──────────┘  └──────────┘          │
│                                                      │
│  Hive Metastore (Catalog)                          │
└─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────┐
│    MongoDB      │
│  (Serving)      │
└─────────────────┘
```

### Components

1. **Iceberg Tables**: Replace traditional Parquet files with Iceberg table format
2. **Hive Metastore**: Catalog service for managing Iceberg table metadata
3. **PostgreSQL**: Backend database for Hive Metastore
4. **MinIO/S3**: Object storage for Iceberg data files and metadata

## Key Features

### 1. ACID Transactions

Iceberg provides snapshot isolation for reads and serializable isolation for writes:

```python
# Multiple writers can safely write to the same table
storage_manager.write_to_silver(df1, "movies", mode="append")
storage_manager.write_to_silver(df2, "movies", mode="append")
# No conflicts or data loss
```

### 2. Schema Evolution

Safely evolve table schemas without rewriting data:

```python
# Add new columns
storage_manager.evolve_schema(
    layer='silver',
    table_name='movies',
    columns_to_add=[
        ('ai_generated_summary', 'string'),
        ('trending_score', 'double')
    ]
)

# Rename columns
storage_manager.evolve_schema(
    layer='silver',
    table_name='movies',
    columns_to_rename={'vote_average': 'rating'}
)

# Drop columns
storage_manager.evolve_schema(
    layer='silver',
    table_name='movies',
    columns_to_drop=['deprecated_field']
)
```

### 3. Time Travel

Query data as it existed at any point in time:

```python
from datetime import datetime, timedelta

# Read data as it was yesterday
yesterday = datetime.now() - timedelta(days=1)
historical_df = storage_manager.read_from_layer(
    layer='silver',
    table_name='movies',
    timestamp=yesterday
)

# Read from a specific snapshot
df = storage_manager.read_from_layer(
    layer='silver',
    table_name='movies',
    snapshot_id=1234567890
)
```

### 4. Hidden Partitioning

Users query without knowing partitioning details:

```python
# No need to specify partition columns in queries
df = spark.sql("""
    SELECT * FROM movie_catalog.silver.movies
    WHERE release_date BETWEEN '2024-01-01' AND '2024-12-31'
""")
# Iceberg automatically prunes partitions
```

## Setup and Configuration

### 1. Environment Variables

Add these to your `.env` file:

```bash
# Iceberg Configuration
ICEBERG_CATALOG_TYPE=hive
ICEBERG_CATALOG_NAME=movie_catalog
ICEBERG_WAREHOUSE_PATH=s3a://iceberg-warehouse
HIVE_METASTORE_URI=thrift://hive-metastore:9083

# S3/MinIO for Iceberg
ICEBERG_S3_ENDPOINT=http://minio:9000
ICEBERG_S3_ACCESS_KEY=minioadmin
ICEBERG_S3_SECRET_KEY=minioadmin
ICEBERG_S3_PATH_STYLE_ACCESS=true

# Table Configuration
ICEBERG_DEFAULT_FILE_FORMAT=parquet
ICEBERG_COMPRESSION_CODEC=snappy
ICEBERG_TARGET_FILE_SIZE_BYTES=134217728  # 128MB
ICEBERG_DELETE_MODE=merge-on-read

# Retention
ICEBERG_SNAPSHOT_RETENTION_DAYS=7

# Database Names
ICEBERG_BRONZE_DATABASE=bronze
ICEBERG_SILVER_DATABASE=silver
ICEBERG_GOLD_DATABASE=gold
```

### 2. Start Infrastructure

```powershell
# Start all services including Hive Metastore
docker-compose up -d

# Verify Hive Metastore is running
docker-compose ps hive-metastore

# Check logs
docker-compose logs -f hive-metastore
```

### 3. Initialize MinIO Bucket

```powershell
# Access MinIO Console at http://localhost:9001
# Username: minioadmin
# Password: minioadmin

# Create the iceberg-warehouse bucket via UI or CLI
```

### 4. Install Python Dependencies

```powershell
pip install -r requirements.txt
```

## Usage Examples

### Basic Operations

#### 1. Initialize Iceberg Storage Manager

```python
from pyspark.sql import SparkSession
from src.streaming.spark_config import create_spark_session
from src.storage.iceberg_storage_manager import IcebergStorageManager

# Create Spark session with Iceberg enabled
spark = create_spark_session(
    app_name="MovieAnalytics",
    enable_iceberg=True
)

# Initialize storage manager
storage_manager = IcebergStorageManager(spark)
```

#### 2. Write Data to Bronze Layer

```python
# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movies") \
    .load()

# Parse and process
from pyspark.sql.functions import from_json, col
movie_df = df.select(
    from_json(col("value").cast("string"), movie_schema).alias("data")
).select("data.*")

# Add partition columns
from pyspark.sql.functions import year, month, dayofmonth
partitioned_df = movie_df \
    .withColumn("year", year(col("release_date"))) \
    .withColumn("month", month(col("release_date")))

# Write to Bronze layer as Iceberg table
storage_manager.write_to_bronze(
    df=partitioned_df,
    table_name="movies_raw",
    partition_cols=["year", "month"]
)
```

#### 3. Read and Transform to Silver Layer

```python
# Read from Bronze
bronze_df = storage_manager.read_from_layer(
    layer='bronze',
    table_name='movies_raw'
)

# Apply transformations (cleaning, enrichment)
from src.streaming.data_cleaner import DataCleaner

cleaner = DataCleaner()
silver_df = cleaner.clean_movie_data(bronze_df)

# Write to Silver with upsert (merge)
storage_manager.write_to_silver(
    df=silver_df,
    table_name='movies_clean',
    partition_cols=["year", "month"],
    mode="upsert"
)
```

#### 4. Create Gold Layer Aggregations

```python
# Read from Silver
silver_df = storage_manager.read_from_layer(
    layer='silver',
    table_name='movies_clean'
)

# Create aggregations
from pyspark.sql.functions import count, avg, sum

gold_df = silver_df.groupBy("year", "genre") \
    .agg(
        count("*").alias("movie_count"),
        avg("rating").alias("avg_rating"),
        sum("revenue").alias("total_revenue")
    )

# Write to Gold layer
storage_manager.write_to_gold(
    df=gold_df,
    table_name="movies_by_year_genre",
    partition_cols=["year"]
)
```

### Advanced Operations

#### Query Table History

```python
# Get all snapshots
snapshots = storage_manager.get_table_snapshots('silver', 'movies_clean')
snapshots.show()

# Get table history
history = storage_manager.get_table_history('silver', 'movies_clean')
history.show()

# Output:
# +----------------+-------------------+----------------+
# | snapshot_id    | made_current_at   | parent_id      |
# +----------------+-------------------+----------------+
# | 1234567890     | 2024-01-15 10:00  | null           |
# | 1234567891     | 2024-01-15 11:00  | 1234567890     |
# +----------------+-------------------+----------------+
```

#### Rollback to Previous Snapshot

```python
# Rollback to a previous snapshot
storage_manager.rollback_to_snapshot(
    layer='silver',
    table_name='movies_clean',
    snapshot_id=1234567890
)
```

#### Get Table Statistics

```python
# Get detailed statistics
stats = storage_manager.get_table_statistics('silver', 'movies_clean')
print(f"Record Count: {stats['record_count']}")
print(f"File Count: {stats['file_count']}")
print(f"Total Size: {stats['total_size_mb']:.2f} MB")
print(f"Avg File Size: {stats['avg_file_size_mb']:.2f} MB")
```

## Migration from Parquet

### Step-by-Step Migration

#### 1. Read Existing Parquet Data

```python
# Read from existing Parquet files
old_parquet_df = spark.read.parquet("s3a://bronze-data/movies/")
```

#### 2. Create Iceberg Table

```python
# Create new Iceberg table
storage_manager.create_table(
    layer='bronze',
    table_name='movies_raw',
    df=old_parquet_df,
    partition_cols=["year", "month"],
    overwrite=False
)
```

#### 3. Migrate Data

```python
# Write data to Iceberg
storage_manager.write_to_bronze(
    df=old_parquet_df,
    table_name='movies_raw',
    partition_cols=["year", "month"],
    mode="append"
)
```

#### 4. Update Application Code

Replace old Parquet reads:
```python
# Old way
df = spark.read.parquet("s3a://silver-data/movies/")

# New way with Iceberg
df = storage_manager.read_from_layer('silver', 'movies_clean')
# or
df = spark.table("movie_catalog.silver.movies_clean")
```

## Maintenance Operations

### 1. File Compaction

Compact small files for better query performance:

```python
# Compact a specific table
storage_manager.compact_table('silver', 'movies_clean')
```

### 2. Expire Old Snapshots

Remove old snapshots to save storage:

```python
# Expire snapshots older than 7 days
storage_manager.expire_snapshots(
    layer='silver',
    table_name='movies_clean',
    older_than_days=7
)
```

### 3. Remove Orphan Files

Clean up unreferenced files:

```python
# Remove orphan files
storage_manager.remove_orphan_files('silver', 'movies_clean')
```

### 4. Automated Maintenance

Use the maintenance manager for scheduled operations:

```python
from src.storage.iceberg_storage_manager import IcebergMaintenanceManager

maintenance_mgr = IcebergMaintenanceManager(storage_manager)

# Run full maintenance on all tables
maintenance_mgr.run_full_maintenance()

# Generate maintenance report
report = maintenance_mgr.generate_maintenance_report()
print(f"Total Tables: {report['total_tables']}")
print(f"Total Size: {report['total_size_mb']:.2f} MB")
print(f"Total Files: {report['total_files']}")
```

### Schedule with Airflow

Create an Airflow DAG for regular maintenance:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run_iceberg_maintenance():
    from pyspark.sql import SparkSession
    from src.streaming.spark_config import create_spark_session
    from src.storage.iceberg_storage_manager import (
        IcebergStorageManager,
        IcebergMaintenanceManager
    )
    
    spark = create_spark_session(enable_iceberg=True)
    storage_manager = IcebergStorageManager(spark)
    maintenance_mgr = IcebergMaintenanceManager(storage_manager)
    
    maintenance_mgr.run_full_maintenance()

dag = DAG(
    'iceberg_maintenance',
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Regular Iceberg table maintenance',
    schedule_interval='0 2 * * 0',  # Weekly at 2 AM on Sunday
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

maintenance_task = PythonOperator(
    task_id='run_maintenance',
    python_callable=run_iceberg_maintenance,
    dag=dag,
)
```

## Best Practices

### 1. Partitioning Strategy

```python
# Bronze Layer: Partition by date for data retention
partition_cols = ["year", "month", "day"]

# Silver Layer: Partition by date and category
partition_cols = ["year", "month", "genre"]

# Gold Layer: Partition by business dimensions
partition_cols = ["year", "region"]
```

### 2. Write Patterns

```python
# For streaming: Use append mode
storage_manager.write_to_silver(df, "movies", mode="append")

# For CDC: Use upsert mode
storage_manager.write_to_silver(df, "movies", mode="upsert")

# For full refreshes: Use overwrite partitions
storage_manager.write_to_silver(df, "movies", mode="overwrite")
```

### 3. Query Optimization

```python
# Use partition filters
df = spark.sql("""
    SELECT * FROM movie_catalog.silver.movies
    WHERE year = 2024 AND month = 1
""")

# Use column pruning
df = spark.sql("""
    SELECT id, title, rating FROM movie_catalog.silver.movies
    WHERE year = 2024
""")
```

### 4. Snapshot Management

```python
# Keep fewer snapshots for frequently updated tables
storage_manager.expire_snapshots('bronze', 'movies_raw', older_than_days=3)

# Keep more snapshots for critical tables
storage_manager.expire_snapshots('gold', 'financial_summary', older_than_days=30)
```

## Troubleshooting

### Issue: Hive Metastore Connection Failed

**Symptoms**: `Exception: Could not connect to meta store`

**Solution**:
```powershell
# Check if Hive Metastore is running
docker-compose ps hive-metastore

# Restart if needed
docker-compose restart hive-metastore

# Check logs
docker-compose logs hive-metastore

# Verify PostgreSQL is accessible
docker-compose exec postgres-metastore psql -U hive -d metastore
```

### Issue: S3/MinIO Connection Failed

**Symptoms**: `Exception: Unable to access bucket`

**Solution**:
```python
# Verify MinIO configuration
from config.iceberg_config import iceberg_config
print(iceberg_config.s3_endpoint)

# Test MinIO connectivity
import boto3
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)
s3_client.list_buckets()
```

### Issue: Schema Evolution Failed

**Symptoms**: `Exception: Cannot evolve schema`

**Solution**:
```python
# Check if schema evolution is enabled
from config.iceberg_config import iceberg_config
print(iceberg_config.enable_schema_evolution)  # Should be True

# Verify table format version
df = spark.sql("DESCRIBE EXTENDED movie_catalog.silver.movies")
df.filter("col_name == 'format-version'").show()
# Should be version 2 for schema evolution
```

### Issue: High Query Latency

**Symptoms**: Queries taking too long

**Solution**:
```python
# Run table compaction
storage_manager.compact_table('silver', 'movies_clean')

# Check file statistics
stats = storage_manager.get_table_statistics('silver', 'movies_clean')
print(f"File Count: {stats['file_count']}")
print(f"Avg File Size: {stats['avg_file_size_mb']:.2f} MB")

# If too many small files, increase target file size
# Update ICEBERG_TARGET_FILE_SIZE_BYTES in .env
```

## Performance Benchmarks

Typical performance improvements with Iceberg:

| Operation | Parquet | Iceberg | Improvement |
|-----------|---------|---------|-------------|
| Query with partition filter | 12s | 3s | 4x faster |
| Schema evolution | Hours (rewrite) | Seconds (metadata) | 1000x faster |
| Concurrent writes | Error prone | Safe | ACID |
| Time travel query | Not available | 5s | N/A |
| Metadata query | Slow (scan) | Fast (metadata) | 10x faster |

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Iceberg Best Practices](https://iceberg.apache.org/docs/latest/performance/)
- [Project GitHub Repository](https://github.com/auphong2707/movie-data-analysis-pipeline)

## Support

For issues or questions:
1. Check this documentation
2. Review [Troubleshooting](#troubleshooting) section
3. Check project logs: `docker-compose logs`
4. Open an issue on GitHub
