# Speed Layer - Real-Time Data Processing

## Overview

The **Speed Layer** provides low-latency views of recent data (last 48 hours), compensating for the batch layer's processing delay. It processes streaming data from Kafka using Spark Structured Streaming and stores results in Cassandra with automatic expiration.

```
TMDB API (real-time stream)
    ↓ (Kafka Producer - streaming)
┌───────────────────────────────────────┐
│          KAFKA TOPICS                 │
│  • movie.reviews (new reviews)        │
│  • movie.ratings (new ratings)        │
│  • movie.metadata (updates)           │
│  • Replication factor: 3              │
└────────────────┬──────────────────────┘
                 ↓ (Spark Structured Streaming)
┌───────────────────────────────────────┐
│      REAL-TIME PROCESSING             │
│  • 5-minute tumbling windows          │
│  • Real-time sentiment (VADER)        │
│  • Incremental aggregations           │
│  • Hot movie detection (velocity)     │
└────────────────┬──────────────────────┘
                 ↓ (Write to Cassandra)
┌───────────────────────────────────────┐
│      CASSANDRA (Speed Views)          │
│  • Table: speed_views                 │
│  • TTL: 48 hours (auto-expire)        │
│  • Partition: (movie_id, hour)        │
│  • Replication factor: 3              │
└────────────────┬──────────────────────┘
                 ↓ (Periodic sync)
┌───────────────────────────────────────┐
│      MONGODB (Speed Views)            │
│  • Collection: speed_views            │
│  • Synced every 5 minutes             │
│  • Merged with batch at query time    │
└───────────────────────────────────────┘
```

---

## Key Characteristics

| Property | Value | Rationale |
|----------|-------|-----------|
| **Latency** | <5 minutes | Real-time user experience |
| **Accuracy** | ~95% | Approximations acceptable |
| **Window Size** | 5 minutes | Balance latency vs cost |
| **Data Retention** | 48 hours | Covered by batch after 2 days |
| **Storage** | Cassandra | Low-latency writes, auto-expire |
| **Processing** | Spark Streaming | Unified with batch layer |

---

## Directory Structure

```
speed_layer/
├── README.md                       # This file
│
├── kafka_producers/               # Real-time ingestion
│   ├── __init__.py
│   ├── tmdb_stream_producer.py    # TMDB API streaming
│   ├── event_producer.py          # Event-driven ingestion
│   ├── schema_registry.py         # Avro schema management
│   └── README.md                  # Producer documentation
│
├── streaming_jobs/                # Spark Structured Streaming
│   ├── __init__.py
│   ├── review_sentiment_stream.py # Real-time sentiment
│   ├── movie_aggregation_stream.py # Incremental aggregations
│   ├── trending_detection_stream.py # Hot movie detection
│   ├── windowing_utils.py         # Window functions
│   └── README.md                  # Job documentation
│
├── cassandra_views/               # Speed layer storage
│   ├── __init__.py
│   ├── schema.cql                 # Cassandra table schemas
│   ├── speed_view_manager.py      # CRUD operations
│   ├── ttl_manager.py             # TTL configuration
│   └── README.md                  # Schema documentation
│
├── connectors/                    # Cassandra-MongoDB sync
│   ├── __init__.py
│   ├── cassandra_to_mongo.py      # Sync pipeline
│   ├── sync_scheduler.py          # Periodic sync
│   └── README.md                  # Connector documentation
│
├── config/                        # Speed Layer Configuration
│   ├── __init__.py                # Config module init
│   ├── config_loader.py           # YAML config loader with env vars
│   ├── kafka_config.yaml          # Kafka settings
│   ├── spark_streaming_config.yaml # Streaming settings
│   └── cassandra_config.yaml      # Cassandra settings
│
└── tests/                         # Unit tests
    ├── test_streaming_jobs.py
    ├── test_kafka_producers.py
    └── test_cassandra_operations.py
```

---

## Kafka Topics

### Topic: `movie.reviews`

**Purpose**: Stream new movie reviews from TMDB

**Schema** (Avro):
```json
{
  "type": "record",
  "name": "MovieReview",
  "namespace": "com.moviepipeline.reviews",
  "fields": [
    {"name": "review_id", "type": "string"},
    {"name": "movie_id", "type": "int"},
    {"name": "author", "type": "string"},
    {"name": "content", "type": "string"},
    {"name": "rating", "type": ["null", "double"]},
    {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "url", "type": "string"}
  ]
}
```

**Configuration**:
- Partitions: 6
- Replication Factor: 3
- Retention: 7 days
- Cleanup Policy: delete

---

### Topic: `movie.ratings`

**Purpose**: Stream rating updates from TMDB

**Schema** (Avro):
```json
{
  "type": "record",
  "name": "MovieRating",
  "namespace": "com.moviepipeline.ratings",
  "fields": [
    {"name": "movie_id", "type": "int"},
    {"name": "vote_average", "type": "double"},
    {"name": "vote_count", "type": "int"},
    {"name": "popularity", "type": "double"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
```

**Configuration**:
- Partitions: 6
- Replication Factor: 3
- Retention: 7 days
- Cleanup Policy: delete

---

### Topic: `movie.metadata`

**Purpose**: Stream metadata updates (new movies, updates)

**Schema** (Avro):
```json
{
  "type": "record",
  "name": "MovieMetadata",
  "namespace": "com.moviepipeline.metadata",
  "fields": [
    {"name": "movie_id", "type": "int"},
    {"name": "title", "type": "string"},
    {"name": "release_date", "type": ["null", "string"]},
    {"name": "genres", "type": {"type": "array", "items": "string"}},
    {"name": "runtime", "type": ["null", "int"]},
    {"name": "budget", "type": ["null", "long"]},
    {"name": "revenue", "type": ["null", "long"]},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "event_type", "type": "string"}
  ]
}
```

**Configuration**:
- Partitions: 6
- Replication Factor: 3
- Retention: 7 days
- Cleanup Policy: delete

---

## Kafka Producers

### TMDB Stream Producer (`kafka_producers/tmdb_stream_producer.py`)

**Purpose**: Continuously poll TMDB API for new data and stream to Kafka

**Features**:
- Rate limiting: 4 requests/second (TMDB limit)
- Exponential backoff on failures
- Avro serialization with schema registry
- Delivery confirmations
- Offset management

**Endpoints Polled**:
- `/movie/latest` - New movies (every 1 minute)
- `/movie/{id}/reviews` - New reviews (every 5 minutes)
- `/trending/movie/day` - Trending updates (every 5 minutes)

**Configuration**:
```yaml
producer:
  bootstrap_servers: "kafka:9092"
  schema_registry_url: "http://schema-registry:8081"
  
  rate_limit:
    requests_per_second: 4
    burst_size: 10
  
  polling:
    movies_interval_seconds: 60
    reviews_interval_seconds: 300
    trending_interval_seconds: 300
  
  kafka:
    compression_type: "snappy"
    acks: "all"
    retries: 3
    max_in_flight_requests: 5
```

**Template**: See `kafka_producers/tmdb_stream_producer.py`

---

## Spark Structured Streaming Jobs

### 1. Review Sentiment Stream (`streaming_jobs/review_sentiment_stream.py`)

**Purpose**: Real-time sentiment analysis on movie reviews

**Input**: Kafka topic `movie.reviews`  
**Output**: Cassandra table `speed_views.review_sentiments`

**Processing**:
1. Read from Kafka with 5-minute tumbling windows
2. Apply VADER sentiment analysis
3. Aggregate by movie_id within window
4. Calculate:
   - Average sentiment score
   - Positive/negative/neutral counts
   - Sentiment velocity (trend)
5. Write to Cassandra with TTL=48h

**Windowing**:
```python
# 5-minute tumbling window
windowedDF = df.groupBy(
    window(col("timestamp"), "5 minutes"),
    col("movie_id")
).agg(
    avg("sentiment_score").alias("avg_sentiment"),
    count("review_id").alias("review_count"),
    sum(when(col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_count"),
    sum(when(col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count")
)
```

**Checkpoint**: `/tmp/checkpoints/review_sentiment`

**Template**: See `streaming_jobs/review_sentiment_stream.py`

---

### 2. Movie Aggregation Stream (`streaming_jobs/movie_aggregation_stream.py`)

**Purpose**: Incremental aggregations for movie statistics

**Input**: Kafka topics `movie.ratings`, `movie.metadata`  
**Output**: Cassandra table `speed_views.movie_stats`

**Processing**:
1. Read from Kafka with 5-minute tumbling windows
2. Join ratings with metadata
3. Calculate:
   - Current vote_average and vote_count
   - Popularity score
   - Rating velocity (change rate)
4. Update Cassandra (upsert by movie_id)

**State Management**:
```python
# Maintain state for incremental updates
stateSchema = StructType([
    StructField("movie_id", IntegerType()),
    StructField("last_vote_count", IntegerType()),
    StructField("last_updated", TimestampType())
])

statefulDF = df.groupByKey(lambda row: row.movie_id).mapGroupsWithState(
    func=update_movie_state,
    stateSchema=stateSchema,
    outputMode="update"
)
```

**Checkpoint**: `/tmp/checkpoints/movie_aggregation`

**Template**: See `streaming_jobs/movie_aggregation_stream.py`

---

### 3. Trending Detection Stream (`streaming_jobs/trending_detection_stream.py`)

**Purpose**: Detect "hot" movies with rapid popularity increases

**Input**: Kafka topic `movie.ratings`  
**Output**: Cassandra table `speed_views.trending_movies`

**Processing**:
1. Read from Kafka with 5-minute tumbling windows
2. Calculate velocity:
   - Popularity change rate
   - Vote count increase rate
   - Acceleration (2nd derivative)
3. Apply trending threshold (e.g., velocity > 100)
4. Rank by trending score
5. Write top 100 trending movies to Cassandra

**Velocity Calculation**:
```python
# Calculate velocity using window functions
windowSpec = Window.partitionBy("movie_id").orderBy("window_end")

velocityDF = df.withColumn(
    "velocity",
    (col("popularity") - lag("popularity", 1).over(windowSpec)) / 
    (col("window_end").cast("long") - lag("window_end", 1).over(windowSpec).cast("long"))
)
```

**Checkpoint**: `/tmp/checkpoints/trending_detection`

**Template**: See `streaming_jobs/trending_detection_stream.py`

---

## Cassandra Schema

### Keyspace Definition

```sql
CREATE KEYSPACE IF NOT EXISTS speed_layer
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
};
```

---

### Table: `review_sentiments`

**Purpose**: Store real-time sentiment aggregations

```sql
CREATE TABLE speed_layer.review_sentiments (
  movie_id INT,
  hour TIMESTAMP,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  avg_sentiment DOUBLE,
  review_count INT,
  positive_count INT,
  negative_count INT,
  neutral_count INT,
  sentiment_velocity DOUBLE,
  PRIMARY KEY ((movie_id, hour), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 172800  -- 48 hours
  AND gc_grace_seconds = 86400;
```

**Indexes**:
```sql
CREATE INDEX ON speed_layer.review_sentiments (window_end);
```

---

### Table: `movie_stats`

**Purpose**: Store real-time movie statistics

```sql
CREATE TABLE speed_layer.movie_stats (
  movie_id INT,
  hour TIMESTAMP,
  vote_average DOUBLE,
  vote_count INT,
  popularity DOUBLE,
  rating_velocity DOUBLE,
  last_updated TIMESTAMP,
  PRIMARY KEY ((movie_id, hour))
) WITH default_time_to_live = 172800  -- 48 hours
  AND gc_grace_seconds = 86400;
```

---

### Table: `trending_movies`

**Purpose**: Store hot/trending movies

```sql
CREATE TABLE speed_layer.trending_movies (
  hour TIMESTAMP,
  rank INT,
  movie_id INT,
  title TEXT,
  trending_score DOUBLE,
  velocity DOUBLE,
  acceleration DOUBLE,
  PRIMARY KEY (hour, rank)
) WITH CLUSTERING ORDER BY (rank ASC)
  AND default_time_to_live = 172800  -- 48 hours
  AND gc_grace_seconds = 86400;
```

**Indexes**:
```sql
CREATE INDEX ON speed_layer.trending_movies (movie_id);
```

---

## Cassandra-MongoDB Sync

### Sync Process (`connectors/cassandra_to_mongo.py`)

**Purpose**: Periodically sync Cassandra speed views to MongoDB for serving layer

**Schedule**: Every 5 minutes

**Process**:
1. Query Cassandra for data updated in last 5 minutes
2. Transform to MongoDB document format
3. Bulk upsert to `speed_views` collection
4. Update sync metadata

**MongoDB Document Format**:
```javascript
{
  "_id": ObjectId("..."),
  "movie_id": 12345,
  "view_type": "sentiment",  // sentiment/stats/trending
  "hour": ISODate("2025-10-17T14:00:00Z"),
  "data": {
    "avg_sentiment": 0.75,
    "review_count": 45,
    "positive_count": 30,
    "negative_count": 10,
    "neutral_count": 5
  },
  "synced_at": ISODate("2025-10-17T14:05:00Z"),
  "ttl_expires_at": ISODate("2025-10-19T14:00:00Z")
}
```

**Indexes**:
```javascript
db.speed_views.createIndex({ "movie_id": 1, "view_type": 1, "hour": -1 })
db.speed_views.createIndex({ "ttl_expires_at": 1 }, { expireAfterSeconds: 0 })
```

**Template**: See `connectors/cassandra_to_mongo.py`

---

## Configuration

### Kafka Configuration (`layers/speed_layer/config/kafka_config.yaml`)

```yaml
kafka:
  bootstrap_servers: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
  schema_registry_url: "http://schema-registry:8081"
  
  topics:
    reviews:
      name: "movie.reviews"
      partitions: 6
      replication_factor: 3
      retention_ms: 604800000  # 7 days
    
    ratings:
      name: "movie.ratings"
      partitions: 6
      replication_factor: 3
      retention_ms: 604800000
    
    metadata:
      name: "movie.metadata"
      partitions: 6
      replication_factor: 3
      retention_ms: 604800000
  
  consumer:
    group_id: "speed_layer_consumers"
    auto_offset_reset: "earliest"
    enable_auto_commit: false
    max_poll_records: 500
```

---

### Spark Streaming Configuration (`layers/speed_layer/config/spark_streaming_config.yaml`)

```yaml
spark_streaming:
  app_name: "speed_layer_streaming"
  master: "spark://spark-master:7077"
  
  executor:
    memory: "2g"
    cores: 1
    instances: 5
  
  driver:
    memory: "1g"
    cores: 1
  
  streaming:
    checkpoint_location: "/tmp/checkpoints"
    trigger_interval: "5 minutes"
    watermark_delay: "10 seconds"
    
  kafka:
    starting_offsets: "earliest"
    max_offsets_per_trigger: 10000
    
  cassandra:
    connection_host: "cassandra-1,cassandra-2,cassandra-3"
    keyspace: "speed_layer"
```

---

### Cassandra Configuration (`layers/speed_layer/config/cassandra_config.yaml`)

```yaml
cassandra:
  contact_points:
    - cassandra-1
    - cassandra-2
    - cassandra-3
  
  port: 9042
  keyspace: "speed_layer"
  replication_factor: 3
  
  consistency:
    read: "LOCAL_QUORUM"
    write: "LOCAL_QUORUM"
  
  ttl:
    default_hours: 48
    gc_grace_seconds: 86400
  
  batch_size: 1000
  concurrent_requests: 100
```

---

## Next Phase Implementation Tasks

### Phase 3A: Kafka Setup (Week 6)
- [ ] Set up Kafka cluster (3 brokers)
  - Configure replication factor
  - Set up monitoring

- [ ] Deploy Schema Registry
  - Register Avro schemas
  - Configure compatibility rules

- [ ] Create Kafka topics
  - movie.reviews
  - movie.ratings
  - movie.metadata

### Phase 3B: Kafka Producers (Week 6)
- [ ] Implement `kafka_producers/tmdb_stream_producer.py`
  - TMDB API polling
  - Rate limiting (4 req/s)
  - Avro serialization
  - Delivery confirmations

- [ ] Create `kafka_producers/schema_registry.py`
  - Schema registration
  - Version management
  - Compatibility checks

### Phase 3C: Cassandra Setup (Week 7)
- [ ] Deploy Cassandra cluster (3 nodes)
  - Configure replication
  - Set up monitoring

- [ ] Create keyspace and tables
  - review_sentiments
  - movie_stats
  - trending_movies

- [ ] Configure TTL policies
  - 48-hour auto-expiration
  - GC grace period

### Phase 3D: Streaming Jobs (Week 7-8)
- [ ] Implement `streaming_jobs/review_sentiment_stream.py`
  - Real-time sentiment analysis
  - 5-minute windowing
  - Cassandra writes

- [ ] Create `streaming_jobs/movie_aggregation_stream.py`
  - Incremental aggregations
  - State management
  - Cassandra upserts

- [ ] Build `streaming_jobs/trending_detection_stream.py`
  - Velocity calculations
  - Trending score
  - Top-N ranking

### Phase 3E: Cassandra-MongoDB Sync (Week 8)
- [ ] Implement `connectors/cassandra_to_mongo.py`
  - Query Cassandra
  - Transform to MongoDB format
  - Bulk upserts

- [ ] Create `connectors/sync_scheduler.py`
  - 5-minute schedule
  - Error handling
  - Sync metrics

### Phase 3F: Testing & Monitoring (Week 8)
- [ ] Write unit tests
  - Test streaming logic
  - Test Kafka producers
  - Test Cassandra operations

- [ ] Set up monitoring
  - Kafka consumer lag
  - Streaming query latency
  - Cassandra write throughput

---

## Testing Strategy

### Unit Tests
```python
# tests/test_streaming_jobs.py
def test_sentiment_analysis():
    # Create test DataFrame
    # Apply sentiment analysis
    # Assert results
    pass

def test_windowing():
    # Create test stream
    # Apply windowing
    # Assert window boundaries
    pass
```

### Integration Tests
```python
# tests/test_kafka_to_cassandra.py
def test_end_to_end_stream():
    # Produce test messages to Kafka
    # Run streaming job
    # Verify data in Cassandra
    pass
```

### Performance Tests
```python
# tests/test_performance.py
def test_throughput():
    # Measure messages processed per second
    # Assert > 1000 msg/s
    pass
```

---

## Monitoring & Alerts

### Key Metrics
- **Kafka Consumer Lag**: Target < 1000 messages
- **Streaming Latency**: Target < 5 minutes end-to-end
- **Cassandra Write Throughput**: Target > 5000 writes/sec
- **Cassandra TTL Expiration**: Verify data expires after 48h

### Alerts
- Consumer lag > 5000 → Warning
- Consumer lag > 10000 → Critical
- Streaming job failure → PagerDuty
- Cassandra node down → Critical

---

## Troubleshooting

### Common Issues

**Issue**: Kafka consumer lag increasing  
**Solution**: Add more streaming executors, increase parallelism

**Issue**: Cassandra write timeouts  
**Solution**: Reduce batch size, increase concurrent requests

**Issue**: Streaming query slow  
**Solution**: Optimize window functions, reduce state size

**Issue**: Data not expiring in Cassandra  
**Solution**: Verify TTL configuration, check gc_grace_seconds

---

## References

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [Cassandra Time-To-Live](https://cassandra.apache.org/doc/latest/cql/dml.html#time-to-live)
- [Lambda Architecture - Speed Layer](http://nathanmarz.com/blog/how-to-beat-the-cap-theorem.html)

---

**Next Step**: Proceed to [Serving Layer README](../serving_layer/README.md)
