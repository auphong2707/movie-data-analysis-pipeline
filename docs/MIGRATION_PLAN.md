# Migration Plan: TMDB-Only → Reddit + TMDB Dual-Source Architecture

**Document Version**: 1.0  
**Date**: December 2, 2025  
**Status**: Ready for Implementation

---

## Executive Summary

### Current State
- **Data Source**: TMDB API only (movie metadata + reviews)
- **Architecture**: Lambda Architecture (batch + speed + serving)
- **Focus**: Historical movie analytics (reviews, ratings, sentiment)
- **Limitation**: Static metadata, no real-time user discussions

### Target State
- **Data Sources**: Reddit API (primary, real-time) + TMDB API (secondary, baselines)
- **Architecture**: Same Lambda Architecture (retain infrastructure)
- **Focus**: Live discussion sentiment vs historical baselines
- **Advantage**: Real-time crisis detection, viral content identification

### Migration Scope
- **KEEP**: 70% of infrastructure (Kafka, Cassandra, Spark, MongoDB, Airflow)
- **MODIFY**: 25% (data schemas, producers, processing logic)
- **REMOVE**: 5% (TMDB-specific streaming components)

---

## Change Impact Analysis

### Phase 1: Speed Layer Transformation (HIGH IMPACT)

#### ✅ KEEP - Infrastructure
- **Kafka Cluster**: 3 brokers, ZooKeeper, Schema Registry
- **Cassandra**: Storage engine with TTL (48 hours)
- **Spark Streaming**: Processing framework
- **Docker Compose**: Container orchestration

#### 🔧 MODIFY - Kafka Topics & Schemas

**Current Topics (TMDB-focused)**:
```
movie.new_reviews      → TMDB review polling
movie.rating_events    → TMDB vote_count changes  
movie.popularity_changes → TMDB popularity tracking
movie.metadata         → TMDB movie metadata
```

**New Topics (Reddit-focused)**:
```
reddit.posts           → Reddit post stream (r/movies, r/boxoffice)
reddit.comments        → Reddit comment stream
reddit.upvotes         → Reddit score changes
reddit.awards          → Reddit awards tracking
tmdb.baselines         → TMDB historical context (batch sync)
```

**Schema Changes Required**:

**File**: `layers/speed_layer/kafka_producers/schema_registry.py`

**Current Schema** (REMOVE):
```python
REVIEW_SCHEMA = """
{
    "type": "record",
    "name": "MovieReview",
    "fields": [
        {"name": "review_id", "type": "string"},
        {"name": "movie_id", "type": "int"},
        {"name": "author", "type": "string"},
        {"name": "content", "type": "string"},
        {"name": "rating", "type": ["null", "double"]},
        {"name": "created_at", "type": "long"},
        {"name": "url", "type": "string"}
    ]
}
"""
```

**New Schema** (ADD):
```python
REDDIT_POST_SCHEMA = """
{
    "type": "record",
    "name": "RedditPost",
    "namespace": "com.moviepipeline.reddit",
    "fields": [
        {"name": "post_id", "type": "string"},
        {"name": "movie_title", "type": ["null", "string"]},
        {"name": "title", "type": "string"},
        {"name": "selftext", "type": "string"},
        {"name": "score", "type": "int"},
        {"name": "ups", "type": "int"},
        {"name": "downs", "type": "int"},
        {"name": "num_comments", "type": "int"},
        {"name": "all_awardings", "type": {"type": "array", "items": "string"}},
        {"name": "created_utc", "type": "long"},
        {"name": "author", "type": "string"},
        {"name": "subreddit", "type": "string"},
        {"name": "url", "type": "string"}
    ]
}
"""

REDDIT_COMMENT_SCHEMA = """
{
    "type": "record",
    "name": "RedditComment",
    "namespace": "com.moviepipeline.reddit",
    "fields": [
        {"name": "comment_id", "type": "string"},
        {"name": "post_id", "type": "string"},
        {"name": "movie_title", "type": ["null", "string"]},
        {"name": "body", "type": "string"},
        {"name": "score", "type": "int"},
        {"name": "ups", "type": "int"},
        {"name": "downs", "type": "int"},
        {"name": "created_utc", "type": "long"},
        {"name": "author", "type": "string"},
        {"name": "depth", "type": "int"},
        {"name": "parent_id", "type": "string"}
    ]
}
"""

REDDIT_AWARD_SCHEMA = """
{
    "type": "record",
    "name": "RedditAward",
    "namespace": "com.moviepipeline.reddit",
    "fields": [
        {"name": "post_id", "type": "string"},
        {"name": "movie_title", "type": ["null", "string"]},
        {"name": "award_type", "type": "string"},
        {"name": "award_count", "type": "int"},
        {"name": "coin_price", "type": "int"},
        {"name": "timestamp", "type": "long"}
    ]
}
"""
```

#### 🔧 MODIFY - Kafka Producer

**File**: `layers/speed_layer/kafka_producers/tmdb_stream_producer.py`

**Action**: Rename to `reddit_stream_producer.py` and rewrite

**Current Logic** (REMOVE):
- TMDB API polling (`/movie/{id}/reviews`, `/movie/{id}`)
- 30-second poll interval for vote_count changes
- Rate limit: 4 requests/second

**New Logic** (ADD):
```python
class RedditStreamProducer:
    """
    Real-time Reddit API stream producer
    
    Polls Reddit API for new posts/comments about movies
    Produces events to Kafka topics
    """
    
    def __init__(self, client_id, client_secret, user_agent, kafka_config):
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        self.producer = Producer(kafka_config)
        self.subreddits = ['movies', 'boxoffice', 'TrueFilm', 'moviecritic']
        self.movie_keywords = self._load_movie_keywords()  # From TMDB metadata
    
    def stream_new_posts(self):
        """Stream new posts from movie subreddits"""
        for subreddit_name in self.subreddits:
            subreddit = self.reddit.subreddit(subreddit_name)
            for post in subreddit.stream.submissions(skip_existing=True):
                # Extract movie title from post
                movie_title = self._extract_movie_title(post.title, post.selftext)
                
                event = {
                    'post_id': post.id,
                    'movie_title': movie_title,
                    'title': post.title,
                    'selftext': post.selftext,
                    'score': post.score,
                    'ups': post.ups,
                    'downs': post.downs,
                    'num_comments': post.num_comments,
                    'all_awardings': [a['name'] for a in post.all_awardings],
                    'created_utc': int(post.created_utc),
                    'author': str(post.author),
                    'subreddit': subreddit_name,
                    'url': post.url
                }
                
                self.producer.produce(
                    topic='reddit.posts',
                    key=post.id,
                    value=self._serialize(event, REDDIT_POST_SCHEMA)
                )
    
    def stream_comments(self, post_id):
        """Stream comments for a specific post"""
        submission = self.reddit.submission(id=post_id)
        submission.comments.replace_more(limit=0)
        
        for comment in submission.comments.list():
            event = {
                'comment_id': comment.id,
                'post_id': post_id,
                'movie_title': submission.movie_title,  # Inherit from post
                'body': comment.body,
                'score': comment.score,
                'ups': comment.ups,
                'downs': comment.downs,
                'created_utc': int(comment.created_utc),
                'author': str(comment.author),
                'depth': comment.depth,
                'parent_id': comment.parent_id
            }
            
            self.producer.produce(
                topic='reddit.comments',
                key=comment.id,
                value=self._serialize(event, REDDIT_COMMENT_SCHEMA)
            )
    
    def _extract_movie_title(self, title, body):
        """Extract movie title using keyword matching + NER"""
        # Match against TMDB movie title database
        for movie in self.movie_keywords:
            if movie.lower() in title.lower() or movie.lower() in body.lower():
                return movie
        return None
```

**Dependencies to ADD**:
- `praw` (Python Reddit API Wrapper): Reddit API client
- `spacy` (optional): Named Entity Recognition for movie title extraction
- TMDB movie title database (from batch layer)

#### 🔧 MODIFY - Spark Streaming Jobs

**File**: `layers/speed_layer/streaming_jobs/review_sentiment_stream.py`

**Current Logic** (TMDB reviews):
```python
# Read from movie.new_reviews topic
# Apply VADER sentiment to review.content
# Aggregate by 5-minute windows
```

**New Logic** (Reddit posts + comments):
```python
class RedditSentimentStream:
    """Real-time sentiment analysis on Reddit discussions"""
    
    def run(self):
        # Read from reddit.posts + reddit.comments topics
        posts_df = spark.readStream \
            .format("kafka") \
            .option("subscribe", "reddit.posts,reddit.comments") \
            .load()
        
        # Parse Avro
        parsed_df = posts_df.select(
            from_avro(col("value"), REDDIT_POST_SCHEMA).alias("data")
        ).select("data.*")
        
        # Combine title + selftext + body for sentiment
        with_text = parsed_df.withColumn(
            "full_text",
            concat_ws(" ", col("title"), col("selftext"), col("body"))
        )
        
        # Apply VADER sentiment (same as before)
        with_sentiment = with_text.withColumn(
            "sentiment_score",
            vader_sentiment_udf(col("full_text"))
        )
        
        # Aggregate by movie + 5-minute windows
        windowed = with_sentiment \
            .withWatermark("created_utc", "10 minutes") \
            .groupBy(
                col("movie_title"),
                window(col("created_utc"), "5 minutes")
            ) \
            .agg(
                avg("sentiment_score").alias("avg_sentiment"),
                count("*").alias("post_count"),
                sum(when(col("sentiment_score") > 0.05, 1).otherwise(0)).alias("positive_count"),
                sum(when(col("sentiment_score") < -0.05, 1).otherwise(0)).alias("negative_count"),
                sum("num_comments").alias("total_comments"),
                sum("score").alias("total_upvotes")
            )
        
        # Write to Cassandra speed_views
        query = windowed.writeStream \
            .foreachBatch(self._write_to_cassandra) \
            .outputMode("update") \
            .start()
```

**File**: `layers/speed_layer/streaming_jobs/movie_aggregation_stream.py`

**REMOVE**: TMDB vote_count aggregation logic
**ADD**: Reddit engagement velocity calculation

```python
# Calculate upvote velocity (upvotes per hour)
velocity_df = posts_df \
    .withWatermark("created_utc", "10 minutes") \
    .groupBy(
        col("movie_title"),
        window(col("created_utc"), "1 hour")
    ) \
    .agg(
        sum("score").alias("total_upvotes"),
        sum("num_comments").alias("total_comments"),
        count("*").alias("post_count"),
        avg("score").alias("avg_score_per_post")
    ) \
    .withColumn(
        "upvote_velocity",
        col("total_upvotes") / 1.0  # Upvotes per hour
    )
```

**File**: `layers/speed_layer/streaming_jobs/trending_detection_stream.py`

**Current**: Fetch TMDB titles via API
**New**: Use Reddit post titles + TMDB movie database for matching

```python
# REMOVE: TMDBTitleFetcher class (no more API calls in streaming)
# ADD: Movie title matching from static database

class MovieTitleMatcher:
    """Match Reddit post text to TMDB movie titles"""
    
    def __init__(self, tmdb_movies_path):
        # Load TMDB movie titles from batch layer export
        self.movies_df = spark.read.parquet(tmdb_movies_path)
        self.movie_titles = self.movies_df.select("title").collect()
    
    def match_movie(self, text):
        """Find best matching movie title in text"""
        for movie in self.movie_titles:
            if movie.title.lower() in text.lower():
                return movie.title
        return None
```

#### 🔧 MODIFY - Cassandra Schema

**File**: `layers/speed_layer/cassandra_views/schema.cql`

**Current Tables**:
- `review_sentiments` (TMDB reviews)
- `movie_stats` (TMDB ratings)
- `trending_movies` (TMDB popularity)

**New Tables**:

```sql
-- Reddit sentiment aggregations (replaces review_sentiments)
CREATE TABLE IF NOT EXISTS reddit_sentiments (
    movie_title text,
    hour timestamp,
    window_start timestamp,
    window_end timestamp,
    avg_sentiment double,
    post_count int,
    comment_count int,
    positive_count int,
    negative_count int,
    neutral_count int,
    total_upvotes int,
    total_awards int,
    sentiment_velocity double,  -- Change per hour
    PRIMARY KEY (movie_title, hour, window_start)
) WITH default_time_to_live = 172800
  AND CLUSTERING ORDER BY (hour DESC, window_start DESC);

-- Reddit engagement metrics (replaces movie_stats)
CREATE TABLE IF NOT EXISTS reddit_engagement (
    movie_title text,
    hour timestamp,
    upvote_velocity double,      -- Upvotes per hour
    comment_velocity double,     -- Comments per hour
    award_velocity double,       -- Awards per hour
    cross_subreddit_count int,   -- Number of subreddits discussing
    viral_score double,          -- Combined viral metric
    last_updated timestamp,
    PRIMARY KEY (movie_title, hour)
) WITH default_time_to_live = 172800
  AND CLUSTERING ORDER BY (hour DESC);

-- Viral content detection (replaces trending_movies)
CREATE TABLE IF NOT EXISTS viral_movies (
    hour timestamp,
    rank int,
    movie_title text,
    viral_score double,
    upvote_velocity double,
    award_count int,
    subreddit_spread int,
    PRIMARY KEY (hour, rank, movie_title)
) WITH default_time_to_live = 172800
  AND CLUSTERING ORDER BY (rank ASC);
```

**Migration Strategy**:
1. Create new tables alongside old ones
2. Run dual-write during transition (if needed)
3. Drop old tables after validation

---

### Phase 2: Batch Layer Transformation (MEDIUM IMPACT)

#### ✅ KEEP - Infrastructure
- **Airflow**: Orchestration (DAG scheduling)
- **Spark**: Processing framework
- **MinIO**: Data lake storage
- **PostgreSQL**: Airflow metadata
- **MongoDB**: Final serving storage

#### 🔧 MODIFY - Airflow DAG

**File**: `layers/batch_layer/airflow_dags/tmdb_batch_pipeline.py`

**Current DAG**:
```python
tmdb_batch_pipeline:
  - bronze_ingest (fetch TMDB reviews)
  - silver_transform (clean + sentiment)
  - gold_aggregate (genre aggregations)
  - export_to_mongo (write to batch_views)
```

**New DAG** (SIMPLIFIED):
```python
tmdb_baseline_pipeline:
  - fetch_tmdb_metadata (movie titles, genres, release dates)
  - fetch_tmdb_reviews (historical reviews for baseline calculation)
  - calculate_baselines (genre sentiment baselines, vote thresholds)
  - export_to_mongo (write to batch_views)
```

**Key Changes**:
- **REMOVE**: Real-time review polling (speed layer handles this)
- **ADD**: Baseline calculation logic
- **REDUCE**: Run frequency from every 4 hours → daily (baselines change slowly)

**New Schedule**:
```python
schedule_interval='0 2 * * *'  # Daily at 2 AM (was: every 4 hours)
```

#### 🔧 MODIFY - Spark Jobs

**File**: `layers/batch_layer/spark_jobs/bronze_ingest.py`

**Current**: Fetch TMDB reviews + metadata
**New**: Fetch TMDB metadata only (reviews optional, for baselines)

```python
class TMDBBaselineIngestion:
    """Fetch TMDB metadata for baseline calculation"""
    
    def fetch_movies(self):
        """Fetch movie metadata from TMDB"""
        # Discover movies (popular, recent, etc.)
        movies = []
        for page in range(1, 100):  # Fetch ~2000 movies
            response = requests.get(
                f"{self.base_url}/movie/popular",
                params={'api_key': self.api_key, 'page': page}
            )
            movies.extend(response.json()['results'])
        
        # Save to MinIO bronze/tmdb_movies/
        self._write_to_minio(movies, "bronze/tmdb_movies/")
    
    def fetch_genres(self):
        """Fetch genre list for baseline grouping"""
        response = requests.get(
            f"{self.base_url}/genre/movie/list",
            params={'api_key': self.api_key}
        )
        genres = response.json()['genres']
        self._write_to_minio(genres, "bronze/tmdb_genres/")
```

**File**: `layers/batch_layer/spark_jobs/silver_transform.py`

**REMOVE**: Review sentiment analysis (moved to speed layer for Reddit)
**ADD**: Baseline calculation logic

```python
class BaselineCalculation:
    """Calculate historical baselines from TMDB data"""
    
    def calculate_genre_baselines(self):
        """Calculate average sentiment by genre"""
        # Read TMDB reviews (if using for baselines)
        reviews_df = spark.read.json("s3a://bronze/tmdb_reviews/")
        
        # Join with movie metadata for genre
        movies_df = spark.read.json("s3a://bronze/tmdb_movies/")
        
        joined = reviews_df.join(movies_df, "movie_id")
        
        # Apply VADER sentiment
        with_sentiment = joined.withColumn(
            "sentiment",
            vader_udf(col("content"))
        )
        
        # Aggregate by genre
        baselines = with_sentiment.groupBy("genre_name") \
            .agg(
                avg("sentiment").alias("avg_sentiment"),
                stddev("sentiment").alias("sentiment_stddev"),
                percentile_approx("vote_count", 0.75).alias("viral_threshold")
            )
        
        # Write to silver/baselines/
        baselines.write.parquet("s3a://silver/genre_baselines/")
```

**File**: `layers/batch_layer/spark_jobs/gold_aggregate.py`

**Current**: Genre-level movie aggregations
**New**: Export baselines for speed layer comparison

```python
# SIMPLIFY: No complex aggregations needed
# Just prepare baselines for MongoDB export

baselines_df = spark.read.parquet("s3a://silver/genre_baselines/")

# Add metadata
final = baselines_df.withColumn("updated_at", current_timestamp()) \
    .withColumn("source", lit("tmdb_batch"))

# Write to gold/
final.write.parquet("s3a://gold/baselines/")
```

**File**: `layers/batch_layer/spark_jobs/export_to_mongo.py`

**MODIFY**: Change collection name + schema

```python
# Write to batch_views collection with baseline schema
baselines = spark.read.parquet("s3a://gold/baselines/")

baselines.write \
    .format("mongo") \
    .mode("overwrite") \
    .option("uri", "mongodb://serving-mongodb:27017") \
    .option("database", "tmdb_analytics") \
    .option("collection", "batch_views") \  # New schema
    .save()
```

#### ❌ REMOVE - Batch Layer Components

**Files to DELETE**:
- `layers/batch_layer/master_dataset/ingestion.py` (no master dataset needed)
- Any complex aggregation jobs for real-time metrics (moved to speed layer)

---

### Phase 3: Serving Layer Transformation (LOW IMPACT)

#### ✅ KEEP - Infrastructure
- **FastAPI**: REST API framework
- **Redis**: Caching layer
- **MongoDB**: Unified storage
- **Query Router**: 48-hour cutoff logic

#### 🔧 MODIFY - Query Logic

**File**: `layers/serving_layer/query_engine/query_router.py`

**Current**: Merge TMDB batch + speed data
**New**: Merge Reddit speed + TMDB baselines

```python
class QueryRouter:
    """Route queries between Reddit (speed) and TMDB (batch)"""
    
    def get_movie_sentiment(self, movie_title, time_range="48h"):
        """Get sentiment with baseline comparison"""
        
        # Get Reddit sentiment (last 48 hours) from speed_views
        reddit_sentiment = self.speed_views.find_one({
            "movie_title": movie_title,
            "timestamp": {"$gte": datetime.utcnow() - timedelta(hours=48)}
        })
        
        # Get TMDB baseline from batch_views
        baseline = self.batch_views.find_one({
            "genre": reddit_sentiment.get("genre"),  # Matched genre
            "type": "baseline"
        })
        
        # Compare and flag crisis
        result = {
            "movie_title": movie_title,
            "current_sentiment": reddit_sentiment["avg_sentiment"],
            "baseline_sentiment": baseline["avg_sentiment"],
            "deviation": reddit_sentiment["avg_sentiment"] - baseline["avg_sentiment"],
            "is_crisis": (reddit_sentiment["avg_sentiment"] - baseline["avg_sentiment"]) < -0.15,
            "data_sources": {
                "current": "reddit_speed_layer",
                "baseline": "tmdb_batch_layer"
            }
        }
        
        return result
```

**File**: `layers/serving_layer/query_engine/view_merger.py`

**MODIFY**: Update merge logic for new schema

```python
def merge_views(self, movie_title):
    """Merge Reddit current + TMDB baseline"""
    
    # Speed layer: Reddit engagement metrics
    speed = self.db.speed_views.aggregate([
        {"$match": {"movie_title": movie_title}},
        {"$sort": {"timestamp": -1}},
        {"$limit": 1}
    ])
    
    # Batch layer: TMDB baselines
    batch = self.db.batch_views.find_one({
        "genre": speed["genre"]  # Match by genre
    })
    
    return {
        "movie_title": movie_title,
        "reddit_metrics": {
            "sentiment": speed["avg_sentiment"],
            "upvotes": speed["total_upvotes"],
            "comments": speed["comment_count"],
            "awards": speed["award_count"]
        },
        "tmdb_baselines": {
            "expected_sentiment": batch["avg_sentiment"],
            "viral_threshold": batch["viral_threshold"]
        },
        "analysis": {
            "sentiment_drop": speed["avg_sentiment"] - batch["avg_sentiment"],
            "is_viral": speed["total_upvotes"] > batch["viral_threshold"]
        }
    }
```

#### 🔧 MODIFY - API Endpoints

**File**: `layers/serving_layer/api/routes/sentiment.py`

**ADD**: New endpoints for Reddit vs TMDB comparison

```python
@router.get("/crisis-detection/{movie_title}")
async def detect_crisis(movie_title: str):
    """Detect PR crisis via Reddit sentiment drop"""
    result = query_router.get_movie_sentiment(movie_title)
    return result

@router.get("/viral-content")
async def get_viral_content(threshold: float = 0.75):
    """Identify viral movies (Reddit engagement > TMDB threshold)"""
    viral = query_engine.find_viral_content(threshold)
    return viral
```

#### 🔧 MODIFY - MongoDB Schema

**Collection**: `batch_views` (TMDB baselines)

```json
{
  "genre": "Action",
  "avg_sentiment": 0.65,
  "sentiment_stddev": 0.12,
  "viral_threshold": 5000,  // 75th percentile upvotes
  "type": "baseline",
  "updated_at": "2025-12-02T00:00:00Z",
  "source": "tmdb_batch"
}
```

**Collection**: `speed_views` (Reddit current)

```json
{
  "movie_title": "Dune: Part Two",
  "genre": "Sci-Fi",
  "avg_sentiment": 0.82,
  "post_count": 450,
  "comment_count": 3200,
  "total_upvotes": 8500,
  "award_count": 120,
  "subreddit_spread": 5,
  "timestamp": "2025-12-02T14:35:00Z",
  "window_start": "2025-12-02T14:30:00Z",
  "window_end": "2025-12-02T14:35:00Z",
  "source": "reddit_speed"
}
```

---

## Phase 4: Implementation Roadmap

### Step 1: Preparation (Week 1)
**Goal**: Set up Reddit API access and dependency management

**Tasks**:
1. **Register Reddit API Application**
   - Go to https://www.reddit.com/prefs/apps
   - Create "script" application
   - Save `client_id`, `client_secret`, `user_agent`

2. **Update Dependencies**
   ```bash
   # Speed layer
   echo "praw==7.7.1" >> layers/speed_layer/requirements.txt
   echo "spacy==3.7.2" >> layers/speed_layer/requirements.txt
   
   # Batch layer (no changes needed)
   ```

3. **Update Environment Variables**
   ```bash
   # Add to .env files
   REDDIT_CLIENT_ID=your_client_id
   REDDIT_CLIENT_SECRET=your_client_secret
   REDDIT_USER_AGENT=MoviePipeline/1.0
   TMDB_API_KEY=your_tmdb_key  # Keep for batch layer
   ```

4. **Create Movie Title Database**
   ```bash
   # Run one-time TMDB fetch to build movie title list
   python layers/batch_layer/spark_jobs/fetch_movie_titles.py
   # Output: bronze/tmdb_movies/titles.parquet
   ```

### Step 2: Speed Layer Migration (Week 2-3)
**Goal**: Replace TMDB streaming with Reddit streaming

**Tasks**:
1. **Update Kafka Schemas** ✅
   - Modify `schema_registry.py` with Reddit schemas
   - Test schema registration
   - Validate Avro serialization

2. **Rewrite Producer** ✅
   - Rename `tmdb_stream_producer.py` → `reddit_stream_producer.py`
   - Implement Reddit API streaming logic
   - Add movie title extraction
   - Test with 1 subreddit first (`r/movies`)

3. **Update Spark Streaming Jobs** ✅
   - Modify `review_sentiment_stream.py` for Reddit text
   - Update `movie_aggregation_stream.py` for upvote velocity
   - Rewrite `trending_detection_stream.py` without TMDB API calls

4. **Update Cassandra Schema** ✅
   - Run new table creation scripts
   - Dual-write to old + new tables (parallel testing)
   - Validate data format

5. **Integration Testing** ✅
   - End-to-end test: Reddit → Kafka → Spark → Cassandra
   - Verify 48-hour TTL works
   - Check sentiment accuracy (VADER on Reddit text)

### Step 3: Batch Layer Migration (Week 4)
**Goal**: Simplify batch layer to baseline calculation

**Tasks**:
1. **Update Airflow DAG** ✅
   - Rename DAG: `tmdb_batch_pipeline` → `tmdb_baseline_pipeline`
   - Change schedule: every 4 hours → daily
   - Remove review ingestion task
   - Add baseline calculation task

2. **Update Spark Jobs** ✅
   - Simplify `bronze_ingest.py` (metadata only)
   - Rewrite `silver_transform.py` for baseline calc
   - Modify `gold_aggregate.py` for baseline export
   - Update `export_to_mongo.py` schema

3. **Test Baseline Calculation** ✅
   - Run DAG manually
   - Verify genre baselines in MongoDB
   - Check baseline accuracy (compare to manual calculation)

### Step 4: Serving Layer Migration (Week 5)
**Goal**: Update query logic for Reddit + TMDB comparison

**Tasks**:
1. **Update Query Router** ✅
   - Modify `query_router.py` for new merge logic
   - Update `view_merger.py` schemas
   - Add crisis detection logic

2. **Add API Endpoints** ✅
   - New endpoint: `/crisis-detection/{movie_title}`
   - New endpoint: `/viral-content`
   - Update existing endpoints for new schema

3. **Update MongoDB Indexes** ✅
   ```python
   # Speed views
   db.speed_views.create_index([("movie_title", 1), ("timestamp", -1)])
   db.speed_views.create_index([("viral_score", -1)])
   
   # Batch views
   db.batch_views.create_index([("genre", 1)])
   ```

4. **Integration Testing** ✅
   - Test `/crisis-detection` endpoint
   - Verify 48-hour cutoff works
   - Load test (100 req/sec)

### Step 5: Cleanup & Documentation (Week 6)
**Goal**: Remove old code and update documentation

**Tasks**:
1. **Remove Old Components** ✅
   - Delete old Kafka topics (movie.new_reviews, etc.)
   - Drop old Cassandra tables
   - Remove `tmdb_stream_producer.py`
   - Clean up unused imports

2. **Update Documentation** ✅
   - Update main README.md
   - Update layer-specific READMEs
   - Update architecture diagrams
   - Add migration notes

3. **Final Validation** ✅
   - Run full end-to-end test
   - Monitor for 48 hours
   - Check data quality metrics
   - Verify business goals met

---

## Risk Mitigation

### Risk 1: Reddit API Rate Limits (60/min)
**Impact**: HIGH  
**Mitigation**:
- Use Reddit streaming API (no rate limit for real-time stream)
- Implement exponential backoff
- Cache movie title matches to reduce API calls
- Limit to 4 subreddits initially

### Risk 2: Movie Title Extraction Accuracy
**Impact**: MEDIUM  
**Mitigation**:
- Maintain TMDB movie title database (daily updates)
- Use fuzzy matching (Levenshtein distance)
- Manual curation for top 100 movies
- Add user feedback loop for corrections

### Risk 3: Data Quality (Reddit noise)
**Impact**: MEDIUM  
**Mitigation**:
- Filter by minimum score (e.g., score > 10)
- Exclude bot accounts
- Limit to verified movie subreddits
- Apply sentiment confidence threshold

### Risk 4: Breaking Changes During Migration
**Impact**: HIGH  
**Mitigation**:
- **Parallel Run**: Keep old TMDB system running for 2 weeks
- **Gradual Rollout**: Start with 1 subreddit, then expand
- **Feature Flags**: Use environment variables to toggle old/new logic
- **Rollback Plan**: Keep old Docker images tagged

---

## Success Metrics

### Technical Metrics
- ✅ Reddit API ingestion rate: >100 posts/hour
- ✅ Sentiment analysis latency: <5 minutes end-to-end
- ✅ Cassandra write throughput: >1000 events/sec
- ✅ MongoDB query latency: <100ms p95
- ✅ System uptime: >99.5%

### Business Metrics
- ✅ Crisis detection accuracy: >80% (manual validation)
- ✅ Viral content recall: >90% (catch all viral movies)
- ✅ Baseline accuracy: ±0.05 sentiment score
- ✅ Data freshness: <5 minutes for Reddit data

---

## Rollback Plan

If migration fails, rollback using these steps:

### Emergency Rollback (Immediate)
```bash
# Stop new services
docker-compose -f docker-compose.yml down

# Restore old configuration
git checkout main
git pull origin main

# Restart old system
docker-compose up -d

# Verify old system works
curl http://localhost:8000/api/v1/health
```

### Partial Rollback (Gradual)
- **Speed Layer**: Toggle producer via environment variable
  ```bash
  SPEED_LAYER_SOURCE=tmdb  # Switch back to TMDB
  ```
- **Batch Layer**: Restore old DAG
  ```bash
  cp airflow_dags/tmdb_batch_pipeline.py.backup airflow_dags/tmdb_batch_pipeline.py
  ```
- **Serving Layer**: Use old query router
  ```bash
  QUERY_ROUTER_VERSION=v1  # Old TMDB-only logic
  ```

---

## Appendix: File Modification Checklist

### Speed Layer
- [ ] `kafka_producers/schema_registry.py` - Add Reddit schemas
- [ ] `kafka_producers/tmdb_stream_producer.py` → `reddit_stream_producer.py` - Rewrite
- [ ] `streaming_jobs/review_sentiment_stream.py` - Modify for Reddit
- [ ] `streaming_jobs/movie_aggregation_stream.py` - Add upvote velocity
- [ ] `streaming_jobs/trending_detection_stream.py` - Remove TMDB API calls
- [ ] `cassandra_views/schema.cql` - Add Reddit tables
- [ ] `config/kafka_topics.yaml` - Update topic list
- [ ] `requirements.txt` - Add `praw`, `spacy`
- [ ] `README.md` - Update documentation

### Batch Layer
- [ ] `airflow_dags/tmdb_batch_pipeline.py` - Simplify to baseline calc
- [ ] `spark_jobs/bronze_ingest.py` - Metadata only
- [ ] `spark_jobs/silver_transform.py` - Baseline calculation
- [ ] `spark_jobs/gold_aggregate.py` - Baseline export
- [ ] `spark_jobs/export_to_mongo.py` - Update schema
- [ ] `README.md` - Update documentation

### Serving Layer
- [ ] `query_engine/query_router.py` - Update merge logic
- [ ] `query_engine/view_merger.py` - New schema
- [ ] `api/routes/sentiment.py` - Add crisis detection endpoint
- [ ] `api/routes/trending.py` - Update for viral content
- [ ] `mongodb/indexes.py` - Update indexes
- [ ] `README.md` - Update documentation

### Root
- [ ] `docker-compose.yml` - Update environment variables
- [ ] `README.md` - Update architecture section
- [ ] `.env.example` - Add Reddit credentials
- [ ] `SETUP.md` - Update setup instructions

---

## Conclusion

This migration transforms the system from a **TMDB-only historical analytics pipeline** to a **dual-source real-time crisis detection system**. The key insight is:

> **Reddit provides the "what's happening now" signal, while TMDB provides the "what's normal" baseline.**

By comparing real-time Reddit sentiment against historical TMDB baselines, we can detect PR crises, identify viral content, and optimize recommendations with unprecedented speed and accuracy.

**Estimated Timeline**: 6 weeks  
**Estimated Effort**: 2-3 engineers  
**Risk Level**: Medium (mitigated by parallel run strategy)

**Next Steps**: Review this plan → Approve → Begin Week 1 preparation tasks.
