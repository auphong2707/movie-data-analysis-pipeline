# Movie Social Engagement Analytics Pipeline - Lambda Architecture

A production-ready big data analytics pipeline implementing **Lambda Architecture** to analyze **real-time social engagement from Reddit** combined with **historical review data from TMDB** for sentiment tracking, viral content detection, and audience insight analytics.

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Lambda Architecture Layer Contributions](#-lambda-architecture-layer-contributions)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Core Features](#-core-features)
- [Data Pipeline Architecture](#-data-pipeline-architecture)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [Implementation Status](#-implementation-status)
- [Documentation](#-documentation)
- [Deployment](#-deployment)
- [Monitoring & Operations](#-monitoring--operations)
- [Contributing](#-contributing)
- [License](#-license)

## 🎯 Project Overview

### Business Goals

1. **PR Crisis Detection & Sentiment Monitoring**
   - **Business Need**: Entertainment companies need to respond to PR crises within hours, not days
   - **Solution**: Detect when current Reddit discussion sentiment drops significantly below historical TMDB baselines
   - **Value**: Enable rapid PR response by identifying statistically significant negative sentiment shifts
   - **Example**: Alert marketing teams when "Dune 2" sentiment drops to +0.3 (vs sci-fi baseline +0.65) indicating genuine crisis, not normal variance

2. **Viral Content Identification for Marketing Amplification**
   - **Business Need**: Marketing teams must identify and amplify viral content during its brief 24-48 hour peak window
   - **Solution**: Distinguish genuine viral events from normal fluctuations by comparing real-time Reddit velocity against historical thresholds
   - **Value**: Maximize ROI on marketing spend by identifying organic viral moments worthy of paid amplification
   - **Example**: Detect "The Creator" discussion at 500 upvotes/hour (10x baseline of 50/hour) triggering viral amplification campaign

3. **Content Recommendation Optimization**
   - **Business Need**: Streaming platforms must surface trending content while it's hot to maximize engagement and subscriptions
   - **Solution**: Re-rank recommendations by combining fresh Reddit buzz with historical TMDB performance data
   - **Value**: Increase user engagement by surfacing content that exceeds both current social buzz AND historical quality benchmarks
   - **Example**: Prioritize "Barbie" (Reddit: 2,000 comments/day + +0.9 sentiment) over competitors based on dual success metrics

### Data Scope & Characteristics

#### Primary Data Sources

**Reddit API (Speed Layer - Real-Time Social Engagement)**
- **Data Types**: Posts, comments, upvotes, Reddit awards, cross-posts from r/movies, r/boxoffice, r/TrueFilm
- **Volume**: 500-2,000 posts/day + 10K-50K comments/day across movie subreddits
- **Update Frequency**: 30-second polling (near real-time)
- **Authentication**: None required (public JSON endpoints: `/r/subreddit.json`)
- **Retention**: Last 48 hours in speed layer (Cassandra TTL)
- **Real-Time Nature**: Discussions happen continuously; posts can go viral (0→10K upvotes) within hours
- **Current Status**: ✅ **OPERATIONAL** - Pipeline extracting TMDB-validated movie titles (TMDB cache active)

**TMDB API (Batch Layer + Speed Layer Movie Validation)**
- **Data Types**: Movie metadata (genres, budget, runtime, release dates), vote counts, popularity scores, limited reviews for sentiment baselines
- **Volume**: ~2,000 movies metadata, ~50 movies with reviews for baseline sentiment calculation
- **Update Frequency**: Daily at 2 AM (baseline recalculation)
- **API Rate Limit**: 4 requests/second
- **Storage**: MinIO/S3-compatible storage (Bronze/Silver/Gold layers)
- **Purpose**: 
  - **Batch Layer**: Calculate genre/franchise sentiment baselines, viral thresholds, and movie intelligence
  - **Speed Layer**: Validate Reddit-extracted movie titles against TMDB database (fuzzy matching)
- **Current Status**: ✅ **INTEGRATED** - Speed layer validates against TMDB database (4 categories: popular, top_rated, now_playing, upcoming)

#### Data Flow Strategy

- **Speed Layer Focus**: Reddit discussions, upvote velocity, community engagement, sentiment analysis (last 48 hours)
- **Batch Layer Focus**: TMDB movie metadata, sentiment baselines, viral thresholds, genre trends (historical only)
- **Merge Strategy**: Query-time merge with 48-hour cutoff - recent Reddit data + historical TMDB baselines
- **Languages**: Primarily English-language content
- **Lambda Architecture Justification**: Reddit provides high-velocity social engagement data; TMDB provides historical movie intelligence and statistical baselines
- **Implementation Note**: Speed layer uses Reddit JSON scraping (no auth); batch layer uses TMDB API

## 🧩 Lambda Architecture Layer Contributions

This section details how each layer of the Lambda Architecture contributes to solving the three business problems.

### Business Problem #1: Multi-Source Sentiment Monitoring

**Goal**: Detect sentiment shifts and PR crises by comparing real-time Reddit discussions against historical TMDB review baselines.

#### Speed Layer Contribution (Reddit API - No Authentication)
**What it processes:**
- Real-time Reddit posts and comments from r/movies, r/boxoffice, r/TrueFilm (JSON scraping)
- Live sentiment analysis on incoming discussions using VADER
- Comment velocity tracking (comments per hour)
- Upvote/downvote patterns indicating agreement/disagreement
- Movie title extraction from post titles and comment bodies

**Output:**
- Current Reddit sentiment score (e.g., +0.3 for "Dune 2" in last 48h)
- Sentiment velocity: "Dropped from +0.8 to +0.3 in last 6 hours"
- Discussion volume: "450 comments in last 24 hours"
- Viral metrics: Upvote velocity, award velocity, cross-subreddit spread

**Limitation:** Cannot determine if +0.3 is good/bad without historical TMDB baselines.

#### Batch Layer Contribution (TMDB API - Historical Only)
**What it processes:**
- TMDB movie metadata (~2,000 movies)
- Limited reviews from top 50 movies for baseline sentiment calculation
- Genre-specific sentiment baselines (e.g., sci-fi avg: +0.65)
- Franchise patterns from movie metadata (e.g., Dune franchise)
- Budget tier analysis (indie/mid/blockbuster)
- Seasonal and temporal patterns

**Output:**
- Historical baseline: "Sci-fi films average +0.65 sentiment" (from sample reviews)
- Viral thresholds: "Action blockbuster summer threshold: 29,058 votes"
- Movie intelligence: Individual movie metadata with aggregated metrics
- Statistical context: Genre norms, franchise expectations, budget tier benchmarks

**Limitation:** Daily refresh (not real-time); limited review sample; no real-time TMDB streaming.

#### Merged Result (Serving Layer)
**Combined Intelligence:**
```
Query: "Is Dune 2 having a PR crisis?"

Speed Layer: Current Reddit sentiment +0.3 (dropped from +0.8)
Batch Layer: Sci-fi baseline +0.65, Dune 1 was +0.78, normal variance ±0.15

Merged Answer:
✅ ALERT: PR Crisis Detected
- Current +0.3 is -0.35 below genre baseline (beyond normal ±0.15 variance)
- Drop of -0.5 in 6 hours exceeds historical drop patterns
- Significantly below franchise expectation (+0.78 for Dune 1)
Recommendation: Immediate PR response required
```

**Value of Lambda Architecture:** Speed layer detects the drop; batch layer proves it's statistically significant.

---

### Business Problem #2: Viral Content & Trending Detection

**Goal**: Identify breakout content by comparing real-time Reddit engagement velocity against historical viral thresholds.

#### Speed Layer Contribution (Reddit API)
**What it processes:**
- Upvote velocity tracking (upvotes per hour)
- Cross-subreddit spread monitoring (r/movies → r/all)
- Reddit award velocity (gold/platinum per hour)
- Comment acceleration (new comments per 5-min window)
- Post age vs engagement ratio

**Output:**
- Current velocity: "500 upvotes/hour for 'The Creator' discussion"
- Cross-subreddit spread: "Appeared in 5 movie-related subs in 2 hours"
- Award rate: "Received 3 gold awards in 30 minutes"

**Limitation:** Cannot determine if 500 upvotes/hour is viral without historical benchmarks.

#### Batch Layer Contribution (TMDB API)
**What it processes:**
- Historical vote velocity patterns from TMDB (vote count changes over time)
- Genre-specific viral thresholds calculated from years of data
- Budget tier analysis (indie vs blockbuster viral patterns)
- Historical viral case studies (e.g., "Everything Everywhere All At Once" vote patterns)
- Seasonal trending baselines (summer vs winter viral thresholds)

**Output:**
- Genre baseline: "Mid-budget sci-fi: 50 votes/hour average, 150 votes/hour top quartile"
- Viral threshold: "300+ votes/hour = 99th percentile (viral territory)"
- Budget tier context: "$80M budget films: 2.5x coefficient = breakout"
- Historical viral cases: "EEAAO peaked at 800 votes/hour during viral surge"

**Limitation:** Historical thresholds don't capture real-time viral momentum.

#### Merged Result (Serving Layer)
**Combined Intelligence:**
```
Query: "Is 'The Creator' going viral?"

Speed Layer: 500 upvotes/hour, cross-posted to 5 subs, 3 gold awards in 30 min
Batch Layer: Genre baseline 50/hour, viral threshold 300/hour (99th percentile)

Merged Answer:
✅ VIRAL EVENT CONFIRMED
- Current 500/hour is 10x genre baseline (viral coefficient 10x)
- Exceeds 99th percentile threshold (300/hour) by 67%
- Cross-subreddit spread indicates organic viral momentum
- Award velocity (6/hour rate) matches historical viral patterns
Recommendation: Amplify marketing; expect 24-48h sustained viral window
```

**Value of Lambda Architecture:** Speed layer captures viral momentum; batch layer validates it's genuinely exceptional.

---

### Business Problem #3: Audience Insight & Competitive Intelligence

**Goal**: Provide competitive context for content performance by combining real-time Reddit engagement with historical TMDB review patterns.

#### Speed Layer Contribution (Reddit API)
**What it processes:**
- Current discussion volume across multiple subreddits
- Real-time sentiment on competing releases (same weekend)
- Discussion topic extraction (spoilers vs non-spoilers, specific scenes)
- User engagement quality (award-giving indicates strong reactions)
- Community buzz intensity (comments per post ratio)

**Output:**
- Current engagement: "Barbie: 2,000 comments/day, +0.9 sentiment"
- Competitor comparison: "Oppenheimer: 800 comments/day, +0.92 sentiment"
- Discussion quality: "Barbie avg 12 awards/post vs Oppenheimer 6 awards/post"

**Limitation:** Cannot determine if this performance is historically strong without baselines.

#### Batch Layer Contribution (TMDB API)
**What it processes:**
- Complete historical review archive for genre/franchise context
- Cross-movie comparisons (similar films, franchises, directors)
- Release timing analysis (summer blockbusters, award season, etc.)
- Genre evolution trends (comedy sentiment 2020: +0.75 → 2023: +0.68)
- Franchise trajectory patterns (sequel performance vs originals)

**Output:**
- Genre baseline: "Summer comedies average +0.71 sentiment"
- Franchise context: "Toy Story 4 (IP revival): +0.85, 3,400 reviews"
- Competitive historical: "July releases face 15% higher competition"
- Trend analysis: "Family films stable at +0.80 across 5 years"

**Limitation:** Historical aggregations updated every 4 hours; misses breaking trends.

#### Merged Result (Serving Layer)
**Combined Intelligence:**
```
Query: "Should we prioritize Barbie in recommendations?"

Speed Layer:
- Barbie: 2,000 Reddit comments/day, +0.9 sentiment, 12 awards/post
- Oppenheimer: 800 comments/day, +0.92 sentiment, 6 awards/post

Batch Layer:
- Genre baseline: Summer comedies +0.71 avg
- Historical comp: Toy Story 4 was +0.85 sentiment, 3,400 TMDB reviews
- Trend: Comedy sentiment declining (2023 avg: +0.68)

Merged Answer:
✅ PRIORITIZE BARBIE - Rare Dual Success
- Reddit engagement 2.5x higher than competitor (2,000 vs 800 comments)
- Sentiment +0.9 beats genre baseline (+0.71) by +0.19
- Exceeds declining genre trend (+0.68) significantly
- Higher engagement quality (awards/post) than competitor
- Comparable to Toy Story 4 historical success pattern
Recommendation: Top placement in recommendations; expect sustained performance
```

**Value of Lambda Architecture:** Speed layer shows current buzz; batch layer proves it's beating historical expectations.

---

### Summary: Why Both Layers Are Essential

| **Layer** | **Contribution** | **Limitation Without Other Layer** |
|-----------|------------------|-------------------------------------|
| **Speed Layer (Reddit)** | Real-time signals, viral detection, immediate sentiment shifts | Cannot distinguish signal from noise; no context for "is this good?" |
| **Batch Layer (TMDB)** | Historical baselines, statistical thresholds, genre/franchise context | 4-hour lag; cannot detect breaking trends or PR crises |
| **Merged (Serving)** | Contextualized real-time insights: "Current performance vs historical norms" | — |

**Key Insight:** 
- Speed layer (Reddit) answers: **"What is happening NOW in social discussions?"**
- Batch layer (TMDB) answers: **"Is this NORMAL or EXCEPTIONAL based on historical data?"**
- Lambda Architecture answers: **"What is happening NOW, and should we ACT on it?"**

**Implementation Status:**
- ✅ **SPEED LAYER OPERATIONAL**: Movie extraction fixed with TMDB validation (313 movies cached)
- ✅ Reddit data collection operational (posts + comments in Kafka)
- ✅ Spark Streaming operational (5-minute windows, VADER sentiment, viral metrics)
- ✅ Cassandra storage operational (post + comment metrics, 48h TTL)
- ✅ MongoDB sync operational (clean TMDB-validated dataset)
- ✅ TMDB validation active (0.8 similarity threshold, fuzzy matching, year extraction)
- ✅ Data quality verified: Real movie titles only (Fight Club, The Avengers, Shrek, etc.)
- ❓ Batch layer TMDB integration needs verification
- ❓ Serving layer merger needs verification

## 🏗️ Architecture

```
                    ┌─────────────────────────────────────┐
                    │         TMDB API                    │
                    │    (4 requests/second limit)        │
                    └────────────┬──────────────┬─────────┘
                                 │              │
                                 │              │
                    ┌────────────▼──────┐  ┌────▼─────────────┐
                    │   BATCH LAYER     │  │   SPEED LAYER    │
                    │ (Historical Data) │  │ (Real-time Data) │
                    │                   │  │                  │
                    │ • HDFS Storage    │  │ • Kafka Streaming│
                    │ • Spark Batch     │  │ • Cassandra      │
                    │ • Airflow         │  │ • Spark Streaming│
                    │                   │  │                  │
                    │ Every 4 hours     │  │ 5-min windows    │
                    │ Complete accuracy │  │ Low latency      │
                    │ (> 48 hours old)  │  │ (≤ 48 hours old) │
                    └────────────┬──────┘  └────┬─────────────┘
                                 │              │
                                 │              │
                                 └──────┬───────┘
                                        │
                              ┌─────────▼──────────┐
                              │  SERVING LAYER     │
                              │                    │
                              │ • MongoDB (merged  │
                              │   batch + speed    │
                              │   views)           │
                              │ • FastAPI REST API │
                              │ • Apache Superset  │
                              │ • Grafana          │
                              │                    │
                              │ Query-time merge   │
                              └────────────────────┘
```

### Lambda Architecture Components

The pipeline implements Nathan Marz's Lambda Architecture pattern with three distinct layers:

**Batch Layer**: Processes complete historical social engagement data (>48 hours old)
- Full review history with sentiment analysis
- Comprehensive rating and voting patterns
- Genre and temporal trend aggregations
- Reprocessing capability for corrections
- Higher latency acceptable (4-hour refresh)

**Speed Layer**: Processes recent user activity for low latency (≤48 hours old)  
- New reviews streamed in near real-time (30s polling)
- Vote count velocity and popularity changes
- Real-time sentiment scoring on fresh reviews
- Trending detection based on recent engagement
- Sub-5-minute processing latency

**Serving Layer**: Merges batch accuracy with speed freshness
- 48-hour cutoff merge strategy (recent speed data + historical batch data)
- Unified query interface combining both views
- Trending dashboards with minute-level freshness
- Historical analytics with complete accuracy

## 🛠️ Technology Stack

### Batch Layer Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow | Schedule & manage batch jobs (4-hour intervals) |
| **Processing** | Apache Spark (Batch) | Transform data through Bronze → Silver → Gold |
| **Storage** | HDFS (Hadoop 3.x) | Distributed storage for all data layers |
| **Data Quality** | Great Expectations | Validate data at each transformation stage |

### Speed Layer Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Streaming** | Apache Kafka | Message queue for real-time data ingestion |
| **Processing** | Spark Structured Streaming | Process data in 5-minute windows |
| **Storage** | Apache Cassandra | Low-latency writes with 48h TTL auto-expiration |
| **Schema** | Confluent Schema Registry | Avro schema management for Kafka topics |

### Serving Layer Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | MongoDB | Unified storage for batch + speed views |
| **API** | FastAPI | High-performance async REST API endpoints |
| **Caching** | Redis | Response caching for frequently accessed data |
| **Visualization** | Grafana | Real-time dashboards and system monitoring |

### Cross-Cutting Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Metadata Management** | DataHub | Data catalog, lineage tracking, governance |
| **Orchestration** | Kubernetes | Container orchestration for all services |
| **Development** | Docker Compose | Lightweight local development environment |
| **Monitoring** | Prometheus | Metrics collection and alerting |
| **Version Control** | Git | Source code management |

## 🎨 Core Features

### Real-Time Social Engagement Analytics
- **Live Review Stream**: Process new TMDB reviews within seconds of posting
- **Sentiment Analysis**: VADER-based sentiment scoring on fresh user reviews
- **Trending Detection**: Identify viral content based on vote velocity and review spikes
- **Engagement Velocity**: Track rating activity, review volume, and popularity changes
- **Crisis Detection**: Alert on sudden negative sentiment patterns for PR management

### Historical Community Insights
- **Sentiment Trends**: Long-term sentiment patterns by genre, release period, and tier
- **Review Analytics**: Comprehensive review statistics and author patterns
- **Engagement Patterns**: User behavior analysis (rating sprees, watchlist trends)
- **Release Performance**: First-week/month review sentiment and volume tracking
- **Competitive Analysis**: Cross-movie sentiment comparison for similar content

### Query Capabilities
- **Fast Queries**: <100ms p95 latency through MongoDB + Redis caching
- **Fresh Data**: Sub-5-minute freshness from speed layer (recent reviews/ratings)
- **Deep History**: 5-year historical sentiment data from batch layer
- **Flexible Search**: Full-text review search with sentiment, genre, and time filters
- **Trending Endpoints**: Real-time "what's hot now" based on recent engagement

### Data Quality
- **Schema Validation**: Automated validation at each layer
- **Deduplication**: Intelligent duplicate removal by review_id and movie_id
- **Completeness Checks**: >95% data quality target
- **Anomaly Detection**: Statistical outlier identification in sentiment and engagement

## 📊 Data Pipeline Architecture

### Batch Layer Flow

```
TMDB API (scheduled extraction)
    ↓ (Airflow DAG - daily at 2 AM)
┌───────────────────────────────────────┐
│         BRONZE LAYER (MinIO)          │
│  • Raw JSON → Parquet                 │
│  • Movie metadata, genres, sample     │
│    reviews from top 50 movies         │
│  • Partition: /data_type              │
│  • No transformations (immutable)     │
└────────────────┬──────────────────────┘
                 ↓ (Spark Batch Job)
┌───────────────────────────────────────┐
│         SILVER LAYER (MinIO)          │
│  • Enrichment (budget tiers, genres)  │
│  • Baseline sentiment calculation     │
│  • Viral threshold computation        │
│  • Movie intelligence aggregation     │
│  • Partition: /dataset_type           │
└────────────────┬──────────────────────┘
                 ↓ (Spark Union)
┌───────────────────────────────────────┐
│          GOLD LAYER (MinIO)           │
│  • Unified batch_views dataset        │
│  • view_type discriminator field      │
│  • Temporal metadata added            │
│  • Partition: /batch_views            │
└────────────────┬──────────────────────┘
                 ↓ (Export to Serving)
┌───────────────────────────────────────┐
│      MONGODB (Batch Views)            │
│  • Collection: batch_views            │
│  • Sentiment baselines, viral         │
│    thresholds, movie intelligence     │
│  • Updated daily at 2 AM              │
│  • 12 compound indexes for queries    │
└───────────────────────────────────────┘
```

### Speed Layer Flow (✅ Fully Operational - Reddit-based)

```
Reddit API (r/movies, r/boxoffice, r/TrueFilm)
    ↓ (30-second polling, no auth required)
┌───────────────────────────────────────┐
│  REDDIT PRODUCER (Python + requests)  │ ✅ OPERATIONAL
│  • Polls 3 subreddits every 30s       │
│  • JSON scraping (no credentials)     │
│  • TMDB validation (cache active)     │
│  • Fuzzy matching (0.8 threshold)     │
└────────────────┬──────────────────────┘
                 ↓ (Kafka topics: reddit.posts, reddit.comments)
┌───────────────────────────────────────┐
│          KAFKA TOPICS                 │ ✅ OPERATIONAL
│  • reddit.posts                       │
│  • reddit.comments                    │
│  • Partitions: 3 per topic            │
│  • Retention: 48 hours                │
│  • Auto-created topics                │
└────────────────┬──────────────────────┘
                 ↓ (Spark Structured Streaming)
┌───────────────────────────────────────┐
│      REAL-TIME PROCESSING             │ ✅ OPERATIONAL
│  • 5-minute tumbling windows          │
│  • VADER sentiment analysis           │
│  • Upvote/comment/award velocity      │
│  • Movie title aggregation            │
│  • Viral score calculation            │
│  • 30-second watermark for low latency│
└────────────────┬──────────────────────┘
                 ↓ (Write to Cassandra)
┌───────────────────────────────────────┐
│      CASSANDRA (Speed Views)          │ ✅ POPULATED
│  • reddit_post_metrics                │
│  • reddit_comment_metrics             │
│  • TTL: 48 hours (auto-expire)        │
│  • Partition: (movie_title, hour)     │
│  • Clean TMDB-validated titles        │
└────────────────┬──────────────────────┘
                 ↓ (Sync every 5 min)
┌───────────────────────────────────────┐
│      MONGODB (Speed Views)            │ ✅ OPERATIONAL
│  • Collection: speed_views            │
│  • Sync connector: RUNNING            │
│  • Clean data (old bad data removed)  │
│  • TMDB-validated titles only         │
└───────────────────────────────────────┘
```

### Serving Layer Flow

```
┌──────────────┐         ┌──────────────┐
│   MongoDB    │         │   MongoDB    │
│ batch_views  │         │ speed_views  │
│ (historical) │         │ (last 48h)   │
│ (>48h old)   │         │ (≤48h old)   │
└──────┬───────┘         └──────┬───────┘
       │                        │
       └────────┬───────────────┘
                ↓
        ┌───────────────┐
        │ Query Router  │  • 48-hour cutoff logic
        │ & Merger      │  • Merge batch + speed
        └───────┬───────┘  • Deduplicate results
                │
                ↓
        ┌───────────────┐
        │  Redis Cache  │  • 5-15 minute TTL
        │               │  • Frequently accessed data
        └───────┬───────┘
                │
                ↓
        ┌───────────────┐
        │   FastAPI     │  • REST API endpoints
        │               │  • <100ms p95 latency
        └───────┬───────┘  • Authentication & rate limiting
                │
                ↓
        ┌───────────────┐
        │    Grafana    │  • Real-time dashboards
        │               │  • System monitoring
        └───────────────┘  • 5 pre-built dashboards
```

## 📊 Data Schema & Features

### Batch Layer (Historical Social Engagement Data)

**Bronze Layer** - Raw ingestion from TMDB API
- **Movie Metadata**: Basic info (title, genres, release_date, runtime, budget, popularity)
- **Limited Reviews**: Reviews from top 50 movies for baseline sentiment calculation
- **Genre Data**: Complete genre list from TMDB
- **Storage**: Parquet in MinIO partitioned by data type (tmdb_movies, tmdb_reviews, tmdb_genres)

**Silver Layer** - Cleaned and enriched
- **Enriched Movies**: Movies joined with genre names, budget tiers, seasonal classification
- **Sentiment Baselines**: Genre/franchise/year sentiment aggregations from review samples
- **Viral Thresholds**: Genre×budget×season popularity thresholds for viral detection
- **Movie Intelligence**: Individual movie data with all metrics for competitive analysis
- **Storage**: Parquet in MinIO partitioned by dataset type

**Gold Layer** - Unified export preparation
- **Unified Batch Views**: Single dataset with view_type discriminator (sentiment_baseline, viral_threshold, movie_intelligence)
- **Temporal Metadata**: Batch run timestamps, aggregation granularity, data period ranges
- **Export Ready**: Formatted for MongoDB upsert with proper indexes
- **Storage**: Parquet + MongoDB batch_views collection

### Speed Layer (Real-Time Reddit Social Engagement)

> **✅ FULLY OPERATIONAL** - Complete Reddit data pipeline with real-time sentiment analysis

**Kafka Topics (✅ OPERATIONAL)** - Event streams (30-second polling)
- **`reddit.posts`**: Reddit posts from r/movies, r/boxoffice, r/TrueFilm
- **`reddit.comments`**: Reddit comments with movie mentions
- **Partitions**: 3 per topic for parallel processing
- **Retention**: 48 hours auto-cleanup
- **TMDB Validation**: Producer validates all extracted titles against 313-movie cache

**Cassandra Tables (✅ OPERATIONAL)** - 5-minute windows (48-hour TTL)
- **`reddit_post_metrics`**: Post-level metrics with viral scores
- **`reddit_comment_metrics`**: Comment-level sentiment aggregations
- **Metrics**: upvote_velocity, comment_velocity, award_velocity, avg_sentiment, viral_score
- **Partition Key**: (movie_title, hour, window_start)
- **Data Quality**: Only TMDB-validated movie titles (Fight Club, The Avengers, Shrek, etc.)
- **Status**: Actively receiving data from Spark Streaming

**MongoDB speed_views (✅ OPERATIONAL)** - Active sync every 5 minutes
- Collection: `speed_views`
- Data types: `reddit_post`, `reddit_comment`
- Sync connector: `reddit-cassandra-sync` container running
- Auto TTL: 48 hours (documents expire automatically)
- Features: Recent Reddit sentiment, viral metrics, upvote velocity, trending signals

### Serving Layer (Merged Views)

**MongoDB Collections**
- **`batch_views`**: Historical sentiment analytics, complete review stats, long-term trends
- **`speed_views`**: Real-time engagement (last 48h), fresh reviews, trending signals
- **Merge Strategy**: 48-hour cutoff - speed data for recent activity, batch data for historical accuracy

**API Response Features**
- Sentiment scores with velocity trends
- Review volume patterns and spikes
- Vote count changes and momentum indicators
- Trending rankings with composite scores
- Time-series sentiment breakdowns
- Genre-based engagement comparisons

**Key Metrics Exposed**
- **Sentiment**: Average score (-1 to 1), positive/negative/neutral distribution, velocity
- **Engagement**: Review volume, vote velocity, rating momentum
- **Trending**: Composite trending score, acceleration metrics, viral detection
- **Temporal**: Daily/weekly/monthly aggregations, first-week performance

## 📁 Project Structure

```
movie-data-analysis-pipeline/
├── README.md                          # This file
├── LICENSE                            # MIT License
├── requirements.txt                   # Python dependencies
├── docker-compose.yml                 # Local development setup
│
├── layers/                           # Lambda Architecture layers
│   ├── batch_layer/                  # Historical processing
│   │   ├── README.md                 # Detailed batch layer docs
│   │   ├── airflow_dags/            # Orchestration workflows
│   │   ├── spark_jobs/              # Bronze → Silver → Gold
│   │   ├── master_dataset/          # TMDB ingestion
│   │   │   └── ingestion.py         # Raw data extraction
│   │   ├── batch_views/             # Pre-computed views
│   │   ├── config/                  # Spark/HDFS configs
│   │   └── tests/                   # Unit tests
│   │
│   ├── speed_layer/                 # Real-time processing
│   │   ├── README.md                # Detailed speed layer docs
│   │   ├── kafka_producers/         # Data streaming
│   │   │   └── tmdb_stream_producer.py
│   │   ├── streaming_jobs/          # Spark Structured Streaming
│   │   ├── cassandra_views/         # Speed view schemas
│   │   ├── connectors/              # Cassandra → MongoDB sync
│   │   ├── config/                  # Kafka/Cassandra configs
│   │   └── tests/                   # Unit tests
│   │
│   └── serving_layer/               # Query interface
│       ├── README.md                # Detailed serving layer docs
│       ├── api/                     # FastAPI REST endpoints
│       │   └── main.py              # API entry point
│       ├── query_engine/            # View merger logic
│       ├── mongodb/                 # Database layer
│       ├── visualization/           # Superset & Grafana
│       ├── config/                  # API/MongoDB configs
│       └── tests/                   # API & integration tests
│
├── kubernetes/                       # Production deployment
│   ├── README.md                    # Kubernetes deployment guide
│   ├── namespace.yaml               # Namespace definition
│   ├── configmap.yaml              # Configuration & secrets
│   ├── kafka.yaml                  # Kafka cluster
│   ├── minio.yaml                  # Object storage (HDFS alternative)
│   ├── mongodb.yaml                # MongoDB replica set
│   ├── spark.yaml                  # Spark cluster
│   ├── applications.yaml           # Application deployments
│   ├── monitoring.yaml             # Prometheus & Grafana
│   ├── visualization.yaml          # Apache Superset
│   └── deploy.sh                   # Automated deployment script
│
├── docs/                            # Additional documentation
│   └── Movie Data Analysis Pipeline.drawio  # Architecture diagrams
│
└── tests/                           # Integration tests
    └── (test files)
```

## 🚀 Quick Start

> **✨ NEW: Unified Setup Available!**  
> The batch and speed layers are now combined into a single setup at the project root with consistent naming conventions.

### Prerequisites

- **Docker Desktop** or **Docker Engine** (version 20.10+)
- **Docker Compose** (version 1.29+)
- **At least 8GB RAM** allocated to Docker
- **TMDB API Key** (free from [themoviedb.org](https://www.themoviedb.org/settings/api))

### Unified Setup (Recommended)

The unified setup runs both Batch Layer and Speed Layer with a single command:

1. **Clone the Repository**
   ```bash
   git clone https://github.com/auphong2707/movie-data-analysis-pipeline.git
   cd movie-data-analysis-pipeline
   ```

2. **Configure Environment Variables**
   ```bash
   # Copy template and add your TMDB API key
   cp .env.example .env
   nano .env  # Set TMDB_API_KEY=your_key_here
   ```

3. **Start All Services**
   ```bash
   # Start complete infrastructure (Batch + Speed layers)
   docker-compose up -d
   
   # Verify all services are running
   docker-compose ps
   ```

4. **Access Web Interfaces**
   - **Airflow (Batch Layer)**: http://localhost:8088 (admin/admin)
   - **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
   - **Schema Registry**: http://localhost:8081

For detailed instructions and troubleshooting, see the layer-specific READMEs:
- **Batch Layer**: See `layers/batch_layer/README.md`
- **Speed Layer**: See `layers/speed_layer/README.md`

### Running the Pipeline

**Batch Layer** (historical data processing):
```bash
# Trigger Airflow DAG manually or wait for scheduled run
# Access Airflow UI at http://localhost:8088
```

**Speed Layer** (real-time streaming):
```bash
# Automatically starts with docker-compose
# View logs: docker-compose logs -f speed-tmdb-producer speed-sentiment-stream
```

**Query Results**:
```bash
# Connect to MongoDB
docker exec -it serving-mongodb mongosh -u admin -p password --authenticationDatabase admin

# View merged data
use moviedb
db.batch_views.find().limit(5)  # Historical (>48h)
db.speed_views.find().limit(5)  # Recent (≤48h)
```

## ✅ Implementation Status

### Phase 1: Setup & Planning - ✅ COMPLETED
- [x] Lambda Architecture design
- [x] Directory structure (`layers/batch_layer`, `layers/speed_layer`, `layers/serving_layer`)
- [x] Documentation (12+ markdown files)
- [x] Template code for all layers

### Phase 2: Batch Layer - ⚠️ NEEDS VERIFICATION
- [x] Deploy HDFS cluster (3 datanodes + namenode) - deployment completed
- [ ] Implement TMDB → HDFS ingestion - needs verification with API key
- [ ] Create Airflow DAGs (batch orchestration) - needs verification
- [ ] Bronze → Silver transformations - needs verification
- [ ] Silver → Gold aggregations - needs verification
- [ ] Sentiment analysis (batch processing) - needs verification
- [ ] Export batch views to MongoDB - needs verification

### Phase 3: Speed Layer - ✅ COMPLETED
- [x] Deploy Kafka cluster (3 brokers + Zookeeper)
- [x] Implement Reddit JSON scraper (no auth required)
- [x] Kafka producers publishing to reddit.posts, reddit.comments (6,932 total messages)
- [x] Deploy Cassandra cluster (1 node, 48h TTL schema)
- [x] **TMDB validation integrated** (multi-category cache, 4 categories)
- [x] **Fuzzy matching** (0.8 similarity threshold, year extraction, article normalization)
- [x] Spark Structured Streaming jobs (fully operational with 30s watermark)
- [x] Real-time sentiment analysis (VADER on 5-minute windows)
- [x] Write to Cassandra speed views (post + comment metrics)
- [x] Viral metrics calculation (upvote/comment/award velocity)
- [x] MongoDB sync connector deployed (active sync, TMDB-validated titles only)
- [x] **Data quality validation** (old bad data cleaned, only TMDB-validated titles remain)
- [x] **Schema alignment** (Cassandra ↔ Spark ↔ MongoDB all synchronized)

### Phase 4: Serving Layer - ✅ OPERATIONAL (Speed Layer), ⚠️ BATCH LAYER PENDING
- [x] Deploy MongoDB (batch_views + speed_views collections)
- [x] **Speed views fully operational** (5-min sync active)
- [x] Implement FastAPI REST API (running on port 8000)
- [x] **Cassandra → MongoDB sync working** (reddit-cassandra-sync running, real-time data flow)
- [x] **Data quality ensured** (only TMDB-validated movie titles)
- [ ] View merger (batch + speed merge logic) - awaiting batch layer completion
- [ ] Redis caching layer - needs verification
- [ ] Apache Superset dashboards - needs verification
- [ ] Grafana monitoring - needs verification
- [ ] API authentication & rate limiting - needs verification

### Phase 5: System Refinement
- [x] **Speed layer data quality validated** (TMDB-validated titles only)
- [x] **Schema synchronization** (Cassandra, Spark, MongoDB aligned)
- [x] **Bad data cleanup** (invalid documents removed, clean dataset maintained)
- [ ] Requirements checklist finalization
- [ ] Batch layer data quality checks & validation
- [ ] Performance optimizations
- [ ] End-to-end integration testing

## 📚 Documentation

### Architecture Documentation
- **[Batch Layer Guide](layers/batch_layer/README.md)**: Complete guide to HDFS storage, Spark batch jobs, Airflow DAGs, and Bronze → Silver → Gold transformations
- **[Speed Layer Guide](layers/speed_layer/README.md)**: Kafka streaming, Spark Structured Streaming, Cassandra setup, and real-time processing
- **[Serving Layer Guide](layers/serving_layer/README.md)**: FastAPI endpoints, MongoDB schema, query merger logic, and caching strategies
- **[Kubernetes Deployment](kubernetes/README.md)**: Production deployment guide with monitoring, scaling, and troubleshooting

### Presentation Materials
- **[First Presentation](First%20Presentation%2028accfcd991180e7889cd9dc5e83ca02.md)**: Project overview, business problems, and architecture explanation

### Technical Specifications
- **Configuration Files**: See `config/` directory for all service configurations
- **API Documentation**: Interactive docs at `/docs` endpoint when API is running
- **Architecture Diagrams**: See `docs/Movie Data Analysis Pipeline.drawio`

## 🚢 Deployment (Dummy, didn't work yet)

### Docker Compose (Development)

Best for local development and testing:

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Clean up (including volumes)
docker-compose down -v
```

### Kubernetes (Production) (Dummy, didn't work yet)

Production-ready deployment with high availability:

```bash
# Navigate to kubernetes directory
cd kubernetes

# Deploy complete stack
./deploy.sh deploy

# Check deployment status
kubectl get pods -n movie-analytics

# Access services via port forwarding
kubectl port-forward -n movie-analytics service/movie-api-service 8000:8000
kubectl port-forward -n movie-analytics service/grafana-service 3000:3000
kubectl port-forward -n movie-analytics service/superset-service 8088:8088

# Clean up
./deploy.sh clean
```

See [kubernetes/README.md](kubernetes/README.md) for detailed deployment instructions.

## 📊 Monitoring & Operations

### Key Performance Indicators

| Metric | Target | Description |
|--------|--------|-------------|
| **Batch Job Success Rate** | >99% | Percentage of successful Airflow DAG runs |
| **Batch Processing Time** | <2 hours | Time to complete Bronze → Silver → Gold |
| **Speed Layer Latency** | <5 minutes | End-to-end processing time for streaming |
| **API Response Time (p95)** | <100ms | 95th percentile API latency |
| **Data Quality Score** | >95% | Percentage of rows passing validation |
| **Kafka Consumer Lag** | <1000 msgs | Number of unprocessed Kafka messages |
| **Cache Hit Rate** | >70% | Percentage of requests served from cache |

### Monitoring Dashboards

**Grafana Dashboards** (http://localhost:3000):
1. **System Health**: API latency, MongoDB performance, Redis cache hit rates
2. **Data Freshness**: Batch layer updates, speed layer lag, view staleness
3. **Infrastructure**: Kafka throughput, Cassandra write rates, Spark job duration

**Apache Superset Dashboards** (http://localhost:8088):
1. **Executive Overview**: Total movies, average ratings, revenue trends
2. **Real-time Analytics**: Trending movies, recent sentiment changes
3. **Historical Analysis**: Year-over-year comparisons, genre performance

### Alerting Rules

- **Critical Alerts** (PagerDuty):
  - Batch job failures
  - Streaming job crashes
  - MongoDB/Cassandra node down
  - API p99 latency >500ms

- **Warning Alerts** (Slack):
  - Kafka consumer lag >5000 messages
  - Data quality score <90%
  - Cache hit rate <50%
  - Speed layer lag >10 minutes

### Log Aggregation (Dummy, didn't work yet)

All logs are centralized and searchable:

```bash
# Docker Compose logs
docker-compose logs -f [service_name]

# Kubernetes logs
kubectl logs -n movie-analytics -l app=[app_name] -f

# View specific service logs
kubectl logs -n movie-analytics deployment/movie-api --tail=100
```

## 🧪 Testing (Dummy, didn't work yet)

### Run Tests

```bash
# Run all tests
pytest tests/

# Run specific layer tests
pytest layers/batch_layer/tests/
pytest layers/speed_layer/tests/
pytest layers/serving_layer/tests/

# Run with coverage
pytest --cov=layers --cov-report=html

# Run integration tests only
pytest -m integration
```

### Test Categories

- **Unit Tests**: Individual component functionality
- **Integration Tests**: End-to-end pipeline flows
- **Performance Tests**: Latency and throughput benchmarks
- **Data Quality Tests**: Schema validation and completeness
