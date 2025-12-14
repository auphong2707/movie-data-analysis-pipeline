# Serving Layer Revision Plan - Comprehensive Implementation Guide

**Project:** Movie Social Engagement Analytics Pipeline  
**Document Version:** 2.0  
**Date:** December 14, 2025  
**Status:** Ready for Implementation  
**Purpose:** Complete architectural revision to align with Reddit + TMDB dual-source business goals

---

## 📋 Executive Summary

This document provides a comprehensive plan to **enhance** the existing Serving Layer to properly support the three business goals:

1. **PR Crisis Detection & Sentiment Monitoring**
2. **Viral Content Identification for Marketing Amplification**
3. **Content Recommendation Optimization**

### 🎯 Key Finding: Infrastructure is Built, Business Logic is Missing

**Good News:** ~70% of the serving layer infrastructure is already operational:
- ✅ FastAPI application with all routes configured
- ✅ MongoDB connection with batch + speed views (5,821 + live data)
- ✅ Redis caching with TTL strategies
- ✅ ViewMerger base class with 48-hour cutoff
- ✅ Basic endpoints working: `/movies/{id}`, `/trending/movies`, `/recommendations`

**The Gap:** The business logic for the three goals needs implementation:
- ⚠️ Crisis detection: Need genre baseline comparison + 3-sigma threshold
- ⚠️ Viral scoring: Need viral coefficient calculation + threshold lookup
- ⚠️ Dual-success recommendations: Need Reddit buzz × TMDB quality scoring

**Effort Required:** ~2 weeks to implement business logic + 1 week for monitoring dashboards

### 📊 What This Plan Delivers

1. **Detailed code implementations** for crisis detection, viral scoring, and dual-success algorithms
2. **Schema alignment** between plan and actual MongoDB structure
3. **Prioritized roadmap** focusing on business value first
4. **Complete monitoring stack** with 5 Grafana dashboards
5. **7 containers specification** with purpose and metrics

---

## 🎯 Business Goals Alignment

### Goal #1: PR Crisis Detection & Sentiment Monitoring
**Business Need:** Detect when current Reddit discussion sentiment drops significantly below historical TMDB baselines

**Current Data Available:**
- **Speed Layer (MongoDB `speed_views`)**: Reddit sentiment in last 48 hours
- **Batch Layer (MongoDB `batch_views`)**: Genre/franchise sentiment baselines from TMDB

**Serving Layer Must Deliver:**
- Compare current Reddit sentiment (+0.3) vs genre baseline (+0.65)
- Calculate statistical deviation (±0.15 threshold)
- Alert if drop exceeds normal variance
- Show sentiment velocity (6-hour drops)

---

### Goal #2: Viral Content Identification
**Business Need:** Identify breakout content by comparing real-time Reddit engagement velocity against historical viral thresholds

**Current Data Available:**
- **Speed Layer**: Reddit upvote velocity (500 upvotes/hour), comment acceleration, award velocity
- **Batch Layer**: Genre-specific viral thresholds (300+ upvotes/hour = 99th percentile)

**Serving Layer Must Deliver:**
- Real-time viral score (current velocity / baseline threshold)
- Cross-subreddit spread tracking
- Viral coefficient calculation (10x baseline = viral)
- 24-48h viral window detection

---

### Goal #3: Content Recommendation Optimization
**Business Need:** Surface trending content by combining fresh Reddit buzz with historical TMDB quality benchmarks

**Current Data Available:**
- **Speed Layer**: Current Reddit discussion volume (2,000 comments/day), sentiment (+0.9)
- **Batch Layer**: Historical TMDB ratings (vote_average: 8.3, vote_count: 3,566), genre benchmarks

**Serving Layer Must Deliver:**
- Dual-success metric ranking (Reddit buzz × TMDB quality)
- Genre-aware recommendations
- Trending vs quality balance
- Real-time re-ranking

---

## 🏗️ Architecture Overview

### Current State Assessment (December 14, 2025)

✅ **FULLY OPERATIONAL:**
- MongoDB with `batch_views` collection (5,821 documents: sentiment baselines, viral thresholds, movie intelligence)
- MongoDB with `speed_views` collection (Reddit data synced every 5 min from Cassandra)
- FastAPI framework running on port 8000 with lifecycle management
- Redis cache on port 6379 with connection pooling
- Basic ViewMerger class with 48-hour cutoff logic
- Working endpoints: `/movies/{id}`, `/movies/{id}/sentiment`, `/trending/movies`, `/recommendations`, `/predictions`
- Cache manager with TTL and LRU eviction
- MongoDB query builders
- Recommendation engine (content-based filtering)
- **5 Grafana dashboards already created** (JSON files exist):
  - `system-health-dashboard.json` (17KB)
  - `data-freshness-dashboard.json` (17KB)
  - `genre-analytics-dashboard.json` (21KB)
  - `movie-analytics-overview.json` (22KB)
  - `trending-movies.json` (12KB)
- **Grafana docker-compose setup** with:
  - Container definition with Infinity datasource plugin
  - Auto-provisioning configuration
  - Dashboard auto-loading from JSON files
  - Start/stop scripts (`start-grafana.sh`, `stop-grafana.sh`)
- **API datasource configured** for Grafana (connects to FastAPI at port 8000)

⚠️ **NEEDS IMPLEMENTATION/MODIFICATION:**

**CRITICAL - Business Logic Gaps:**
1. **Crisis Detection Logic** - The `merge_sentiment_data()` needs:
   - Genre baseline comparison (batch layer)
   - 3-sigma threshold detection
   - Sentiment velocity calculation (6-hour drops)
   - Franchise baseline comparison

2. **Viral Scoring Logic** - Need new `merge_viral_data()` function:
   - Compare Reddit velocity vs viral thresholds
   - Calculate viral coefficient
   - Cross-subreddit spread tracking
   - Percentile ranking

3. **Dual-Success Recommendations** - Enhance existing recommendation engine:
   - Combine Reddit buzz score (speed layer) with TMDB quality (batch layer)
   - Weighted scoring (60% Reddit, 40% TMDB)
   - Genre-aware filtering

**MEDIUM - Schema Alignment:**
- Update merger logic to use actual `metrics` object (not `post_metrics`/`comment_metrics`)
- Use `data_type: "reddit_post"` instead of `"reddit_metrics"`
- Handle both `reddit_post` and `reddit_comment` data types

**LOW - Monitoring & Visualization (Mostly Done):**
- ✅ Grafana container configuration exists (`docker-compose.grafana.yml`)
- ✅ 5 Grafana dashboards created (JSON files ready)
- ✅ Infinity datasource plugin configured for API queries
- ✅ Start/stop scripts available
- ⚠️ Need to verify dashboards work with actual API endpoints
- ⚠️ Need to add Prometheus + exporters (MongoDB, Redis exporters)
- ⚠️ Need to add `/metrics` endpoint to FastAPI
- ⚠️ Configure alerting rules

---

## 🗄️ Data Layer Architecture

### MongoDB Collections Schema

#### Collection 1: `batch_views` (Historical - >48h old)

**Purpose:** Store TMDB-derived historical baselines and movie intelligence

**Document Types (discriminated by `view_type` field):**

**1. Sentiment Baseline Documents:**
```javascript
{
  "_id": ObjectId("..."),
  "view_type": "sentiment_baseline",
  "type": "sentiment_baseline",
  
  // Dimension (one of: genre, franchise, or year)
  "genre": "Action",           // Genre-level baseline
  "franchise": null,            // or franchise name
  "year": null,                // or year value
  
  // Metrics
  "avg_sentiment": 0.0021,     // Genre average sentiment
  "sentiment_stddev": 0.0162,  // Standard deviation (±0.15 threshold)
  "movie_count": 393,          // Movies in this category
  "review_count": 33,          // Total reviews analyzed
  
  // Temporal
  "batch_run_timestamp": "2025-12-05T17:27:13Z",
  "updated_at": ISODate("2025-12-05T17:27:00Z")
}
```

**Purpose & Intuition:** Provides statistical baseline to answer "Is current Reddit sentiment normal or exceptional?" For example, if Action genre averages +0.65 with ±0.15 variance, current Reddit sentiment of +0.3 signals a crisis.

**2. Viral Threshold Documents:**
```javascript
{
  "_id": ObjectId("..."),
  "view_type": "viral_threshold",
  "type": "viral_threshold",
  
  // Dimensions
  "genre": "Action",
  "budget_tier": "blockbuster",  // indie/mid/blockbuster
  "season": "summer",            // spring/summer/fall/winter
  
  // Thresholds
  "viral_threshold": 29058,      // 99th percentile vote velocity
  "avg_popularity": 6.97,        // Normal popularity for this segment
  "movie_count": 3,              // Historical sample size
  
  "batch_run_timestamp": "2025-12-05T17:27:13Z"
}
```

**Purpose & Intuition:** Provides context-aware viral thresholds. A summer blockbuster needs 29K votes/hour to be considered viral, while indie films have lower thresholds. Answers "Is this engagement truly exceptional for its category?"

**3. Movie Intelligence Documents:**
```javascript
{
  "_id": ObjectId("..."),
  "view_type": "movie_intelligence",
  "type": "movie_intelligence",
  
  // Identity
  "movie_id": 914,
  "title": "The Great Dictator",
  "director": "Charlie Chaplin",
  "genre": "Comedy",
  
  // Metadata
  "budget": 2000000,
  "budget_tier": "indie",
  "runtime": 125,
  "release_date": "1940-10-15",
  "release_year": 1940,
  "release_month": "October",
  
  // TMDB Metrics
  "vote_average": 8.3,
  "vote_count": 3566,
  "popularity": 2.5774,
  "avg_sentiment": 0.0,
  "review_count": 0,
  
  "batch_run_timestamp": "2025-12-05T17:27:13Z"
}
```

**Purpose & Intuition:** Complete movie profile from TMDB for enrichment and quality benchmarking. Used to enrich Reddit discussions with official metadata and historical performance data.

**Indexes for Performance:**
```javascript
// Primary lookups
db.batch_views.createIndex({ "view_type": 1, "genre": 1 })
db.batch_views.createIndex({ "view_type": 1, "movie_id": 1 })
db.batch_views.createIndex({ "view_type": 1, "franchise": 1 })

// Viral threshold lookups
db.batch_views.createIndex({ 
  "view_type": 1, 
  "genre": 1, 
  "budget_tier": 1, 
  "season": 1 
})

// Temporal queries
db.batch_views.createIndex({ "batch_run_timestamp": -1 })
```

---

#### Collection 2: `speed_views` (Real-time - ≤48h old)

**Purpose:** Store Reddit real-time engagement metrics synced from Cassandra every 5 minutes

**Document Schema (ACTUAL - from running system):**
```javascript
{
  "_id": ObjectId("..."),
  "movie_title": "Fight Club",     // TMDB-validated title
  "data_type": "reddit_post",      // or "reddit_comment"
  "hour": ISODate("2025-12-14T18:00:00Z"),  // Hour bucket
  "window_start": ISODate("2025-12-14T18:05:00Z"),  // 5-minute window
  "data_source": "reddit",
  
  // All metrics in single "metrics" object
  "metrics": {
    "post_count": 15,              // Posts in this window
    "total_upvotes": 4523,         // Total upvotes
    "avg_upvote_ratio": 0.82,      // Upvote ratio
    "total_comments": 342,         // Total comments
    "total_awards": 8,             // Reddit awards (gold, platinum)
    "avg_sentiment": 0.75,         // VADER sentiment (-1 to +1)
    "max_upvotes": 501,            // Highest upvote count
    "upvote_velocity": 450.5,      // Upvotes per hour
    "comment_velocity": 68.4,      // Comments per hour
    "award_velocity": 1.6,         // Awards per hour
    "viral_score": 0.85            // Calculated viral score
  },
  
  // Metadata
  "processed_at": ISODate("2025-12-14T15:55:38Z"),
  "synced_at": ISODate("2025-12-14T16:09:58Z"),
  "ttl_expires_at": ISODate("2025-12-14T18:00:00Z")  // 48h TTL
}
```

**Note:** Speed views use a single `metrics` object, not separate nested objects. Data types are `reddit_post` and `reddit_comment`.

**Purpose & Intuition:** Captures "what's happening NOW" on Reddit. The 5-minute granularity allows tracking rapid viral surges or sentiment drops. TTL ensures we only keep last 48 hours (Lambda Architecture speed layer characteristic).

**Indexes for Performance:**
```javascript
// Primary lookups
db.speed_views.createIndex({ "movie_title": 1, "hour": -1 })
db.speed_views.createIndex({ "data_type": 1, "hour": -1 })

// TTL cleanup
db.speed_views.createIndex({ "ttl_expires_at": 1 }, { expireAfterSeconds: 0 })

// Velocity queries
db.speed_views.createIndex({ "post_metrics.upvote_velocity": -1, "hour": -1 })
```

---

## 🔀 Query Merge Strategy

### 48-Hour Cutoff Implementation

**Core Logic:**
```python
def get_cutoff_time():
    """
    Calculate 48-hour cutoff point
    
    Returns: datetime object 48 hours ago
    """
    return datetime.utcnow() - timedelta(hours=48)

def merge_sentiment_data(movie_title: str):
    """
    Merge sentiment data with crisis detection logic
    
    Business Goal #1: PR Crisis Detection
    """
    cutoff = get_cutoff_time()
    
    # Step 1: Get current Reddit sentiment (speed layer)
    speed_query = {
        "movie_title": movie_title,
        "data_type": "reddit_metrics",
        "hour": {"$gte": cutoff}
    }
    reddit_data = list(speed_views.find(speed_query).sort("hour", -1))
    
    if not reddit_data:
        return {"error": "No recent Reddit data"}
    
    # Calculate current metrics
    current_sentiment = np.mean([d["post_metrics"]["avg_sentiment"] 
                                 for d in reddit_data])
    sentiment_velocity = calculate_velocity(reddit_data, window_hours=6)
    
    # Step 2: Get movie metadata for genre lookup
    movie_intel = batch_views.find_one({
        "view_type": "movie_intelligence",
        "title": movie_title
    })
    
    if not movie_intel:
        return {"error": "Movie not found in TMDB database"}
    
    genre = movie_intel["genre"]
    
    # Step 3: Get historical baseline (batch layer)
    baseline = batch_views.find_one({
        "view_type": "sentiment_baseline",
        "genre": genre
    })
    
    baseline_sentiment = baseline["avg_sentiment"]
    normal_variance = baseline["sentiment_stddev"]
    
    # Step 4: Crisis detection logic
    deviation = current_sentiment - baseline_sentiment
    is_crisis = abs(deviation) > (3 * normal_variance)  # 3-sigma rule
    
    return {
        "movie_title": movie_title,
        "genre": genre,
        "current_sentiment": {
            "score": current_sentiment,
            "label": get_sentiment_label(current_sentiment),
            "velocity": sentiment_velocity,
            "data_source": "reddit_last_48h"
        },
        "historical_baseline": {
            "score": baseline_sentiment,
            "variance": normal_variance,
            "data_source": "tmdb_historical"
        },
        "analysis": {
            "deviation": deviation,
            "is_crisis": is_crisis,
            "severity": "critical" if abs(deviation) > 4*normal_variance else "warning",
            "recommendation": "Immediate PR response required" if is_crisis else "Monitor closely"
        }
    }
```

**Intuition:** Speed layer tells us "what's happening now" (Reddit sentiment +0.3), batch layer tells us "what's normal" (genre baseline +0.65). Merging them answers "should we take action?" (yes, -0.35 deviation is a crisis).

---

### Viral Detection Merge Logic

```python
def merge_viral_data(movie_title: str, limit: int = 20):
    """
    Calculate viral score with threshold comparison
    
    Business Goal #2: Viral Content Identification
    """
    cutoff = get_cutoff_time()
    
    # Step 1: Get current Reddit velocity (speed layer)
    recent_data = speed_views.find({
        "movie_title": movie_title,
        "hour": {"$gte": cutoff}
    }).sort("hour", -1).limit(12)  # Last hour (12 × 5-min windows)
    
    recent_list = list(recent_data)
    if not recent_list:
        return {"error": "No recent activity"}
    
    # Calculate current velocities
    current_upvote_velocity = np.mean([
        d["post_metrics"]["upvote_velocity"] for d in recent_list
    ])
    current_comment_velocity = np.mean([
        d["post_metrics"]["comment_velocity"] for d in recent_list
    ])
    current_award_velocity = np.mean([
        d["post_metrics"]["award_velocity"] for d in recent_list
    ])
    
    # Cross-subreddit spread
    subreddits = set()
    for doc in recent_list:
        for sub in doc.get("subreddit_distribution", []):
            subreddits.add(sub["subreddit"])
    cross_sub_spread = len(subreddits)
    
    # Step 2: Get movie metadata
    movie_intel = batch_views.find_one({
        "view_type": "movie_intelligence",
        "title": movie_title
    })
    
    genre = movie_intel["genre"]
    budget_tier = movie_intel["budget_tier"]
    season = get_season(movie_intel["release_month"])
    
    # Step 3: Get viral threshold (batch layer)
    threshold_doc = batch_views.find_one({
        "view_type": "viral_threshold",
        "genre": genre,
        "budget_tier": budget_tier,
        "season": season
    })
    
    if not threshold_doc:
        # Fallback to genre-only threshold
        threshold_doc = batch_views.find_one({
            "view_type": "viral_threshold",
            "genre": genre
        })
    
    viral_threshold = threshold_doc.get("viral_threshold", 300)
    
    # Step 4: Calculate viral score
    viral_coefficient = current_upvote_velocity / viral_threshold
    is_viral = viral_coefficient >= 1.0
    
    return {
        "movie_title": movie_title,
        "current_metrics": {
            "upvote_velocity": current_upvote_velocity,
            "comment_velocity": current_comment_velocity,
            "award_velocity": current_award_velocity,
            "cross_subreddit_spread": cross_sub_spread,
            "data_source": "reddit_last_hour"
        },
        "viral_threshold": {
            "threshold": viral_threshold,
            "genre": genre,
            "budget_tier": budget_tier,
            "season": season,
            "data_source": "tmdb_historical"
        },
        "viral_analysis": {
            "viral_coefficient": viral_coefficient,
            "is_viral": is_viral,
            "status": "VIRAL" if viral_coefficient >= 1.5 else 
                     "TRENDING" if viral_coefficient >= 1.0 else 
                     "NORMAL",
            "percentile": calculate_percentile(current_upvote_velocity, genre),
            "recommendation": f"Amplify marketing - {int(viral_coefficient*100)}% above threshold" if is_viral else "Monitor for viral potential"
        }
    }
```

**Intuition:** Speed layer shows current velocity (500 upvotes/hour), batch layer provides the bar to beat (300 = viral). Viral coefficient of 1.67x means "this is 67% above the viral threshold - activate marketing amplification now."

---

### Recommendation Merge Logic

```python
def merge_recommendation_data(genre: str = None, limit: int = 20):
    """
    Generate recommendations combining Reddit buzz + TMDB quality
    
    Business Goal #3: Content Recommendation Optimization
    """
    cutoff = get_cutoff_time()
    
    # Step 1: Get trending on Reddit (speed layer)
    speed_query = {
        "data_type": "reddit_metrics",
        "hour": {"$gte": cutoff - timedelta(hours=6)}  # Last 6 hours
    }
    
    if genre:
        speed_query["genre"] = genre  # If we store genre in speed_views
    
    reddit_trending = list(speed_views.aggregate([
        {"$match": speed_query},
        {"$group": {
            "_id": "$movie_title",
            "avg_sentiment": {"$avg": "$post_metrics.avg_sentiment"},
            "total_engagement": {
                "$sum": {
                    "$add": [
                        "$post_metrics.total_score",
                        {"$multiply": ["$post_metrics.total_comments", 2]},
                        {"$multiply": ["$post_metrics.total_awards", 10]}
                    ]
                }
            },
            "reddit_buzz_score": {"$avg": "$post_metrics.upvote_velocity"}
        }},
        {"$sort": {"total_engagement": -1}},
        {"$limit": limit * 2}  # Get more candidates for filtering
    ]))
    
    # Step 2: Enrich with TMDB quality scores (batch layer)
    recommendations = []
    
    for reddit_movie in reddit_trending:
        movie_title = reddit_movie["_id"]
        
        # Get TMDB intelligence
        tmdb_data = batch_views.find_one({
            "view_type": "movie_intelligence",
            "title": movie_title
        })
        
        if not tmdb_data:
            continue  # Skip if no TMDB data
        
        # Get genre baseline for context
        baseline = batch_views.find_one({
            "view_type": "sentiment_baseline",
            "genre": tmdb_data["genre"]
        })
        
        # Calculate dual-success score
        reddit_score = normalize_score(reddit_movie["reddit_buzz_score"], 0, 1000)
        tmdb_quality = tmdb_data["vote_average"] / 10.0  # Normalize to 0-1
        
        # Weighted combination (60% Reddit buzz, 40% TMDB quality)
        recommendation_score = (0.6 * reddit_score) + (0.4 * tmdb_quality)
        
        # Check if exceeding genre baseline
        sentiment_vs_baseline = (reddit_movie["avg_sentiment"] - 
                                baseline["avg_sentiment"])
        
        recommendations.append({
            "movie_title": movie_title,
            "recommendation_score": recommendation_score,
            "reddit_metrics": {
                "sentiment": reddit_movie["avg_sentiment"],
                "engagement": reddit_movie["total_engagement"],
                "buzz_score": reddit_movie["reddit_buzz_score"],
                "vs_genre_baseline": sentiment_vs_baseline
            },
            "tmdb_metrics": {
                "vote_average": tmdb_data["vote_average"],
                "vote_count": tmdb_data["vote_count"],
                "popularity": tmdb_data["popularity"],
                "genre": tmdb_data["genre"]
            },
            "analysis": {
                "status": "DUAL_SUCCESS" if recommendation_score > 0.7 and sentiment_vs_baseline > 0 else "TRENDING",
                "reason": f"High Reddit buzz ({reddit_score:.2f}) + Strong TMDB rating ({tmdb_quality:.2f})"
            }
        })
    
    # Sort by recommendation score
    recommendations.sort(key=lambda x: x["recommendation_score"], reverse=True)
    
    return recommendations[:limit]
```

**Intuition:** Combines "hot right now on Reddit" (buzz score) with "historically good according to TMDB" (quality rating). A movie like Barbie with both high Reddit engagement AND high TMDB rating gets prioritized over movies that only have one or the other.

---

## 🚀 Container Architecture

### Container 1: MongoDB (Existing - `serving-mongodb`)

**Purpose:** Unified data storage for batch and speed views

**Configuration:**
- **Image:** `mongo:7.0`
- **Port:** `27017`
- **Volumes:** `serving-mongodb-data:/data/db`
- **Resources:** 
  - CPU: 2 cores
  - Memory: 4GB
  - Storage: 50GB SSD

**Collections:**
- `batch_views` - Historical TMDB data (5,821 documents)
- `speed_views` - Recent Reddit data (48h TTL)

**Monitoring Metrics:**
- Query latency (target: p95 < 50ms)
- Collection size growth
- Index hit ratio (target: > 95%)
- Connection pool usage

**Why This Container:** MongoDB provides flexible schema for our varied document types (sentiment baselines, viral thresholds, movie intelligence) while maintaining high query performance through proper indexing.

---

### Container 2: Redis Cache (Existing - `serving-redis`)

**Purpose:** High-speed cache for frequently accessed queries

**Configuration:**
- **Image:** `redis:7-alpine`
- **Port:** `6379`
- **Memory:** `256MB` with LRU eviction
- **Volumes:** `serving-redis-data:/data`

**Cache Strategy:**

| Data Type | TTL | Eviction Policy |
|-----------|-----|-----------------|
| Trending movies | 5 minutes | LRU |
| Movie details | 30 minutes | LRU |
| Sentiment analysis | 10 minutes | LRU |
| Viral scores | 5 minutes | LRU |
| Search results | 15 minutes | LRU |
| Genre analytics | 60 minutes | LRU |

**Cache Keys Pattern:**
```
trending:{genre}:{timestamp}
movie:{movie_id}:details
movie:{movie_title}:sentiment
movie:{movie_title}:viral
search:{query_hash}
analytics:genre:{genre}:{year}
```

**Monitoring Metrics:**
- Hit rate (target: > 70%)
- Memory usage (alert at 90%)
- Eviction rate
- Average latency (target: < 2ms)

**Why This Container:** Redis provides sub-millisecond response times for cached queries, dramatically reducing MongoDB load and improving API response times from ~50ms to ~2ms for cached requests.

---

### Container 3: FastAPI Application (Existing - `serving-api`)

**Purpose:** REST API exposing merged batch + speed data

**Configuration:**
- **Build Context:** `./layers/serving_layer`
- **Port:** `8000`
- **Workers:** `4` (Uvicorn workers)
- **Resources:**
  - CPU: 2 cores
  - Memory: 2GB

**Environment Variables:**
```bash
# MongoDB
MONGODB_URI=mongodb://admin:password@mongodb:27017
MONGODB_DATABASE=moviedb

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# API
API_HOST=0.0.0.0
API_PORT=8000
LOG_LEVEL=INFO

# Business Logic
CRISIS_THRESHOLD_SIGMA=3.0       # 3-sigma rule for crisis detection
VIRAL_COEFFICIENT_THRESHOLD=1.0   # 1.0 = at viral threshold
RECOMMENDATION_WEIGHT_REDDIT=0.6  # 60% Reddit, 40% TMDB
```

**Monitoring Metrics:**
- Request latency (p50, p95, p99)
- Throughput (requests/second)
- Error rate (target: < 0.1%)
- Active connections
- Worker CPU/memory usage

**Why This Container:** FastAPI provides async request handling for high concurrency, automatic API documentation (Swagger UI), and excellent performance characteristics (1500 req/s per instance).

---

### Container 4: Grafana (✅ CONFIGURED - `serving-grafana`)

**Purpose:** Real-time visualization dashboards for monitoring and business analytics

**Status:** Container definition and dashboards exist, needs to be started

**Configuration (Already Set Up):**
- **Image:** `grafana/grafana:latest`
- **Port:** `3000`
- **Admin Credentials:** admin/admin
- **Volumes:** 
  - `./layers/serving_layer/visualization/grafana/provisioning:/etc/grafana/provisioning`
  - `./layers/serving_layer/visualization/grafana/dashboards:/etc/grafana/provisioning/dashboards`
  - `grafana-data:/var/lib/grafana`
- **Plugin:** `yesoreyeram-infinity-datasource` (pre-installed)

**Data Sources (Configured):**
1. **Movie Analytics API** (Infinity datasource) - Points to http://host.docker.internal:8000
2. ⚠️ **Prometheus** (needs to be added) - For metrics collection
3. ⚠️ **Redis metrics** (needs to be added) - Via Redis exporter

**Dashboards Available (5 JSON files - 89KB total):**
1. **system-health-dashboard.json** (17KB) - API health, service status
2. **data-freshness-dashboard.json** (17KB) - Batch/speed layer sync status
3. **genre-analytics-dashboard.json** (21KB) - Genre performance metrics
4. **movie-analytics-overview.json** (22KB) - Overall movie analytics
5. **trending-movies.json** (12KB) - Real-time trending movies

**How to Start:**
```bash
cd layers/serving_layer/visualization/grafana
./start-grafana.sh
# Access at http://localhost:3000 (admin/admin)
```

**Dashboard 1: Business Metrics Dashboard**
**Purpose:** Executive view of business goal achievements

**Panels:**
1. **PR Crisis Alerts** (Goal #1)
   - Current movies with sentiment < baseline - 3σ
   - Sentiment deviation gauge
   - Recent crisis events timeline
   - **Intuition:** Quick view of movies needing immediate PR response

2. **Viral Content Tracker** (Goal #2)
   - Top 10 viral movies (coefficient > 1.0)
   - Viral velocity trends (upvotes/hour)
   - Cross-subreddit spread heatmap
   - **Intuition:** Identify marketing amplification opportunities

3. **Recommendation Performance** (Goal #3)
   - Top 20 dual-success movies (Reddit buzz + TMDB quality)
   - Engagement vs quality scatter plot
   - Genre distribution of recommendations
   - **Intuition:** Monitor recommendation algorithm effectiveness

**MongoDB Queries for Dashboard:**
```javascript
// PR Crisis Detection
db.speed_views.aggregate([
  {$match: {hour: {$gte: ISODate("2h ago")}}},
  {$group: {
    _id: "$movie_title",
    current_sentiment: {$avg: "$post_metrics.avg_sentiment"}
  }},
  {$lookup: {
    from: "batch_views",
    let: {title: "$_id"},
    pipeline: [
      {$match: {view_type: "movie_intelligence"}},
      {$match: {$expr: {$eq: ["$title", "$$title"]}}}
    ],
    as: "movie_info"
  }},
  // ... join with sentiment baselines and calculate deviation
])
```

---

**Dashboard 2: System Health Dashboard**
**Purpose:** Monitor serving layer infrastructure health

**Panels:**
1. **API Performance**
   - Request latency histogram (p50, p95, p99)
   - Throughput (requests/second)
   - Error rate percentage
   - Active connections

2. **MongoDB Metrics**
   - Query latency by collection
   - Collection sizes (batch_views, speed_views)
   - Index usage statistics
   - Connection pool status

3. **Redis Cache Performance**
   - Hit rate percentage (target: > 70%)
   - Memory usage
   - Eviction rate
   - Commands per second

4. **Data Freshness**
   - Speed layer last sync time
   - Batch layer last update time
   - Data lag warnings (> 10 minutes)

**Prometheus Metrics for Dashboard:**
```
# API metrics
api_request_duration_seconds{quantile="0.95"}
api_requests_total
api_errors_total

# MongoDB metrics
mongodb_query_duration_seconds
mongodb_connections_current
mongodb_collection_size_bytes

# Redis metrics
redis_cache_hit_rate
redis_memory_usage_bytes
redis_evicted_keys_total
```

---

**Dashboard 3: Data Quality Dashboard**
**Purpose:** Monitor data pipeline quality and completeness

**Panels:**
1. **Speed Layer Data Quality**
   - TMDB validation success rate (should be 100%)
   - Documents synced per hour
   - Invalid movie titles rejected
   - Cassandra → MongoDB sync lag

2. **Batch Layer Data Quality**
   - Sentiment baselines coverage (genres covered)
   - Viral thresholds completeness
   - Movie intelligence count
   - Last batch run status

3. **Merge Quality Metrics**
   - Queries using speed data (%)
   - Queries using batch data (%)
   - Failed merges (missing baseline/metadata)
   - Average merge latency

---

**Dashboard 4: Reddit Engagement Trends**
**Purpose:** Monitor Reddit social engagement patterns

**Panels:**
1. **Real-time Engagement**
   - Top 10 movies by upvote velocity (last hour)
   - Comment velocity trends
   - Award velocity spikes
   - Subreddit activity distribution

2. **Sentiment Trends**
   - Average sentiment over time (24h view)
   - Sentiment volatility
   - Positive/negative ratio
   - Sentiment by subreddit

3. **Viral Detection**
   - Movies crossing viral threshold
   - Cross-subreddit spread tracking
   - Viral window duration (24-48h tracking)

**Time Series Queries:**
```javascript
// Upvote velocity trend (last 24h)
db.speed_views.aggregate([
  {$match: {
    hour: {$gte: ISODate("24h ago")},
    "movie_title": "Fight Club"
  }},
  {$project: {
    hour: 1,
    upvote_velocity: "$post_metrics.upvote_velocity",
    comment_velocity: "$post_metrics.comment_velocity"
  }},
  {$sort: {hour: 1}}
])
```

---

**Dashboard 5: TMDB Baseline Context**
**Purpose:** Understand historical context from TMDB data

**Panels:**
1. **Genre Baselines**
   - Sentiment by genre (bar chart)
   - Genre variance (error bars showing ±σ)
   - Review count by genre

2. **Viral Thresholds Heatmap**
   - Genre × Budget Tier × Season matrix
   - Threshold values color-coded
   - Coverage gaps highlighted

3. **Movie Intelligence Stats**
   - Total movies in database
   - Genre distribution
   - Budget tier distribution
   - Release year timeline

**Why Grafana:** Provides real-time dashboards with automatic refresh, MongoDB/Prometheus integration, and rich visualization options. Essential for monitoring both business metrics (crisis alerts, viral content) and system health.

---

### Container 5: Prometheus (⚠️ NEEDS TO BE ADDED - `serving-prometheus`)

**Purpose:** Metrics collection and alerting for system monitoring

**Status:** Not yet configured - needs to be added to docker-compose

**Configuration (To Be Added):**
- **Image:** `prom/prometheus:latest`
- **Port:** `9090`
- **Volumes:**
  - `./layers/serving_layer/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml`
  - `prometheus-data:/prometheus`

**Required File:** Create `layers/serving_layer/monitoring/prometheus.yml`

**Scrape Configuration:**
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  # FastAPI metrics
  - job_name: 'fastapi'
    static_configs:
      - targets: ['serving-api:8000']
    metrics_path: '/metrics'
  
  # MongoDB metrics (via mongodb_exporter)
  - job_name: 'mongodb'
    static_configs:
      - targets: ['mongodb-exporter:9216']
  
  # Redis metrics (via redis_exporter)
  - job_name: 'redis'
    static_configs:
      - targets: ['redis-exporter:9121']
  
  # Node metrics (system resources)
  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']
```

**Alerting Rules:**
```yaml
groups:
  - name: serving_layer_alerts
    interval: 30s
    rules:
      # Critical: API down
      - alert: APIDown
        expr: up{job="fastapi"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "FastAPI is down"
          
      # Critical: High error rate
      - alert: HighErrorRate
        expr: rate(api_errors_total[5m]) > 0.01
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "API error rate > 1%"
      
      # Warning: High latency
      - alert: HighLatency
        expr: api_request_duration_seconds{quantile="0.95"} > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "API p95 latency > 100ms"
      
      # Warning: Low cache hit rate
      - alert: LowCacheHitRate
        expr: redis_cache_hit_rate < 0.5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Redis cache hit rate < 50%"
      
      # Warning: MongoDB connection issues
      - alert: MongoDBConnectionIssues
        expr: mongodb_connections_current > mongodb_connections_max * 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "MongoDB connection pool at 90%"
      
      # Warning: Data freshness issue
      - alert: StaleSpeedData
        expr: (time() - speed_layer_last_sync_timestamp) > 600
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Speed layer data not synced for 10+ minutes"
```

**Why Prometheus:** Industry-standard metrics collection with powerful query language (PromQL), excellent Grafana integration, and flexible alerting capabilities.

---

### Container 6: MongoDB Exporter (NEW - `mongodb-exporter`)

**Purpose:** Export MongoDB metrics to Prometheus

**Configuration:**
- **Image:** `bitnami/mongodb-exporter:latest`
- **Port:** `9216`
- **Environment:**
  ```bash
  MONGODB_URI=mongodb://admin:password@mongodb:27017
  ```

**Exported Metrics:**
- Connection pool stats
- Query latency by operation type
- Collection sizes and index usage
- Replication lag (if using replica set)

**Why This Container:** Bridges MongoDB metrics to Prometheus for centralized monitoring.

---

### Container 7: Redis Exporter (NEW - `redis-exporter`)

**Purpose:** Export Redis metrics to Prometheus

**Configuration:**
- **Image:** `oliver006/redis_exporter:latest`
- **Port:** `9121`
- **Environment:**
  ```bash
  REDIS_ADDR=redis:6379
  ```

**Exported Metrics:**
- Hit/miss rates
- Memory usage and fragmentation
- Connected clients
- Commands per second
- Key eviction stats

**Why This Container:** Provides visibility into cache performance and helps optimize TTL strategies.

---

## 📡 API Endpoints Implementation

### Endpoint Category 1: Movie Endpoints

#### `GET /api/v1/movies/{movie_title}`
**Purpose:** Get complete movie view with merged batch + speed data

**Business Goal:** Support all three goals with comprehensive movie data

**Response Example:**
```json
{
  "movie_title": "Fight Club",
  "tmdb_metadata": {
    "movie_id": 550,
    "director": "David Fincher",
    "genre": "Drama",
    "budget": 63000000,
    "budget_tier": "mid",
    "runtime": 139,
    "release_date": "1999-10-15",
    "vote_average": 8.4,
    "vote_count": 26234,
    "data_source": "batch_layer"
  },
  "reddit_engagement": {
    "last_48h_metrics": {
      "post_count": 45,
      "total_upvotes": 12450,
      "total_comments": 890,
      "avg_sentiment": 0.82,
      "upvote_velocity": 260.4,
      "comment_velocity": 18.5
    },
    "subreddit_spread": ["movies", "TrueFilm"],
    "data_source": "speed_layer"
  },
  "last_updated": "2025-12-14T10:05:00Z"
}
```

**Implementation:**
```python
@router.get("/movies/{movie_title}")
async def get_movie(movie_title: str, db: MongoDB = Depends(get_db)):
    """Get complete movie view"""
    
    # Check cache first
    cache_key = f"movie:{movie_title}:details"
    cached = await redis.get(cache_key)
    if cached:
        return json.loads(cached)
    
    # Get TMDB metadata (batch layer)
    tmdb_data = db.batch_views.find_one({
        "view_type": "movie_intelligence",
        "title": movie_title
    })
    
    if not tmdb_data:
        raise HTTPException(404, "Movie not found")
    
    # Get Reddit engagement (speed layer)
    cutoff = datetime.utcnow() - timedelta(hours=48)
    reddit_data = list(db.speed_views.find({
        "movie_title": movie_title,
        "hour": {"$gte": cutoff}
    }))
    
    # Merge and format response
    result = {
        "movie_title": movie_title,
        "tmdb_metadata": format_tmdb_data(tmdb_data),
        "reddit_engagement": aggregate_reddit_data(reddit_data) if reddit_data else None
    }
    
    # Cache for 30 minutes
    await redis.setex(cache_key, 1800, json.dumps(result))
    
    return result
```

---

#### `GET /api/v1/movies/{movie_title}/sentiment`
**Purpose:** PR crisis detection endpoint

**Business Goal #1:** PR Crisis Detection & Sentiment Monitoring

**Query Parameters:**
- `window` (optional): Time window (6h, 12h, 24h, 48h)

**Response Example:**
```json
{
  "movie_title": "Dune 2",
  "genre": "Science Fiction",
  "current_sentiment": {
    "score": 0.3,
    "label": "positive",
    "velocity": -0.083,
    "window": "6h",
    "sample_size": 450,
    "data_source": "reddit_last_48h"
  },
  "historical_baseline": {
    "genre_baseline": 0.65,
    "genre": "Science Fiction",
    "variance": 0.15,
    "franchise_baseline": 0.78,
    "franchise": "Dune",
    "data_source": "tmdb_historical"
  },
  "crisis_analysis": {
    "deviation_from_genre": -0.35,
    "deviation_from_franchise": -0.48,
    "sigma_level": 2.33,
    "is_crisis": true,
    "severity": "warning",
    "confidence": 0.92,
    "recommendation": "Monitor closely - approaching crisis threshold"
  },
  "sentiment_breakdown": [
    {
      "timestamp": "2025-12-14T04:00:00Z",
      "sentiment": 0.45,
      "sample_size": 80
    },
    {
      "timestamp": "2025-12-14T05:00:00Z",
      "sentiment": 0.38,
      "sample_size": 95
    },
    {
      "timestamp": "2025-12-14T06:00:00Z",
      "sentiment": 0.30,
      "sample_size": 120
    }
  ]
}
```

**Implementation:** Uses `merge_sentiment_data()` logic defined earlier

---

### Endpoint Category 2: Viral Detection Endpoints

#### `GET /api/v1/viral/trending`
**Purpose:** Identify viral content for marketing amplification

**Business Goal #2:** Viral Content Identification

**Query Parameters:**
- `limit` (optional, default: 20): Number of results
- `genre` (optional): Filter by genre
- `min_coefficient` (optional, default: 1.0): Minimum viral coefficient

**Response Example:**
```json
{
  "trending_movies": [
    {
      "rank": 1,
      "movie_title": "The Creator",
      "genre": "Science Fiction",
      "viral_metrics": {
        "upvote_velocity": 500,
        "comment_velocity": 42,
        "award_velocity": 1.8,
        "cross_subreddit_spread": 5
      },
      "viral_threshold": {
        "threshold": 300,
        "genre": "Science Fiction",
        "budget_tier": "mid",
        "season": "fall"
      },
      "viral_analysis": {
        "viral_coefficient": 1.67,
        "percentile": 99.2,
        "status": "VIRAL",
        "window_duration": "3h",
        "recommendation": "Amplify marketing - 67% above threshold"
      },
      "tmdb_context": {
        "vote_average": 7.8,
        "popularity": 125.4
      }
    },
    {
      "rank": 2,
      "movie_title": "Barbie",
      "viral_coefficient": 1.45,
      "status": "VIRAL"
    }
  ],
  "generated_at": "2025-12-14T10:00:00Z",
  "window": "last_6_hours",
  "total_viral": 2,
  "total_trending": 12
}
```

**Implementation:** Uses `merge_viral_data()` logic

---

#### `GET /api/v1/viral/{movie_title}`
**Purpose:** Get detailed viral analysis for specific movie

**Response includes:**
- Current velocity metrics
- Historical threshold comparison
- Cross-subreddit spread timeline
- Viral window tracking (when it started going viral)
- Predicted viral duration (24-48h window)

---

### Endpoint Category 3: Recommendation Endpoints

#### `GET /api/v1/recommendations`
**Purpose:** Content recommendation optimization

**Business Goal #3:** Surface trending content with dual-success metrics

**Query Parameters:**
- `genre` (optional): Filter by genre
- `limit` (optional, default: 20)
- `min_reddit_score` (optional): Minimum Reddit buzz score
- `min_tmdb_rating` (optional): Minimum TMDB rating

**Response Example:**
```json
{
  "recommendations": [
    {
      "rank": 1,
      "movie_title": "Barbie",
      "recommendation_score": 0.87,
      "reddit_metrics": {
        "sentiment": 0.9,
        "engagement": 25680,
        "buzz_score": 920,
        "vs_genre_baseline": 0.19,
        "data_freshness": "5_minutes"
      },
      "tmdb_metrics": {
        "vote_average": 8.1,
        "vote_count": 4521,
        "popularity": 145.8,
        "genre": "Comedy"
      },
      "analysis": {
        "status": "DUAL_SUCCESS",
        "reason": "High Reddit buzz (0.92) + Strong TMDB rating (0.81)",
        "confidence": 0.95
      }
    }
  ],
  "algorithm": {
    "weights": {
      "reddit_buzz": 0.6,
      "tmdb_quality": 0.4
    },
    "data_sources": {
      "reddit": "last_6_hours",
      "tmdb": "historical"
    }
  },
  "generated_at": "2025-12-14T10:00:00Z"
}
```

**Implementation:** Uses `merge_recommendation_data()` logic

---

### Endpoint Category 4: Analytics Endpoints

#### `GET /api/v1/analytics/genre/{genre}`
**Purpose:** Genre-level analytics with baseline context

**Response Example:**
```json
{
  "genre": "Action",
  "period": "last_30_days",
  "statistics": {
    "total_movies_discussed": 45,
    "avg_reddit_sentiment": 0.68,
    "genre_baseline_sentiment": 0.65,
    "sentiment_trend": "increasing",
    "total_engagement": 156780,
    "avg_tmdb_rating": 7.2
  },
  "top_performers": [
    {
      "movie_title": "Mission Impossible 7",
      "reddit_sentiment": 0.89,
      "tmdb_rating": 8.3,
      "engagement": 28900
    }
  ],
  "viral_movies": [
    {
      "movie_title": "The Creator",
      "viral_coefficient": 1.67
    }
  ],
  "crisis_alerts": [
    {
      "movie_title": "Flash",
      "deviation": -0.42,
      "severity": "critical"
    }
  ]
}
```

---

#### `GET /api/v1/analytics/trends/sentiment`
**Purpose:** Time-series sentiment trend analysis

**Query Parameters:**
- `movie_title` (optional)
- `genre` (optional)
- `window`: 7d, 30d, 90d

**Response:** Time-series data with trend analysis

---

### Endpoint Category 5: Search Endpoints

#### `GET /api/v1/search/movies`
**Purpose:** Search movies with merged data

**Query Parameters:**
- `q`: Search query
- `genre`: Genre filter
- `min_sentiment`: Minimum sentiment score
- `sort_by`: sentiment, engagement, viral_score, recommendation_score

**Response:** List of movies matching criteria with merged data

---

### Endpoint Category 6: Health Endpoints

#### `GET /api/v1/health`
**Purpose:** System health check

**Response Example:**
```json
{
  "status": "healthy",
  "timestamp": "2025-12-14T10:00:00Z",
  "services": {
    "mongodb": {
      "status": "up",
      "latency_ms": 5,
      "collections": {
        "batch_views": {"count": 5821, "last_update": "2025-12-11T02:00:00Z"},
        "speed_views": {"count": 1248, "last_sync": "2025-12-14T09:55:00Z"}
      }
    },
    "redis": {
      "status": "up",
      "latency_ms": 2,
      "hit_rate": 0.76,
      "memory_usage_mb": 128
    }
  },
  "data_freshness": {
    "batch_layer": {
      "last_update": "2025-12-11T02:00:00Z",
      "staleness_hours": 80,
      "status": "normal"
    },
    "speed_layer": {
      "last_sync": "2025-12-14T09:55:00Z",
      "lag_seconds": 300,
      "status": "healthy"
    }
  },
  "version": "2.0.0"
}
```

---

## 🎨 Visualization Strategy

### Grafana Dashboard Purpose & Metrics

**Dashboard 1: Business Metrics (Executive View)**
- **Target Audience:** Business stakeholders, marketing teams, PR managers
- **Refresh Rate:** 1 minute
- **Purpose:** Monitor business goal KPIs

**Dashboard 2: System Health (DevOps View)**
- **Target Audience:** DevOps, SRE, backend engineers
- **Refresh Rate:** 30 seconds
- **Purpose:** Ensure system reliability and performance

**Dashboard 3: Data Quality (Data Engineering View)**
- **Target Audience:** Data engineers, data scientists
- **Refresh Rate:** 5 minutes
- **Purpose:** Monitor data pipeline quality

**Dashboard 4: Reddit Engagement (Product View)**
- **Target Audience:** Product managers, content strategists
- **Refresh Rate:** 1 minute
- **Purpose:** Understand user engagement patterns

**Dashboard 5: TMDB Context (Analyst View)**
- **Target Audience:** Data analysts, business analysts
- **Refresh Rate:** 1 hour (batch data)
- **Purpose:** Historical context and baselines

---

## 🔧 Implementation Roadmap (REVISED)

### What's Already Built (Don't Redo)

✅ **Core Infrastructure (100% Complete):**
- MongoDB client with connection pooling (`mongodb/client.py`)
- Redis cache manager with TTL support (`query_engine/cache_manager.py`)
- FastAPI app with lifecycle management (`api/main.py`)
- ViewMerger base class with 48-hour cutoff (`query_engine/view_merger.py`)
- MongoDB query builders (`mongodb/queries.py`)
- Basic endpoints: movies, trending, recommendations

✅ **Working Features:**
- Movie details endpoint: `GET /movies/{id}` - merges batch metadata + speed stats
- Movie sentiment endpoint: `GET /movies/{id}/sentiment` - basic implementation exists
- Trending endpoint: `GET /trending/movies` - basic trending calculation
- Recommendation engine: Content-based filtering working
- Cache decorators and middleware
- CORS, rate limiting setup

---

### Phase 1: Business Logic Implementation (Week 1)
**Priority: CRITICAL - Core business goals**

**Task 1.1: Crisis Detection Enhancement** (2 days)
File: `query_engine/view_merger.py` - Enhance `merge_sentiment_data()`

Current state: Basic sentiment merging exists
Need to add:
- [ ] Genre baseline lookup from batch layer
- [ ] 3-sigma threshold calculation
- [ ] Sentiment velocity calculation (6-hour window)
- [ ] Franchise baseline comparison
- [ ] Crisis severity levels (warning/critical)

```python
# Add to ViewMerger class
def merge_sentiment_data(self, movie_title: str, window_hours: int = 48):
    """Enhanced crisis detection logic"""
    cutoff = self.get_cutoff_time()
    
    # 1. Get Reddit sentiment from speed layer
    reddit_docs = list(self.speed_views.find({
        "movie_title": movie_title,
        "data_type": "reddit_post",
        "hour": {"$gte": cutoff}
    }).sort("hour", -1))
    
    if not reddit_docs:
        return {"error": "No recent Reddit data"}
    
    # 2. Calculate current sentiment from metrics object
    current_sentiment = np.mean([
        doc["metrics"]["avg_sentiment"] for doc in reddit_docs
    ])
    
    # 3. Get movie metadata for genre
    movie_intel = self.batch_views.find_one({
        "view_type": "movie_intelligence",
        "title": movie_title
    })
    
    genre = movie_intel["genre"] if movie_intel else None
    
    # 4. Get genre baseline from batch layer
    baseline = self.batch_views.find_one({
        "view_type": "sentiment_baseline",
        "genre": genre
    })
    
    baseline_sentiment = baseline["avg_sentiment"]
    stddev = baseline["sentiment_stddev"]
    
    # 5. Crisis detection logic
    deviation = current_sentiment - baseline_sentiment
    sigma_level = abs(deviation) / stddev if stddev > 0 else 0
    is_crisis = sigma_level >= 3.0
    
    # 6. Calculate sentiment velocity (last 6 hours)
    velocity = self._calculate_sentiment_velocity(reddit_docs, hours=6)
    
    return {
        "movie_title": movie_title,
        "current_sentiment": current_sentiment,
        "baseline_sentiment": baseline_sentiment,
        "deviation": deviation,
        "sigma_level": sigma_level,
        "is_crisis": is_crisis,
        "sentiment_velocity": velocity,
        # ... more fields
    }
```

**Task 1.2: Viral Detection Implementation** (2 days)
File: `query_engine/view_merger.py` - Add new `merge_viral_data()`

Current state: NOT IMPLEMENTED
Need to add:
- [ ] New function `merge_viral_data(movie_title, limit)`
- [ ] Calculate upvote/comment/award velocities from speed layer
- [ ] Lookup viral thresholds from batch layer
- [ ] Calculate viral coefficient
- [ ] Track cross-subreddit spread
- [ ] Percentile ranking

**Task 1.3: Dual-Success Recommendations** (2 days)
File: `query_engine/recommendation_engine.py` - Enhance existing

Current state: Content-based filtering exists
Need to enhance:
- [ ] Add Reddit buzz score calculation (from speed layer)
- [ ] Weighted scoring: 60% Reddit buzz + 40% TMDB quality
- [ ] Genre-aware filtering
- [ ] Trending boost integration

---

### Phase 2: API Endpoints Enhancement (Week 2)
**Priority: HIGH - Expose business logic**

**Task 2.1: Update Sentiment Endpoint** (1 day)
File: `api/routes/movies.py`

- [ ] Update to use enhanced `merge_sentiment_data()`
- [ ] Return crisis analysis in response
- [ ] Add query parameters for window (6h, 12h, 24h)

2. Implement `query_engine/query_router.py`
   - [ ] 48-hour cutoff routing
   - [ ] Query optimization
   - [ ] Error handling for missing data

3. Update `query_engine/cache_manager.py`
   - [ ] Cache decorators for all endpoint types
   - [ ] TTL strategies per endpoint
   - [ ] Cache invalidation logic

**Deliverable:** Core merger working with basic caching

---

### Phase 2: API Endpoints (Week 2)
**Priority: HIGH**

**Tasks:**
1. Implement movie endpoints
   - [ ] `GET /api/v1/movies/{movie_title}`
   - [ ] `GET /api/v1/movies/{movie_title}/sentiment`
   - [ ] Response formatting
   - [ ] Error handling

2. Implement viral endpoints
   - [ ] `GET /api/v1/viral/trending`
   - [ ] `GET /api/v1/viral/{movie_title}`
   - [ ] Viral coefficient calculation
   - [ ] Cross-subreddit tracking

3. Implement recommendation endpoints
   - [ ] `GET /api/v1/recommendations`
   - [ ] Dual-success scoring
   - [ ] Genre filtering
   - [ ] Sorting and pagination

4. Implement analytics endpoints
   - [ ] `GET /api/v1/analytics/genre/{genre}`
   - [ ] `GET /api/v1/analytics/trends/sentiment`
   - [ ] Time-series aggregations

**Deliverable:** Complete API with all business goal endpoints

---

### Phase 3: Monitoring & Visualization (Week 3)
**Priority: MEDIUM** (Most work already done)

**Tasks:**
1. ✅ Grafana container - ALREADY CONFIGURED
   - ✅ Docker-compose file exists
   - ✅ 5 dashboards created (89KB JSON files)
   - ✅ Infinity datasource configured
   - [ ] Start Grafana: `cd layers/serving_layer/visualization/grafana && ./start-grafana.sh`
   - [ ] Verify dashboards work with actual API
   - [ ] Adjust dashboard queries if needed

2. Add Prometheus + Exporters (NEW - not yet configured)
   - [ ] Add Prometheus container to docker-compose
   - [ ] Add MongoDB exporter container
   - [ ] Add Redis exporter container
   - [ ] Configure scrape targets
   - [ ] Configure alerting rules

3. Instrument FastAPI (NEW - not yet configured)
   - [ ] Install `prometheus-fastapi-instrumentator`
   - [ ] Add `/metrics` endpoint
   - [ ] Add custom business metrics:
     - `crisis_alerts_total` counter
     - `viral_movies_detected` counter
     - `recommendation_requests_total` counter
   - [ ] Add Prometheus datasource to Grafana

**Deliverable:** Working Grafana + Prometheus monitoring stack

**Estimated Effort:** 2-3 days (down from 1 week because Grafana is done)

---

### Phase 4: Testing & Optimization (Week 4)
**Priority: MEDIUM**

**Tasks:**
1. Unit tests
   - [ ] Merge logic tests
   - [ ] Cache manager tests
   - [ ] Business logic tests

2. Integration tests
   - [ ] End-to-end API tests
   - [ ] MongoDB query tests
   - [ ] Cache integration tests

3. Performance optimization
   - [ ] MongoDB index optimization
   - [ ] Query performance tuning
   - [ ] Cache TTL optimization
   - [ ] Load testing (1000 req/s target)

4. Documentation
   - [ ] API documentation (Swagger)
   - [ ] Dashboard user guides
   - [ ] Deployment guide updates

**Deliverable:** Production-ready serving layer with >95% test coverage

---

### Phase 5: Advanced Features (Week 5)
**Priority: LOW**

**Tasks:**
1. Authentication & authorization
   - [ ] API key authentication
   - [ ] Rate limiting per user
   - [ ] JWT tokens

2. Advanced analytics
   - [ ] Prediction engine (trend prediction)
   - [ ] Anomaly detection
   - [ ] A/B testing framework

3. Enhanced caching
   - [ ] Predictive cache warming
   - [ ] Multi-tier caching (L1: Redis, L2: MongoDB)

**Deliverable:** Enterprise-grade features

---

## 📊 Success Metrics

### Business Goal Metrics

| Business Goal | Metric | Target | Current |
|---------------|--------|--------|---------|
| **Goal #1: Crisis Detection** | Time to detect sentiment drop | < 6 hours | TBD |
| | False positive rate | < 5% | TBD |
| | Crisis alert accuracy | > 90% | TBD |
| **Goal #2: Viral Identification** | Time to detect viral surge | < 1 hour | TBD |
| | Viral prediction accuracy | > 85% | TBD |
| | Marketing response time | < 24 hours | TBD |
| **Goal #3: Recommendations** | Recommendation CTR | > 15% | TBD |
| | User engagement increase | > 25% | TBD |
| | Dual-success rate | > 60% | TBD |

---

### Technical Metrics

| Component | Metric | Target | Monitoring |
|-----------|--------|--------|------------|
| **API Performance** | p95 latency | < 100ms | Prometheus |
| | Throughput | > 1000 req/s | Prometheus |
| | Error rate | < 0.1% | Prometheus |
| **Cache** | Hit rate | > 70% | Redis Exporter |
| | Average latency | < 2ms | Redis Exporter |
| **MongoDB** | Query latency (p95) | < 50ms | MongoDB Exporter |
| | Index hit ratio | > 95% | MongoDB Exporter |
| **Data Freshness** | Speed layer lag | < 5 min | Custom metric |
| | Batch layer staleness | < 24 hours | Custom metric |

---

## 🚨 Alerting Strategy

### Critical Alerts (PagerDuty/Email)
- **API Down:** Service unreachable for > 1 minute
- **High Error Rate:** Error rate > 1% for > 5 minutes
- **MongoDB Down:** Database unreachable
- **Redis Down:** Cache unavailable
- **Data Pipeline Failure:** No speed layer updates for > 15 minutes

### Warning Alerts (Slack/Dashboard)
- **High Latency:** p95 > 100ms for > 5 minutes
- **Low Cache Hit Rate:** Hit rate < 50% for > 10 minutes
- **Stale Data:** Speed layer lag > 10 minutes
- **High Resource Usage:** CPU/memory > 80%

### Business Alerts (Email/Dashboard)
- **Crisis Detected:** New PR crisis identified
- **Viral Surge:** Movie crosses viral threshold
- **Recommendation Performance:** Low engagement on recommendations

---

## 🔒 Security Considerations

### API Security
- **Authentication:** API key required for all endpoints
- **Rate Limiting:** 100 requests/minute per user (burst: 20)
- **Input Validation:** Sanitize all user inputs
- **SQL Injection Prevention:** Use parameterized queries
- **CORS:** Whitelist allowed origins

### Data Security
- **MongoDB:** Username/password authentication enabled
- **Redis:** Password protection enabled
- **Environment Variables:** Use .env for secrets
- **Network Isolation:** Internal services not exposed publicly

### Monitoring Security
- **Grafana:** Authentication required
- **Prometheus:** Internal network only
- **API Keys:** Rotate every 90 days

---

## 🎓 Documentation Deliverables

### User Documentation
1. **API Reference Guide** - Complete endpoint documentation with examples
2. **Dashboard User Guide** - How to use each Grafana dashboard
3. **Business Metrics Guide** - Interpreting crisis alerts, viral scores, recommendations

### Technical Documentation
1. **Architecture Guide** - Updated with new containers and merge logic
2. **Deployment Guide** - Step-by-step deployment instructions
3. **Monitoring Guide** - Setting up Prometheus, Grafana, alerts
4. **Troubleshooting Guide** - Common issues and solutions

### Developer Documentation
1. **API Development Guide** - Adding new endpoints
2. **Testing Guide** - Writing tests for merge logic
3. **Performance Tuning Guide** - Optimizing queries and cache

---

## 🏁 Acceptance Criteria

### Functional Requirements
- [ ] All three business goals supported by API endpoints
- [ ] 48-hour cutoff merge logic working correctly
- [ ] Crisis detection with 3-sigma threshold
- [ ] Viral coefficient calculation
- [ ] Dual-success recommendation scoring
- [ ] 5 Grafana dashboards operational

### Non-Functional Requirements
- [ ] API p95 latency < 100ms
- [ ] Cache hit rate > 70%
- [ ] MongoDB query p95 < 50ms
- [ ] Zero data loss during merge
- [ ] Speed layer lag < 5 minutes
- [ ] Test coverage > 80%

### Documentation Requirements
- [ ] All endpoints documented in Swagger
- [ ] Dashboard user guides complete
- [ ] Deployment guide updated
- [ ] Troubleshooting guide written

---

## 📞 Contact & Support

**Team:** Data Engineering & Analytics  
**Project Lead:** [Your Name]  
**Documentation:** `/layers/serving_layer/README.md`  
**Issue Tracking:** GitHub Issues  
**Slack Channel:** #movie-pipeline-serving

---

---

## 📝 Quick Status Summary

### What's Already Built (Don't Rebuild)
| Component | Status | Location |
|-----------|--------|----------|
| FastAPI Application | ✅ Running | `layers/serving_layer/api/` |
| MongoDB Client | ✅ Working | `layers/serving_layer/mongodb/` |
| Redis Cache | ✅ Working | `layers/serving_layer/query_engine/cache_manager.py` |
| ViewMerger Base | ✅ Partial | `layers/serving_layer/query_engine/view_merger.py` |
| Basic Endpoints | ✅ Working | `/movies/{id}`, `/trending/movies`, etc. |
| **Grafana Setup** | ✅ **Ready** | `layers/serving_layer/visualization/grafana/` |
| **5 Dashboards** | ✅ **Created** | 89KB JSON files ready to use |
| **Start Scripts** | ✅ **Ready** | `start-grafana.sh`, `stop-grafana.sh` |

### What Needs Implementation (Focus Here)
| Priority | Component | Effort | Files to Modify |
|----------|-----------|--------|-----------------|
| 🔴 CRITICAL | Crisis Detection Logic | 2 days | `query_engine/view_merger.py` |
| 🔴 CRITICAL | Viral Scoring Logic | 2 days | `query_engine/view_merger.py` |
| 🔴 CRITICAL | Dual-Success Recommendations | 2 days | `query_engine/recommendation_engine.py` |
| 🟡 MEDIUM | Prometheus + Exporters | 2-3 days | New containers in `docker-compose.yml` |
| 🟡 MEDIUM | FastAPI `/metrics` endpoint | 1 day | `api/main.py` |
| 🟢 LOW | Grafana verification | 1 day | Test existing dashboards |

### Quick Start Commands

**To Start Grafana (Ready Now):**
```bash
cd layers/serving_layer/visualization/grafana
./start-grafana.sh
# Access: http://localhost:3000 (admin/admin)
```

**To Implement Business Logic:**
1. Edit `layers/serving_layer/query_engine/view_merger.py`
2. Add crisis detection, viral scoring functions
3. Test with actual MongoDB data

**Total Effort Estimate:** 2-3 weeks (reduced from original 5 weeks because infrastructure is done)

---

**Document Status:** ✅ Ready for Implementation  
**Last Updated:** December 14, 2025  
**Next Review:** After Phase 1 completion
