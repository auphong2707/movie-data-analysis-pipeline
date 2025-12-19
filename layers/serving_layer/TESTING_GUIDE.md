# Serving Layer Testing Guide

**Purpose:** Test the three business goals of the Movie Social Engagement Analytics Pipeline

---

## 🎯 Three Business Goals

1. **PR Crisis Detection & Sentiment Monitoring**
2. **Viral Content Identification for Marketing Amplification**
3. **Content Recommendation Optimization**

---

## 🚀 Quick Start

### Prerequisites

```bash
# Start all services
cd /home/veil/Documents/GitHub/movie-data-analysis-pipeline
docker-compose up -d serving-mongodb

cd layers/serving_layer
docker-compose -f docker-compose.serving.yml up -d

# Verify services are healthy
docker ps --filter "name=serving-"
curl http://localhost:8000/api/v1/health | jq .status
```

**Expected Result:** All containers running, API status: "healthy"

---

## 📚 Understanding Old vs New Movies in Lambda Architecture

### The 48-Hour Window

This pipeline uses **Lambda Architecture** with two data layers:

**Batch Layer (Historical Data):**
- Source: TMDB API metadata + historical reviews
- Coverage: ~311 movies from TMDB popular/top_rated/now_playing/upcoming
- Update frequency: Daily batch processing
- Database: `moviedb.batch_views` collection
- Data age: Can be months or years old

**Speed Layer (Real-Time Data):**
- Source: Reddit discussions (r/movies, r/boxoffice, r/TrueFilm)
- Coverage: Only movies discussed in **last 48 hours**
- Update frequency: Every 5 minutes (Spark streaming)
- Database: `moviedb.speed_views` collection (48h TTL)
- Data age: Maximum 48 hours

### Test Case Categories

| Movie Type | Batch Data | Speed Data | Use Cases |
|------------|------------|------------|-----------|
| **Old Movies** (pre-2024) | ✅ Yes | ❌ No | Historical analysis, baseline comparisons |
| **New Movies** (2024-2025) | ✅ Yes | ✅ Yes (if discussed) | Real-time monitoring, crisis detection, viral trends |

### Key Testing Distinctions

**When Testing Old Movies:**
- Expect `data_sources.speed = null`
- Expect empty `breakdown` arrays
- Expect low `confidence` scores (< 0.5)
- Expect zero `reddit_mentions`
- Still useful for: genre baselines, franchise comparisons, historical context

**When Testing New Movies:**
- Expect `data_sources.speed` with recent timestamp
- Expect populated `breakdown` arrays (hourly sentiment)
- Expect high `confidence` scores (≥ 0.7)
- Expect non-zero `reddit_mentions`
- Enables: real-time crisis detection, viral identification, velocity tracking

### Movies Available for Testing

**Old Movies (Batch Only):**
- The Flash (2023, movie_id: 298618)
- Oppenheimer (2023, movie_id: 872585)
- Barbie (2023, movie_id: 346698)

**New Movies (Batch + Speed):**
- Wicked (2024, movie_id: 402431)
- Nosferatu (2024, movie_id: 748783)
- Mufasa: The Lion King (2024, movie_id: 762509)
- Zootopia 2 (2025, movie_id: 1084242)

> **Note:** New movie speed data depends on active Reddit discussions. If Reddit isn't talking about a 2024 movie, it will behave like an old movie (batch only).

---

## 🎯 Goal #1: PR Crisis Detection & Sentiment Monitoring

### Purpose
Detect when current Reddit discussion sentiment drops significantly below historical TMDB baselines to alert PR teams for immediate response.

### API Endpoints
```bash
GET /api/v1/movies/{movie_id}/sentiment        # By TMDB movie ID
GET /api/v1/movies/by-title/{title}/sentiment  # By movie title
```

---

### Test Case 1A: Old Movie (No Real-Time Reddit Data)

**Scenario:** Testing a 2023 movie with no recent Reddit discussions

**Test "The Flash" (2023 release, not currently trending):**
```bash
curl -s "http://localhost:8000/api/v1/movies/by-title/The%20Flash/sentiment" | jq .
```

**Expected Response:**
```json
{
  "movie_id": 298618,
  "title": "The Flash",
  "sentiment": {
    "overall_score": 0.0,          // ✅ No recent sentiment data
    "label": "neutral",
    "positive_count": 0,
    "negative_count": 0,
    "neutral_count": 0,
    "total_reviews": 0,            // ✅ No Reddit reviews in 48h window
    "velocity": 0,
    "confidence": 0.3              // ✅ Low confidence - batch data only
  },
  "breakdown": [],                 // ✅ Empty - no speed layer data
  "data_sources": {
    "batch": "2025-12-13T01:16:41.221826",  // ✅ Historical batch data only
    "speed": null                  // ✅ No recent Reddit data
  }
}
```

**What This Tests:**
- ✅ API handles movies with **no active Reddit discussions**
- ✅ Returns batch layer data when speed layer is empty
- ✅ Indicates low confidence when only historical data exists
- ✅ Empty breakdown array when no hourly data available

**Use Cases:**
- Older movies (pre-2024)
- Movies not currently being discussed
- Movies outside Reddit's popular subreddits

**Success Criteria:**
- ✅ Returns 200 OK (not 404)
- ✅ `speed` data source is `null`
- ✅ `confidence` is low (< 0.5)
- ✅ Response time < 100ms

---

### Test Case 1B: New Movie (Active Reddit Discussions)

**Scenario:** Testing a 2024/2025 movie with current Reddit buzz

**Test "Wicked" (2024 release, currently trending):**
```bash
curl -s "http://localhost:8000/api/v1/movies/by-title/Wicked/sentiment" | jq .
```

**Expected Response:**
```json
{
  "movie_id": 402431,
  "title": "Wicked",
  "sentiment": {
    "overall_score": 0.12,         // ✅ Real-time Reddit sentiment
    "label": "positive",
    "positive_count": 0,
    "negative_count": 0,
    "neutral_count": 0,
    "total_reviews": 7,            // ✅ Reddit mentions in last 48h
    "reddit_mentions": 7,          // ✅ Speed layer data
    "velocity": 0,
    "confidence": 0.85             // ✅ High confidence - fresh data
  },
  "breakdown": [                   // ✅ Hourly breakdown (newest first)
    {
      "date": "2025-12-14 18:00",
      "avg_sentiment": 0.78,
      "post_count": 0,
      "data_type": "reddit_comment"
    },
    {
      "date": "2025-12-14 17:00",
      "avg_sentiment": 0.44,
      "post_count": 0,
      "data_type": "reddit_comment"
    }
  ],
  "data_sources": {
    "batch": "2025-12-13T01:16:17.256024",
    "speed": "2025-12-15T01:16:17.258475"  // ✅ Recent speed layer update
  }
}
```

**What This Tests:**
- ✅ API merges **batch + speed layer data**
- ✅ Returns real-time Reddit sentiment from last 48 hours
- ✅ Provides hourly breakdown sorted **newest first**
- ✅ High confidence score with fresh data
- ✅ Shows both `total_reviews` and `reddit_mentions`

**Use Cases:**
- New releases (2024-2025)
- Movies currently trending on Reddit
- Active marketing campaigns

**Success Criteria:**
- ✅ Returns 200 OK
- ✅ `speed` data source has recent timestamp
- ✅ `confidence` is high (≥ 0.7)
- ✅ `breakdown` array contains entries (sorted newest first)
- ✅ `reddit_mentions` > 0
- ✅ Response time < 100ms

---

### Test Case 1C: Compare Old vs New Movie Side-by-Side

**Run both tests and compare:**
```bash
echo "=== OLD MOVIE (The Flash 2023) ===" && \
curl -s "http://localhost:8000/api/v1/movies/by-title/The%20Flash/sentiment" | jq '{
  title, 
  reddit_mentions: .sentiment.reddit_mentions,
  confidence: .sentiment.confidence,
  has_speed_data: (.data_sources.speed != null),
  breakdown_count: (.breakdown | length)
}'

echo -e "\n=== NEW MOVIE (Wicked 2024) ===" && \
curl -s "http://localhost:8000/api/v1/movies/by-title/Wicked/sentiment" | jq '{
  title,
  reddit_mentions: .sentiment.reddit_mentions, 
  confidence: .sentiment.confidence,
  has_speed_data: (.data_sources.speed != null),
  breakdown_count: (.breakdown | length)
}'
```

**Expected Comparison:**
```
=== OLD MOVIE (The Flash 2023) ===
{
  "title": "The Flash",
  "reddit_mentions": null,          // ✅ No recent mentions
  "confidence": 0.3,                // ✅ Low confidence
  "has_speed_data": false,          // ✅ No speed layer
  "breakdown_count": 0              // ✅ Empty breakdown
}

=== NEW MOVIE (Wicked 2024) ===
{
  "title": "Wicked",
  "reddit_mentions": 7,             // ✅ Active discussions
  "confidence": 0.85,               // ✅ High confidence
  "has_speed_data": true,           // ✅ Speed layer present
  "breakdown_count": 17             // ✅ Hourly breakdown
}
```

---

### Test Case 1D: Title vs ID Access

**Both endpoints should work:**
```bash
# Access by title (user-friendly)
curl -s "http://localhost:8000/api/v1/movies/by-title/Zootopia/sentiment" | jq '.movie_id'

# Access by ID (programmatic)
curl -s "http://localhost:8000/api/v1/movies/269149/sentiment" | jq '.title'
```

**Success Criteria:**
- ✅ Both return same movie data
- ✅ Title lookup handles URL encoding
- ✅ ID lookup handles numeric input
- ✅ Both endpoints have same response schema

### Automated Test
```bash
# Run crisis detection tests
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py::TestCrisisDetection -v
```

**Expected Output:**
```
TestCrisisDetection::test_sentiment_endpoint_exists PASSED
TestCrisisDetection::test_sentiment_response_structure PASSED
TestCrisisDetection::test_genre_baseline_comparison PASSED
```

---

## 🎯 Goal #2: Viral Content Identification

### Purpose
Identify breakout content by comparing real-time Reddit engagement velocity against historical viral thresholds to enable marketing amplification.

### API Endpoint

```bash
GET /api/v1/trending/movies
```

### 📊 Understanding Viral Detection: Old vs New Movies

**Old Movies (No Speed Layer Data):**
- Will NOT appear in trending endpoint
- No real-time Reddit velocity metrics available
- Historical viral thresholds exist but no current comparison possible

**New Movies (With Speed Layer Data):**
- Appear in trending endpoint if Reddit activity detected
- Real-time upvote/comment/award velocity calculated from 48h window
- Viral coefficient shows current activity vs historical thresholds
- Updated every 5 minutes as Spark processes Reddit streams

**Why This Matters:**
- Trending endpoint only returns movies with **recent Reddit activity**
- Old movies like "The Flash" (2023) won't show up unless Reddit discusses them again
- New releases like "Wicked" (2024) will appear if they have current buzz

---

### Test Case 1: Identify Viral Movies (Coefficient > 1.0)

**Get top viral movies:**
```bash
curl -s "http://localhost:8000/api/v1/trending/movies?limit=5" | jq .
```

**What to Verify:**
```json
{
  "viral_movies": [
    {
      "movie_title": "Barbie",
      "viral_metrics": {
        "upvote_velocity": 500.2,      // ✅ Upvotes per hour
        "comment_velocity": 82.5,      // ✅ Comments per hour
        "award_velocity": 1.8          // ✅ Awards per hour
      },
      "viral_threshold": {
        "threshold": 300,              // ✅ Genre-specific threshold
        "genre": "Comedy"
      },
      "viral_analysis": {
        "viral_coefficient": 1.67,     // ✅ 500/300 = 1.67x viral!
        "status": "VIRAL",             // ✅ Status: VIRAL/TRENDING/NORMAL
        "percentile": 99.2,            // ✅ Top 0.8% of all movies
        "cross_subreddit_spread": 8    // ✅ Spreading across subreddits
      }
    }
  ]
}
```

**Success Criteria:**
- ✅ `viral_coefficient > 1.0` means above viral threshold
- ✅ `status: "VIRAL"` for coefficient > 1.5
- ✅ `status: "TRENDING"` for coefficient 1.0-1.5
- ✅ `cross_subreddit_spread` tracks viral spread
- ✅ Response time < 100ms

### Test Case 2: Filter by Viral Threshold

**Get only highly viral content (>2x threshold):**
```bash
curl -s "http://localhost:8000/api/v1/trending/movies?viral_coefficient_threshold=2.0&limit=10" | jq .
```

**What to Verify:**
```json
{
  "threshold_used": 2.0,
  "viral_movies": [
    {
      "viral_analysis": {
        "viral_coefficient": 2.3  // ✅ All movies > 2.0
      }
    }
  ]
}
```

**Success Criteria:**
- ✅ All returned movies have `viral_coefficient >= 2.0`
- ✅ Empty array if no movies meet threshold

### Test Case 3: Genre-Specific Viral Detection

**Get viral Action movies:**
```bash
curl -s "http://localhost:8000/api/v1/trending/movies?genre=Action&limit=10" | jq .
```

**What to Verify:**
- ✅ Viral threshold is genre-specific (Action threshold ≠ Comedy threshold)
- ✅ All returned movies are Action genre

### Test Case 4: Cross-Subreddit Viral Spread

**Check viral spread across Reddit:**
```bash
curl -s "http://localhost:8000/api/v1/trending/movies?limit=3" | jq '.viral_movies[0].viral_analysis.cross_subreddit_spread'
```

**What to Verify:**
```
8  // ✅ Movie discussed in 8 different subreddits
```

**Success Criteria:**
- ✅ Higher cross-subreddit count = broader viral spread
- ✅ Value >= 5 indicates significant viral momentum

### Automated Test
```bash
# Run viral scoring tests
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py::TestViralScoring -v
```

**Expected Output:**
```
TestViralScoring::test_viral_coefficient_calculation PASSED
TestViralScoring::test_cross_subreddit_tracking PASSED
TestViralScoring::test_viral_threshold_filtering PASSED
```

---

## 🎯 Goal #3: Content Recommendation Optimization

### Purpose
Surface trending content by combining fresh Reddit buzz (60%) with historical TMDB quality (40%) to recommend movies that are both critically acclaimed and socially engaging.

### API Endpoint
```bash
GET /api/v1/recommendations
```

### Test Case 1: Dual-Success Recommendations (60% Reddit + 40% TMDB)

**Get top dual-success movies:**
```bash
curl -s "http://localhost:8000/api/v1/recommendations?genre=Action&limit=10" | jq .
```

**What to Verify:**
```json
{
  "recommendations": [
    {
      "movie_title": "Oppenheimer",
      "dual_success_score": 87.5,      // ✅ 0-100 score
      "reddit_metrics": {
        "buzz_score": 92.0,            // ✅ 60% weight
        "sentiment": 0.85,
        "engagement": 25680,
        "subreddit_spread": 12
      },
      "tmdb_metrics": {
        "quality_score": 81.0,         // ✅ 40% weight
        "vote_average": 8.1,
        "vote_count": 4521
      },
      "analysis": {
        "status": "DUAL_SUCCESS",      // ✅ High on both dimensions
        "reason": "High Reddit buzz (0.92) + Strong TMDB rating (0.81)"
      }
    }
  ],
  "algorithm": {
    "weights": {
      "reddit_buzz": 0.6,              // ✅ 60% Reddit
      "tmdb_quality": 0.4              // ✅ 40% TMDB
    }
  }
}
```

**Success Criteria:**
- ✅ `dual_success_score = (reddit_buzz * 0.6) + (tmdb_quality * 0.4)`
- ✅ Score between 0-100
- ✅ `status: "DUAL_SUCCESS"` for high-performing movies
- ✅ Response time < 200ms

### Test Case 2: Verify 60/40 Weighting

**Calculate score manually:**
```bash
curl -s "http://localhost:8000/api/v1/recommendations?genre=Sci-Fi&limit=1" | jq '.recommendations[0] | {
  reddit: .reddit_metrics.buzz_score,
  tmdb: .tmdb_metrics.quality_score,
  dual_success: .dual_success_score
}'
```

**Manual Calculation:**
```
Reddit Buzz: 92.0
TMDB Quality: 81.0

Expected Score = (92.0 * 0.6) + (81.0 * 0.4)
               = 55.2 + 32.4
               = 87.6

Actual Score: 87.5 ✅ (matches within rounding)
```

**Success Criteria:**
- ✅ Manual calculation matches API response
- ✅ Reddit component (60%) has higher weight

### Test Case 3: Genre-Aware Filtering

**Compare different genres:**
```bash
# Action recommendations
curl -s "http://localhost:8000/api/v1/recommendations?genre=Action&limit=5" | jq '.recommendations[].movie_title'

# Comedy recommendations
curl -s "http://localhost:8000/api/v1/recommendations?genre=Comedy&limit=5" | jq '.recommendations[].movie_title'
```

**What to Verify:**
- ✅ Action results only contain Action movies
- ✅ Comedy results only contain Comedy movies
- ✅ Different genres have different recommendations

### Test Case 4: Minimum Rating Filter

**Get high-quality recommendations only:**
```bash
curl -s "http://localhost:8000/api/v1/recommendations?min_rating=8.0&limit=10" | jq .
```

**What to Verify:**
```json
{
  "recommendations": [
    {
      "tmdb_metrics": {
        "vote_average": 8.3  // ✅ All movies >= 8.0
      }
    }
  ]
}
```

**Success Criteria:**
- ✅ All returned movies have `vote_average >= 8.0`
- ✅ Combines with Reddit buzz to surface quality trending content

### Automated Test
```bash
# Run dual-success recommendation tests
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py::TestDualSuccessRecommendations -v
```

**Expected Output:**
```
TestDualSuccessRecommendations::test_recommendations_endpoint_exists PASSED
TestDualSuccessRecommendations::test_dual_success_scoring PASSED
TestDualSuccessRecommendations::test_recommendations_with_filters PASSED
```

---

## 🧪 Run All Tests

### Full Test Suite
```bash
# Run all 35 tests
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py -v

# Expected output:
# ===== 33 passed, 2 skipped in 1.75s =====
```

### Run Tests by Business Goal
```bash
# Goal #1: Crisis Detection (3 tests)
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py::TestCrisisDetection -v

# Goal #2: Viral Scoring (3 tests)
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py::TestViralScoring -v

# Goal #3: Recommendations (3 tests)
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py::TestDualSuccessRecommendations -v
```

---

## 📊 Monitoring & Metrics

### Check Prometheus Metrics

**Business metrics exposed at `/metrics`:**
```bash
curl -s http://localhost:8000/metrics | grep -E "crisis_alerts|viral_detections|recommendation_requests"
```

**Expected metrics:**
```
# HELP crisis_alerts_total PR crisis alerts triggered
# TYPE crisis_alerts_total counter
crisis_alerts_total{genre="Action",severity="critical"} 5.0

# HELP viral_detections_total Viral content detected
# TYPE viral_detections_total counter
viral_detections_total{genre="Comedy"} 12.0

# HELP recommendation_requests_total Recommendation requests
# TYPE recommendation_requests_total counter
recommendation_requests_total{genre="Sci-Fi"} 45.0
```

### View in Grafana

```bash
# Grafana should already be running via docker-compose
# If not started, run:
cd /home/veil/Documents/GitHub/movie-data-analysis-pipeline
docker-compose up -d serving-grafana

# Access at http://localhost:3001 (admin/admin)
# Note: Grafana runs on port 3001 (mapped from container port 3000)
```

**Available Dashboards:**
1. **System Health Dashboard** - API/MongoDB/Redis metrics
2. **Data Freshness Dashboard** - Batch/speed layer sync
3. **Genre Analytics Dashboard** - Genre performance
4. **Movie Analytics Overview** - Overall analytics
5. **Trending Movies Dashboard** - Viral content trends

**⚠️ Missing Business-Focused Dashboards:**
- **PR Crisis Detection Dashboard** - Real-time sentiment drops, crisis alerts by severity, genre baselines
- **Viral Content Dashboard** - Viral coefficient tracking, cross-subreddit spread, engagement velocity
- **Recommendation Performance Dashboard** - Dual-success score distribution, recommendation click-through, A/B test results

---

## � Dashboard Visualization Testing

### Purpose
Validate that Grafana dashboards correctly visualize metrics from the serving layer and provide actionable insights for the three business goals.

### Prerequisites

**Start Grafana and generate test data:**

```bash
# 1. Start Grafana (should already be running via docker-compose)
cd /home/veil/Documents/GitHub/movie-data-analysis-pipeline
docker-compose up -d serving-grafana

# 2. Access Grafana at http://localhost:3001
# Default credentials: admin/admin
# Note: Grafana runs on port 3001 (mapped from container port 3000)

# 3. Generate test traffic to populate metrics
for i in {1..50}; do
  curl -s "http://localhost:8000/api/v1/movies/by-title/Wicked/sentiment" > /dev/null
  curl -s "http://localhost:8000/api/v1/trending/movies?limit=10" > /dev/null
  curl -s "http://localhost:8000/api/v1/recommendations?genre=Action&limit=10" > /dev/null
  sleep 2
done
```

---

### Test Case 1: System Health Dashboard

**Dashboard:** System Health Dashboard  
**Purpose:** Monitor API performance, database connections, and cache hit rates

**Test Steps:**

1. Navigate to http://localhost:3001
2. Login with credentials (admin/admin)
3. Search for "System Health Dashboard" in the dashboard search
4. Select time range: Last 15 minutes

**What to Verify:**

| Panel | Metric | Expected Behavior |
|-------|--------|-------------------|
| **API Request Rate** | `http_requests_total` | Should show spike from test traffic (50+ requests) |
| **API Response Time** | `http_request_duration_seconds` | P95 latency < 200ms |
| **API Success Rate** | `http_requests_total{status="200"}` | Should be ~100% (green) |
| **MongoDB Connections** | `mongodb_connections_active` | Should be stable (1-5 connections) |
| **Redis Cache Hit Rate** | `cache_hit_rate` | Should be > 70% after warmup |
| **Memory Usage** | `process_resident_memory_bytes` | Should be stable, not increasing |

**Success Criteria:**
- ✅ All panels load without errors
- ✅ Data appears within 30 seconds
- ✅ No "No Data" messages after generating traffic
- ✅ Time series charts show continuous data points
- ✅ Refresh button updates data in real-time

**Manual Verification:**
```bash
# Verify Prometheus is scraping metrics
curl -s http://localhost:9090/api/v1/query?query=http_requests_total | jq '.data.result[0].value[1]'

# Expected: Non-zero value indicating requests tracked
```

---

### Test Case 2: Data Freshness Dashboard

**Dashboard:** Data Freshness Dashboard  
**Purpose:** Monitor batch/speed layer sync and data staleness

**Test Steps:**
1. Navigate to "Data Freshness Dashboard"
2. Set time range: Last 1 hour

**What to Verify:**

| Panel | Metric | Expected Behavior |
|-------|--------|-------------------|
| **Batch Layer Last Update** | `batch_layer_last_update_timestamp` | Should be within last 24 hours |
| **Speed Layer Last Update** | `speed_layer_last_update_timestamp` | Should be within last 5 minutes |
| **Data Staleness** | `data_staleness_seconds` | Speed layer: < 300s, Batch layer: < 86400s |
| **Movies with Speed Data** | `movies_with_speed_data_count` | Should match active Reddit discussions (~5-20) |
| **Speed/Batch Coverage** | `speed_batch_coverage_ratio` | Typically 5-10% (only new movies have speed data) |

**Success Criteria:**
- ✅ Batch layer timestamp shows valid date (not "No Data")
- ✅ Speed layer timestamp updates every 5 minutes
- ✅ Data staleness chart shows two distinct lines (batch vs speed)
- ✅ Coverage ratio is reasonable (not 0% or 100%)

**Trigger Data Refresh:**
```bash
# Force a speed layer update by checking current timestamp
curl -s "http://localhost:8000/api/v1/movies/by-title/Wicked/sentiment" | jq '.data_sources.speed'

# Then refresh Grafana dashboard to see updated timestamp
```

---

### Test Case 3: Genre Analytics Dashboard

**Dashboard:** Genre Analytics Dashboard  
**Purpose:** Compare sentiment and engagement across movie genres

**Test Steps:**
1. Navigate to "Genre Analytics Dashboard"
2. Set time range: Last 7 days
3. Use genre filter dropdown to select "Action"

**What to Verify:**

| Panel | Metric | Expected Behavior |
|-------|--------|-------------------|
| **Sentiment by Genre** | `avg_sentiment_by_genre` | Bar chart showing all genres (-1.0 to 1.0 range) |
| **Reddit Engagement by Genre** | `reddit_engagement_by_genre` | Action/Sci-Fi typically have highest engagement |
| **Genre Distribution** | `movie_count_by_genre` | Pie chart showing genre proportions |
| **Top Genres by Viral Coefficient** | `viral_coefficient_by_genre` | Sorted table, top genres > 1.0 |

**Success Criteria:**
- ✅ Genre filter updates all panels simultaneously
- ✅ Bar charts use color coding (green=positive, red=negative sentiment)
- ✅ Hovering over bars shows exact values
- ✅ Pie chart percentages add up to 100%
- ✅ Table is sortable by clicking column headers

**Compare Multiple Genres:**
```bash
# Generate traffic for different genres
curl -s "http://localhost:8000/api/v1/recommendations?genre=Action&limit=10" > /dev/null
curl -s "http://localhost:8000/api/v1/recommendations?genre=Comedy&limit=10" > /dev/null
curl -s "http://localhost:8000/api/v1/recommendations?genre=Drama&limit=10" > /dev/null

# Refresh dashboard - should see metrics for all three genres
```

---

### Test Case 4: Movie Analytics Overview

**Dashboard:** Movie Analytics Overview  
**Purpose:** High-level view of movie performance metrics

**Test Steps:**
1. Navigate to "Movie Analytics Overview"
2. Set time range: Last 24 hours

**What to Verify:**

| Panel | Metric | Expected Behavior |
|-------|--------|-------------------|
| **Total Movies Tracked** | `total_movies_count` | Should match batch_views count (~311) |
| **Movies with Active Discussions** | `active_movies_count` | Should match speed_views count (~5-20) |
| **Average Sentiment (All)** | `avg_sentiment_overall` | Typically 0.0 to 0.5 (slightly positive) |
| **Top 10 Movies by Engagement** | `reddit_mentions` | Table with movie title, sentiment, mentions |
| **Sentiment Distribution** | `sentiment_histogram` | Histogram showing sentiment spread |

**Success Criteria:**
- ✅ Single-stat panels show large, readable numbers
- ✅ Top 10 table includes movie titles (not just IDs)
- ✅ Histogram shows bell curve or skewed distribution
- ✅ Clicking on a movie in the table filters other panels (if drill-down enabled)

**Verify Counts Match:**
```bash
# Check MongoDB counts
echo "Batch movies:" && docker exec serving-mongodb mongosh moviedb -u admin -p password --authenticationDatabase admin --quiet --eval "db.batch_views.countDocuments({view_type: 'movie_intelligence'})"

echo "Speed movies:" && docker exec serving-mongodb mongosh moviedb -u admin -p password --authenticationDatabase admin --quiet --eval "db.speed_views.countDocuments({view_type: 'reddit_sentiment'})"

# Compare with dashboard "Total Movies Tracked" and "Movies with Active Discussions"
```

---

### Test Case 5: Trending Movies Dashboard

**Dashboard:** Trending Movies Dashboard  
**Purpose:** Identify viral content and engagement spikes

**Test Steps:**
1. Navigate to "Trending Movies Dashboard"
2. Set time range: Last 6 hours
3. Set refresh interval to 1 minute (for real-time monitoring)

**What to Verify:**

| Panel | Metric | Expected Behavior |
|-------|--------|-------------------|
| **Viral Coefficient Over Time** | `viral_coefficient` | Time series chart showing trending movies |
| **Movies Exceeding Viral Threshold** | `viral_movies_count` | Gauge showing count of viral movies |
| **Cross-Subreddit Spread** | `cross_subreddit_spread` | Heatmap showing viral spread |
| **Engagement Velocity** | `upvote_velocity, comment_velocity` | Line charts showing velocity trends |
| **Viral Status Breakdown** | `viral_status` | Pie chart (VIRAL/TRENDING/NORMAL) |

**Success Criteria:**
- ✅ Time series charts show smooth trends (not jagged)
- ✅ Gauge indicators use color thresholds (green/yellow/red)
- ✅ Heatmap shows intensity gradients
- ✅ Auto-refresh updates charts without full page reload
- ✅ Legend shows movie titles for each time series

**Simulate Viral Event:**
```bash
# Generate burst of traffic for one movie
for i in {1..100}; do
  curl -s "http://localhost:8000/api/v1/movies/by-title/Wicked/sentiment" > /dev/null &
done
wait

# Check dashboard - should see spike in engagement velocity
```

---

### Test Case 6: Business Goal Dashboard Testing

**For each business goal, verify the corresponding metrics:**

#### Goal #1: PR Crisis Detection

**Metrics to Test:**
```bash
# Trigger crisis detection test
curl -s "http://localhost:8000/api/v1/movies/by-title/The%20Flash/sentiment" | jq .

# Check Prometheus for crisis alerts
curl -s http://localhost:9090/api/v1/query?query=crisis_alerts_total | jq .
```

**Expected Dashboard Panels:**
- Sentiment drop alerts by severity (counter)
- Movies in crisis state (table)
- Sentiment vs baseline comparison (time series)
- Crisis response time (histogram)

#### Goal #2: Viral Content Identification

**Metrics to Test:**
```bash
# Trigger viral detection
curl -s "http://localhost:8000/api/v1/trending/movies?viral_coefficient_threshold=1.0&limit=10" | jq .

# Check Prometheus for viral detections
curl -s http://localhost:9090/api/v1/query?query=viral_detections_total | jq .
```

**Expected Dashboard Panels:**
- Viral movies count by genre (bar chart)
- Viral coefficient distribution (histogram)
- Cross-subreddit spread (heatmap)
- Viral detection rate over time (time series)

#### Goal #3: Recommendation Optimization

**Metrics to Test:**
```bash
# Trigger recommendation requests
curl -s "http://localhost:8000/api/v1/recommendations?genre=Action&limit=10" | jq .

# Check Prometheus for recommendation requests
curl -s http://localhost:9090/api/v1/query?query=recommendation_requests_total | jq .
```

**Expected Dashboard Panels:**
- Dual-success score distribution (histogram)
- Recommendations by genre (stacked bar chart)
- Reddit weight vs TMDB weight contribution (pie chart)
- Recommendation response time (time series)

---

### Test Case 7: Dashboard Alerting

**Purpose:** Verify Grafana alerts trigger correctly

**Test Steps:**
1. Navigate to Alerting → Alert rules
2. Check configured alerts:
   - High API error rate (> 5%)
   - Slow API response time (P95 > 500ms)
   - Data staleness (speed layer > 10 minutes)
   - Low cache hit rate (< 50%)

**Trigger an Alert:**
```bash
# Simulate high error rate by querying non-existent movie
for i in {1..50}; do
  curl -s "http://localhost:8000/api/v1/movies/999999/sentiment" > /dev/null
done

# Check alert state in Grafana Alerting page
# Should show "Firing" state for high error rate alert
```

**Success Criteria:**
- ✅ Alert rules are visible and enabled
- ✅ Alert state changes from "Normal" to "Pending" to "Firing"
- ✅ Alert annotations appear on dashboard charts
- ✅ Notification channels are configured (email/Slack)
- ✅ Alert resolves when condition clears

---

### Test Case 8: Dashboard Variables and Filters

**Purpose:** Test dashboard interactivity

**Test Steps:**
1. Open "Genre Analytics Dashboard"
2. Test dropdown variables:
   - Genre filter (Action, Comedy, Drama, etc.)
   - Time range presets (Last 1h, 6h, 24h, 7d)
   - Movie title filter (if available)

**What to Verify:**
- ✅ Changing genre filter updates all panels
- ✅ Panels show "Loading..." spinner during refresh
- ✅ No panels break or show errors
- ✅ URL updates with selected variables (shareable links)
- ✅ "All" option shows aggregated data

**Test Multi-Select:**
```bash
# If multi-select is enabled, test selecting multiple genres
# Dashboard should show comparison across selected genres
```

---

### Test Case 9: Dashboard Performance

**Purpose:** Ensure dashboards load quickly under load

**Load Test:**
```bash
# Generate sustained traffic
for i in {1..200}; do
  curl -s "http://localhost:8000/api/v1/trending/movies?limit=20" > /dev/null &
  curl -s "http://localhost:8000/api/v1/recommendations?genre=Action&limit=20" > /dev/null &
  sleep 1
done
```

**Performance Metrics:**
- ✅ Dashboard initial load time < 3 seconds
- ✅ Panel refresh time < 1 second
- ✅ No browser console errors
- ✅ Memory usage stable (check browser DevTools)
- ✅ Prometheus query execution time < 500ms

**Check Prometheus Query Performance:**
```bash
# Access Prometheus UI at http://localhost:9090
# Go to Status → Targets
# Verify all targets are "UP" with low scrape duration (< 100ms)
```

---

### Test Case 10: Dashboard Export and Sharing

**Purpose:** Test dashboard portability

**Test Steps:**
1. Open any dashboard
2. Click Share icon (top right)
3. Test export options:
   - Export as JSON
   - Create snapshot
   - Generate shareable link

**What to Verify:**
- ✅ JSON export includes all panels and queries
- ✅ Snapshot creates public URL (if enabled)
- ✅ Shareable link preserves time range and variables
- ✅ Imported dashboard works on different Grafana instance

**Import Test:**
```bash
# Export dashboard JSON
# Import into fresh Grafana instance
# Verify all panels render correctly
```

---

### Automated Dashboard Testing

**Using Grafana API for automated checks:**

```bash
# Set Grafana credentials
GRAFANA_URL="http://localhost:3001"
GRAFANA_USER="admin"
GRAFANA_PASS="admin"

# List all dashboards
curl -s -u $GRAFANA_USER:$GRAFANA_PASS "$GRAFANA_URL/api/search?type=dash-db" | jq '.[] | {title, uid}'

# Expected output:
# {
#   "title": "System Health Dashboard",
#   "uid": "system-health"
# }
# {
#   "title": "Data Freshness Dashboard",
#   "uid": "data-freshness"
# }
# ... (5 dashboards total)

# Test specific dashboard API
curl -s -u $GRAFANA_USER:$GRAFANA_PASS "$GRAFANA_URL/api/dashboards/uid/system-health" | jq '.dashboard.title'

# Expected: "System Health Dashboard"

# Test Prometheus data source connection
curl -s -u $GRAFANA_USER:$GRAFANA_PASS "$GRAFANA_URL/api/datasources" | jq '.[] | select(.type=="prometheus") | {name, url}'

# Expected:
# {
#   "name": "Prometheus",
#   "url": "http://prometheus:9090"
# }
```

---

### Troubleshooting Dashboard Issues

#### Issue: "No Data" in all panels

**Possible Causes:**
1. Prometheus not scraping metrics
2. Data source not configured
3. Time range too narrow

**Solutions:**
```bash
# Check Prometheus targets
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job, health}'

# Verify serving-api is exposing metrics
curl -s http://localhost:8000/metrics | head -20

# Check Grafana data source
curl -s -u admin:admin http://localhost:3001/api/datasources | jq '.[] | {name, type, url}'
```

#### Issue: Dashboard panels show errors

**Check Grafana logs:**
```bash
docker logs grafana --tail 50
```

**Common errors:**
- `Template variables could not be initialized` → Check variable queries
- `Invalid PromQL query` → Verify metric names in Prometheus
- `Datasource not found` → Reconfigure data source in Grafana settings

#### Issue: Slow dashboard loading

**Optimize queries:**
```bash
# Check query execution time in Prometheus UI
# Navigate to http://localhost:9090/graph
# Paste the slow query and check execution time

# If > 1 second, consider:
# - Adding recording rules for complex queries
# - Reducing time range
# - Using rate() instead of increase() for counters
```

#### Issue: Alerts not firing

**Verify alert configuration:**
```bash
# Check alert rules
curl -s http://localhost:9090/api/v1/rules | jq '.data.groups[].rules[] | select(.type=="alerting")'

# Check alert state
curl -s http://localhost:9090/api/v1/alerts | jq '.data.alerts[] | {name, state}'
```

---

### Dashboard Testing Checklist

**Before considering dashboards production-ready:**

- [ ] All 5 existing dashboards load without errors
- [ ] Panels refresh automatically (test auto-refresh)
- [ ] Time range selector works for all presets
- [ ] Variables/filters update panels correctly
- [ ] No "N/A" or "No Data" messages (after generating traffic)
- [ ] Tooltips show on hover with correct formatting
- [ ] Legends are readable and color-coded
- [ ] Axes are labeled with units (seconds, count, percentage)
- [ ] Dashboard links work (if cross-linking exists)
- [ ] Export/import works without data loss
- [ ] Alerts trigger correctly (test one alert)
- [ ] Mobile view is readable (test on small screen)
- [ ] Performance is acceptable (load time < 3s)
- [ ] Prometheus queries are optimized (execution < 1s)
- [ ] Documentation exists for each dashboard panel

**Missing Dashboards to Create:**
- [ ] PR Crisis Detection Dashboard
- [ ] Viral Content Dashboard  
- [ ] Recommendation Performance Dashboard

---

## �🔍 Troubleshooting

### Issue: "No recent Reddit data"

**Problem:** Movie not discussed on Reddit in last 48 hours

**Solution:** Try a popular recent movie:
```bash
curl -s "http://localhost:8000/api/v1/trending/movies?limit=5" | jq '.viral_movies[0].movie_title'
# Use one of the trending movies
```

### Issue: "Movie not found in TMDB database"

**Problem:** Movie title doesn't match TMDB data

**Solution:** Check available movies:
```bash
# Query MongoDB directly
docker exec serving-mongodb mongosh moviedb -u admin -p password --authenticationDatabase admin --eval "db.batch_views.find({view_type: 'movie_intelligence'}).limit(10).forEach(d => print(d.data.title))"
```

### Issue: Empty viral_movies array

**Problem:** No movies meet viral threshold in current time window

**Solution:** Lower the threshold:
```bash
curl -s "http://localhost:8000/api/v1/trending/movies?viral_coefficient_threshold=0.5&limit=20" | jq .
```

### Issue: API returns 500 error

**Check logs:**
```bash
docker logs serving-api --tail 50
```

**Common causes:**
- MongoDB connection lost
- Redis connection lost
- Invalid data in speed_views collection

**Fix:**
```bash
# Restart services
docker restart serving-api serving-mongodb serving-redis
```

---

## ✅ Success Criteria Summary

### Goal #1: PR Crisis Detection
- ✅ Detects sentiment drops > 3σ below baseline
- ✅ Calculates sentiment velocity (rate of change)
- ✅ Compares against genre baseline
- ✅ Shows crisis severity (warning/critical)
- ✅ Response time < 100ms

### Goal #2: Viral Content Identification
- ✅ Calculates viral coefficient (velocity/threshold)
- ✅ Tracks cross-subreddit spread
- ✅ Filters by viral threshold
- ✅ Provides percentile ranking
- ✅ Response time < 100ms

### Goal #3: Content Recommendation Optimization
- ✅ Combines Reddit buzz (60%) + TMDB quality (40%)
- ✅ Genre-aware recommendations
- ✅ Supports filtering (rating, genre)
- ✅ Real-time re-ranking from speed layer
- ✅ Response time < 200ms

---

## 📚 Additional Resources

- **API Documentation:** http://localhost:8000/docs (Swagger UI)
- **Health Check:** http://localhost:8000/api/v1/health
- **Prometheus UI:** http://localhost:9090
- **Grafana Dashboards:** http://localhost:3001

---

**Last Updated:** December 15, 2025
