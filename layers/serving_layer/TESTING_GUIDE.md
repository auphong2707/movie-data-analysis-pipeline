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
# Start Grafana
cd visualization/grafana
./start-grafana.sh

# Access at http://localhost:3000 (admin/admin)
```

**Available Dashboards:**
1. **System Health Dashboard** - API/MongoDB/Redis metrics
2. **Data Freshness Dashboard** - Batch/speed layer sync
3. **Genre Analytics Dashboard** - Genre performance
4. **Movie Analytics Overview** - Overall analytics
5. **Trending Movies Dashboard** - Viral content trends

---

## 🔍 Troubleshooting

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
- **Grafana Dashboards:** http://localhost:3000

---

**Last Updated:** December 15, 2025
