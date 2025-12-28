# Serving Layer Patch Plan

**Created:** December 15, 2025  
**Status:** Draft  
**Priority:** High

---

## 📊 Executive Summary

Testing revealed that **Goal #1 (Sentiment Monitoring)** is production-ready with 100% functionality, while **Goals #2 (Viral Detection)** and **Goal #3 (Recommendations)** require patches to reach production quality. This plan outlines the specific fixes needed.

### Current Status
| Business Goal | Status | Test Results | Priority |
|--------------|--------|--------------|----------|
| Goal #1: PR Crisis Detection | ✅ Production Ready | 100% functional | Maintain |
| Goal #2: Viral Content ID | 🟡 Needs Fixes | Data present, logic broken | High |
| Goal #3: Recommendations | 🔴 Incomplete | Missing data pipeline | Medium |
| Monitoring & Metrics | 🔴 Not Implemented | Tests pass but endpoint 404 | Medium |

---

## 🎯 Goal #2: Viral Content Identification - CRITICAL FIXES

### Issue 2.1: Viral Coefficient Returns Empty Results
**Severity:** High  
**File:** `query_engine/view_merger.py`  
**Line:** 1062-1262 (merge_viral_data method)

**Problem:**
- Trending endpoint returns empty array when `viral_coefficient_threshold >= 1.0`
- Speed layer has valid data (279 documents with viral metrics)
- Movies like "Lilo & Stitch" (viral_score: 8.48) should be visible
- Issue: Line 1134-1142 tries to match speed layer `movie_title` with batch layer `title/data.title`

**Root Cause:**
```python
# Current problematic logic (Line 1097-1142)
movie_intel = self.batch_views.find_one({
    "view_type": "movie_intelligence",
    "$or": [
        {"title": movie_title_key},
        {"data.title": movie_title_key}
    ]
})
```
Speed layer has `movie_title: "Zootopia 2"` but batch layer may have `title: "Zootopia"` or use a different schema entirely.

**Fix:**
1. Add logging to track matching failures
2. Implement fuzzy title matching with edit distance (Levenshtein)
3. Add fallback to movie_id mapping collection
4. Create a title normalization utility

**Implementation Steps:**
```python
# Step 1: Add title normalization utility
# File: query_engine/utils.py (NEW FILE)

import re
from difflib import SequenceMatcher

def normalize_title(title: str) -> str:
    """Normalize movie title for matching"""
    if not title:
        return ""
    # Remove special characters, lowercase, strip whitespace
    normalized = re.sub(r'[^\w\s]', '', title.lower())
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

def fuzzy_match_title(title1: str, title2: str, threshold: float = 0.85) -> bool:
    """Check if two titles are similar enough"""
    norm1 = normalize_title(title1)
    norm2 = normalize_title(title2)
    ratio = SequenceMatcher(None, norm1, norm2).ratio()
    return ratio >= threshold

# Step 2: Update merge_viral_data in view_merger.py
# Add after line 1097:

def _find_movie_intelligence(self, movie_title_key: str) -> Optional[Dict]:
    """Find movie intelligence with fuzzy matching"""
    # Try exact match first
    movie_intel = self.batch_views.find_one({
        "view_type": "movie_intelligence",
        "$or": [
            {"title": movie_title_key},
            {"data.title": movie_title_key}
        ]
    })
    
    if movie_intel:
        return movie_intel
    
    # Try fuzzy matching
    normalized_search = normalize_title(movie_title_key)
    
    # Get all movie_intelligence docs and fuzzy match
    candidates = self.batch_views.find({"view_type": "movie_intelligence"}).limit(1000)
    
    for candidate in candidates:
        candidate_title = candidate.get("title") or candidate.get("data", {}).get("title")
        if candidate_title and fuzzy_match_title(movie_title_key, candidate_title):
            logger.info(f"Fuzzy matched '{movie_title_key}' -> '{candidate_title}'")
            return candidate
    
    logger.warning(f"No match found for movie_title: {movie_title_key}")
    return None
```

**Testing:**
```bash
# After fix, this should return results:
curl -s "http://localhost:8000/api/v1/trending/movies?viral_coefficient_threshold=0.5&limit=10" | jq '.viral_movies | length'
# Expected: > 0
```

**Files to Modify:**
- [ ] `query_engine/view_merger.py` (lines 1097-1142)
- [ ] Create `query_engine/utils.py` (new file)
- [ ] Update `requirements.txt` (add `python-Levenshtein` if needed)

**Estimated Time:** 2-3 hours

---

### Issue 2.2: NoneType Rounding Error at Threshold 0
**Severity:** Medium  
**File:** `query_engine/view_merger.py`  
**Line:** ~1230 (viral coefficient calculation)

**Problem:**
```bash
curl "http://localhost:8000/api/v1/trending/movies?viral_coefficient_threshold=0&limit=20"
# Returns: 500 Internal Server Error
# Log: "type NoneType doesn't define __round__ method"
```

**Root Cause:**
When `engagement_threshold` or viral metrics are `None`, the code tries to call `round()` on `None`.

**Fix:**
```python
# Current code (Line ~1230):
viral_coefficient = engagement_velocity / engagement_threshold if engagement_threshold > 0 else 0

# Add null safety:
viral_coefficient = (
    engagement_velocity / engagement_threshold 
    if engagement_threshold and engagement_threshold > 0 
    else 0.0
)

# Ensure all round() calls check for None:
"viral_coefficient": round(viral_coefficient, 2) if viral_coefficient is not None else 0.0,
"percentile_rank": round(percentile, 1) if percentile is not None else 0.0,
```

**Files to Modify:**
- [ ] `query_engine/view_merger.py` (lines 1210-1240)

**Estimated Time:** 30 minutes

---

### Issue 2.3: Speed Layer Schema Mismatch
**Severity:** Medium  
**File:** `query_engine/view_merger.py`

**Problem:**
Speed layer has schema:
```json
{
  "movie_title": "Avatar: Fire and Ash",
  "metrics": {
    "upvote_velocity": 0.356,
    "viral_score": 0.191
  }
}
```

But code expects:
```python
"metrics.upvotes"  # Should be "metrics.total_upvotes"
"metrics.num_comments"  # Should be "metrics.total_comments"
```

**Fix:**
Verify speed layer schema and update aggregation pipeline (lines 1104-1120):

```python
# Check actual field names in speed_views
{
    "$group": {
        "_id": "$movie_title",
        "total_upvotes": {"$sum": "$metrics.total_upvotes"},  # Verify field name
        "total_comments": {"$sum": "$metrics.total_comments"},  # Verify field name
        "total_awards": {"$sum": "$metrics.total_awards"},
        # ...
    }
}
```

**Investigation Command:**
```bash
docker exec serving-mongodb mongosh moviedb -u admin -p password --authenticationDatabase admin --quiet --eval "db.speed_views.findOne({data_type: 'reddit_post'}).metrics"
```

**Files to Modify:**
- [ ] `query_engine/view_merger.py` (lines 1104-1120)

**Estimated Time:** 1 hour

---

## 🎯 Goal #3: Recommendations - DATA PIPELINE FIXES

### Issue 3.1: Empty Recommendation Results
**Severity:** High  
**File:** `query_engine/recommendation_engine.py`

**Problem:**
All recommendation endpoints return empty:
```bash
curl "http://localhost:8000/api/v1/recommendations/genres/Action?limit=10"
# Returns: {"recommendations": [], "total": 0}
```

**Root Cause:**
Missing aggregation views in batch layer. The recommendation engine queries for:
- `view_type: 'movie_similarity'` (doesn't exist)
- `view_type: 'genre_recommendations'` (doesn't exist)
- `view_type: 'hybrid_recommendations'` (doesn't exist)

**Investigation:**
```bash
# Check what views exist:
docker exec serving-mongodb mongosh moviedb -u admin -p password --authenticationDatabase admin --quiet --eval "db.batch_views.distinct('view_type')"

# Current output: ["movie_intelligence"]
# Missing: ["movie_similarity", "genre_recommendations", "hybrid_recommendations"]
```

**Fix Options:**

#### Option A: Implement Real-Time Recommendations (No Batch Dependency)
Create recommendations directly from `movie_intelligence` views without pre-computed similarity:

```python
# File: query_engine/recommendation_engine.py
# Method: get_similar_movies_realtime()

def get_genre_recommendations_realtime(
    self, 
    genre: str, 
    limit: int = 20,
    min_rating: float = 6.0
) -> List[Dict]:
    """
    Generate recommendations in real-time from movie_intelligence
    
    No batch pre-processing required
    """
    # Query movie_intelligence directly
    pipeline = [
        {
            "$match": {
                "view_type": "movie_intelligence",
                "$or": [
                    {"genre": genre},
                    {"data.genres": genre}
                ],
                "$or": [
                    {"vote_average": {"$gte": min_rating}},
                    {"data.vote_average": {"$gte": min_rating}}
                ]
            }
        },
        {
            "$project": {
                "movie_id": 1,
                "title": {"$ifNull": ["$title", "$data.title"]},
                "vote_average": {"$ifNull": ["$vote_average", "$data.vote_average"]},
                "popularity": {"$ifNull": ["$popularity", "$data.popularity"]},
                "genres": {"$ifNull": ["$genres", "$data.genres"]}
            }
        },
        {"$sort": {"vote_average": -1, "popularity": -1}},
        {"$limit": limit}
    ]
    
    results = list(self.db.batch_views.aggregate(pipeline))
    return results
```

**Files to Modify:**
- [ ] `query_engine/recommendation_engine.py` (add real-time methods)
- [ ] `api/routes/recommendations.py` (update to use real-time methods)

**Estimated Time:** 4-6 hours

#### Option B: Add Batch Layer Jobs for Pre-computed Recommendations
Create Spark jobs to compute similarity matrices and store in batch_views.

**Files to Create:**
- [ ] `layers/batch_layer/spark_jobs/recommendation_views.py` (new)
- [ ] Update `layers/batch_layer/airflow_dags/tmdb_batch_pipeline.py`

**Estimated Time:** 8-12 hours

**Recommendation:** Start with **Option A** for quick fix, migrate to **Option B** for production scale.

---

### Issue 3.2: Empty Search Results
**Severity:** Medium  
**File:** `api/routes/search.py`

**Problem:**
```bash
curl "http://localhost:8000/api/v1/search/movies?query=Wicked"
# Returns: {"results": [], "total_results": 0}
```

**Root Cause:**
Search endpoint queries a non-existent field or uses wrong schema.

**Investigation:**
```bash
# Check search implementation
grep -n "def search_movies" layers/serving_layer/api/routes/search.py
```

**Fix:**
Update search to use correct schema and add text indexes.

**Files to Modify:**
- [ ] `api/routes/search.py`
- [ ] `mongodb/indexes.py` (add text indexes)

**Estimated Time:** 2-3 hours

---

### Issue 3.3: Genre List Returns Empty
**Severity:** Low  
**File:** `api/routes/search.py`

**Problem:**
```bash
curl "http://localhost:8000/api/v1/search/genres"
# Returns: {"genres": [], "total_genres": 0}
```

**Fix:**
```python
# Update to extract from movie_intelligence:
genres = db.batch_views.aggregate([
    {"$match": {"view_type": "movie_intelligence"}},
    {"$project": {"genres": {"$ifNull": ["$genres", "$data.genres"]}}},
    {"$unwind": "$genres"},
    {"$group": {"_id": "$genres"}},
    {"$sort": {"_id": 1}}
])
```

**Files to Modify:**
- [ ] `api/routes/search.py`

**Estimated Time:** 30 minutes

---

## 📊 Monitoring & Metrics

### Issue 4.0: Outdated/Missing Business Dashboards
**Severity:** High  
**Location:** `visualization/grafana/dashboards/`

**Problem:**
Current dashboards don't align with the three business goals. They focus on infrastructure (system health, data freshness) but lack business-critical visualizations.

**Current Dashboards:**
1. ✅ `system-health-dashboard.json` - Infrastructure metrics
2. ✅ `data-freshness-dashboard.json` - Lambda architecture monitoring
3. ✅ `genre-analytics-dashboard.json` - Genre statistics
4. ✅ `movie-analytics-overview.json` - General analytics
5. ✅ `trending-movies.json` - Basic trending view

**Missing Business-Focused Dashboards:**

#### Dashboard 1: PR Crisis Detection Dashboard
**Purpose:** Real-time monitoring for Goal #1

**Required Panels:**
- **Crisis Alert Counter** - Total crisis alerts by severity (warning/critical)
- **Sentiment Drop Timeline** - Movies with sentiment drops > 3σ below baseline
- **Genre Baseline Comparison** - Current sentiment vs genre average
- **Hourly Sentiment Breakdown** - 48-hour sentiment trend for monitored movies
- **Velocity Indicator** - Rate of sentiment change (critical if velocity < -0.5/hr)
- **Reddit vs TMDB Sentiment** - Comparison chart showing divergence

**Metrics to Query:**
```promql
# Crisis alerts by severity
rate(crisis_alerts_total[5m])

# Sentiment velocity
sentiment_velocity_gauge

# Genre baseline deviation
sentiment_baseline_deviation_gauge
```

**API Endpoints to Use (via Infinity datasource):**
- `GET /api/v1/movies/{movie_id}/sentiment` - Current sentiment
- `GET /api/v1/analytics/sentiment/comparison` - Baseline comparison
- `GET /api/v1/analytics/genre/{genre}` - Genre baseline

#### Dashboard 2: Viral Content Dashboard
**Purpose:** Track viral content for Goal #2

**Required Panels:**
- **Viral Coefficient Heatmap** - Movies by viral coefficient (color-coded)
- **Cross-Subreddit Spread** - Viral spread visualization
- **Engagement Velocity Timeline** - Upvote/comment velocity over 48h
- **Viral Status Distribution** - Pie chart (viral vs trending vs normal)
- **Top Viral Movies** - Table with viral metrics
- **Genre-Specific Viral Thresholds** - P99 thresholds by genre
- **Viral Detection Rate** - Counter of viral detections per hour

**Metrics to Query:**
```promql
# Viral detections by genre
rate(viral_detections_total{genre="Action"}[5m])

# Viral coefficient distribution
histogram_quantile(0.95, viral_coefficient_bucket)
```

**API Endpoints to Use:**
- `GET /api/v1/trending/movies?viral_coefficient_threshold=1.0` - Viral movies
- `GET /api/v1/trending/movies?viral_coefficient_threshold=0.5` - Trending movies
- `GET /api/v1/trending/genres/{genre}` - Genre-specific viral content

#### Dashboard 3: Recommendation Performance Dashboard
**Purpose:** Monitor recommendation quality for Goal #3

**Required Panels:**
- **Dual-Success Score Distribution** - Histogram of recommendation scores
- **Reddit Buzz vs TMDB Quality** - Scatter plot showing 60/40 weighting
- **Recommendation Request Rate** - Requests per minute by type
- **Genre Recommendation Balance** - Are recommendations diverse or biased?
- **Personalized vs Genre Recommendations** - Usage comparison
- **Cache Hit Rate for Recommendations** - Performance optimization metric
- **Top Recommended Movies** - Most frequently recommended titles

**Metrics to Query:**
```promql
# Recommendation requests
rate(recommendation_requests_total[5m])

# Dual-success score distribution
dual_success_score_bucket

# Cache hit rate
recommendation_cache_hit_rate
```

**API Endpoints to Use:**
- `GET /api/v1/recommendations/genres/{genre}` - Genre recommendations
- `POST /api/v1/recommendations/personalized` - Personalized recs
- `GET /api/v1/analytics/overview` - Overall recommendation stats

#### Dashboard 4: Business Goals KPI Dashboard (NEW)
**Purpose:** Executive-level view of all three goals

**Required Panels:**
- **Goal #1 Health**: Crisis alerts (last 24h), avg response time
- **Goal #2 Health**: Viral detections (last 24h), avg viral coefficient
- **Goal #3 Health**: Recommendation requests (last 24h), avg dual-success score
- **SLA Compliance**: API response times (p95, p99)
- **Data Freshness**: Speed layer lag, batch layer last update
- **Overall System Status**: Combined health indicator

**Implementation Steps:**

1. **Create new dashboard files:**
   ```bash
   # Files to create:
   - visualization/grafana/dashboards/pr-crisis-detection-dashboard.json
   - visualization/grafana/dashboards/viral-content-dashboard.json
   - visualization/grafana/dashboards/recommendation-performance-dashboard.json
   - visualization/grafana/dashboards/business-kpi-dashboard.json
   ```

2. **Update datasource configuration:**
   - Ensure Prometheus datasource is configured (currently missing)
   - Keep Infinity datasource for API queries
   - Configure both in `visualization/grafana/provisioning/datasources/`

3. **Update dashboard provisioning:**
   ```yaml
   # File: visualization/grafana/provisioning/dashboards/dashboards.yml
   - name: 'Business Goals'
     folder: 'Business Metrics'
     type: file
     options:
       path: /etc/grafana/provisioning/dashboards/business
   ```

**Files to Create:**
- [ ] `visualization/grafana/dashboards/pr-crisis-detection-dashboard.json`
- [ ] `visualization/grafana/dashboards/viral-content-dashboard.json`
- [ ] `visualization/grafana/dashboards/recommendation-performance-dashboard.json`
- [ ] `visualization/grafana/dashboards/business-kpi-dashboard.json`
- [ ] `visualization/grafana/provisioning/datasources/prometheus.yml`

**Files to Update:**
- [ ] `visualization/grafana/provisioning/dashboards/dashboards.yml`
- [ ] `TESTING_GUIDE.md` - Update dashboard list

**Estimated Time:** 12-16 hours (3-4 hours per dashboard)

---

### Issue 4.1: Missing /metrics Endpoint
**Severity:** Medium  
**File:** `api/main.py`

**Problem:**
```bash
curl "http://localhost:8000/metrics"
# Returns: 404 Not Found
```

The pytest tests pass because they mock the metrics, but the actual endpoint isn't exposed.

**Root Cause:**
Prometheus Instrumentator is initialized but `/metrics` endpoint not properly exposed.

**Fix:**
```python
# File: api/main.py (around line 160-180)

# Current:
instrumentator = Instrumentator()
instrumentator.instrument(app)

# Should be:
instrumentator = Instrumentator(
    should_group_status_codes=False,
    should_ignore_untemplated=True,
    should_respect_env_var=True,
    should_instrument_requests_inprogress=True,
    excluded_handlers=["/health"],
    inprogress_name="fastapi_inprogress",
    inprogress_labels=True
)

instrumentator.instrument(app).expose(app, endpoint="/metrics")  # Add .expose()
```

**Testing:**
```bash
curl "http://localhost:8000/metrics" | grep "crisis_alerts_total"
# Should return: # TYPE crisis_alerts_total counter
```

**Files to Modify:**
- [ ] `api/main.py` (lines ~160-180)

**Estimated Time:** 30 minutes

---

### Issue 4.2: Prometheus Targets Down
**Severity:** Medium  
**File:** `monitoring/prometheus.yml`

**Problem:**
```bash
curl "http://localhost:9090/api/v1/targets" | jq '.data.activeTargets[].health'
# Returns: "down" for fastapi, mongodb, redis
```

**Root Cause:**
Incorrect scrape configuration or network issues.

**Fix:**
```yaml
# File: monitoring/prometheus.yml
scrape_configs:
  - job_name: 'fastapi'
    scrape_interval: 15s
    static_configs:
      - targets: ['serving-api:8000']  # Use Docker service name, not localhost
    metrics_path: '/metrics'

  - job_name: 'mongodb'
    static_configs:
      - targets: ['serving-mongodb-exporter:9216']

  - job_name: 'redis'
    static_configs:
      - targets: ['serving-redis-exporter:9121']
```

**Files to Modify:**
- [ ] `monitoring/prometheus.yml`

**Estimated Time:** 1 hour

---

## 🧪 Testing Updates

### Issue 5.1: Update Testing Guide
**Severity:** Low  
**File:** `TESTING_GUIDE.md`

**Changes Needed:**
1. Update Goal #2 tests to reflect lower viral thresholds
2. Update Goal #3 to use correct endpoint paths
3. Add troubleshooting section for empty results
4. Add data pipeline prerequisites section

**Files to Modify:**
- [ ] `TESTING_GUIDE.md`

**Estimated Time:** 1 hour

---

## 📅 Implementation Roadmap

### Phase 1: Critical Fixes (Week 1)
**Goal:** Restore Goal #2 functionality

| Task | Priority | Time | Assignee |
|------|----------|------|----------|
| Fix viral coefficient matching (2.1) | P0 | 3h | - |
| Fix NoneType errors (2.2) | P0 | 0.5h | - |
| Verify speed layer schema (2.3) | P0 | 1h | - |
| Add /metrics endpoint (4.1) | P1 | 0.5h | - |

**Total:** 5 hours

### Phase 2: Recommendation Fixes (Week 2)
**Goal:** Enable Goal #3 basic functionality

| Task | Priority | Time | Assignee |
|------|----------|------|----------|
| Implement real-time recommendations (3.1 Option A) | P1 | 6h | - |
| Fix search endpoints (3.2) | P1 | 3h | - |
| Fix genre listing (3.3) | P2 | 0.5h | - |
| Update testing guide (5.1) | P2 | 1h | - |

**Total:** 10.5 hours

### Phase 3: Monitoring & Dashboards (Week 3)
**Goal:** Production-ready monitoring with business-focused dashboards

| Task | Priority | Time | Assignee |
|------|----------|------|----------|
| Create PR Crisis Detection Dashboard (4.0.1) | P1 | 4h | - |
| Create Viral Content Dashboard (4.0.2) | P1 | 4h | - |
| Create Recommendation Performance Dashboard (4.0.3) | P1 | 4h | - |
| Create Business KPI Dashboard (4.0.4) | P1 | 3h | - |
| Fix Prometheus datasource config (4.0.5) | P1 | 1h | - |
| Fix Prometheus targets (4.2) | P1 | 1h | - |
| Update dashboard provisioning (4.0.6) | P2 | 1h | - |

**Total:** 18 hours

### Phase 4: Optimization & Scale (Week 4)
**Goal:** Production scale and performance

| Task | Priority | Time | Assignee |
|------|----------|------|----------|
| Add batch recommendation jobs (3.1 Option B) | P2 | 12h | - |
| Performance testing | P2 | 4h | - |
| Load testing dashboards | P2 | 2h | - |
| Documentation update | P2 | 2h | - |

**Total:** 20 hours

---

## 🔍 Verification Checklist

After implementing fixes, verify:

### Goal #1: PR Crisis Detection ✅
- [x] Old movies return batch-only data
- [x] New movies return batch + speed data
- [x] Sentiment breakdowns are sorted newest first
- [x] Response time < 100ms

### Goal #2: Viral Content Identification
- [ ] Trending endpoint returns movies with viral_coefficient >= 1.0
- [ ] No NoneType errors at threshold 0
- [ ] Cross-subreddit spread is calculated
- [ ] Genre filtering works
- [ ] Response time < 100ms

### Goal #3: Recommendations
- [ ] Genre recommendations return results
- [ ] Personalized recommendations work with liked movies
- [ ] Search returns movie results
- [ ] Genre list is populated
- [ ] Response time < 200ms

### Monitoring
- [ ] `/metrics` endpoint returns Prometheus metrics
- [ ] Prometheus scrapes API successfully (target "up")
- [ ] MongoDB exporter target is "up"
- [ ] Redis exporter target is "up"
- [ ] Custom business metrics visible in Prometheus

### Business Dashboards
- [ ] PR Crisis Detection Dashboard shows crisis alerts and sentiment drops
- [ ] Viral Content Dashboard displays viral coefficients and engagement velocity
- [ ] Recommendation Performance Dashboard tracks dual-success scores
- [ ] Business KPI Dashboard provides executive overview of all three goals
- [ ] All dashboards load without errors
- [ ] Dashboards update in real-time (refresh interval < 30s)

---

## 📚 Additional Resources

### Debugging Commands

```bash
# Check speed layer schema
docker exec serving-mongodb mongosh moviedb -u admin -p password --authenticationDatabase admin --quiet --eval "db.speed_views.findOne()"

# Check batch_views types
docker exec serving-mongodb mongosh moviedb -u admin -p password --authenticationDatabase admin --quiet --eval "db.batch_views.distinct('view_type')"

# Check API logs
docker logs serving-api --tail 100 -f

# Test viral endpoint with debug
curl -v "http://localhost:8000/api/v1/trending/movies?viral_coefficient_threshold=0.1&limit=5" 2>&1 | grep -A 10 "viral_movies"

# Check Prometheus targets
curl -s "http://localhost:9090/api/v1/targets" | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'
```

### Schema Verification

```bash
# Verify movie_intelligence schema
docker exec serving-mongodb mongosh moviedb -u admin -p password --authenticationDatabase admin --quiet --eval "db.batch_views.findOne({view_type: 'movie_intelligence'})" | head -50

# Verify speed_views schema
docker exec serving-mongodb mongosh moviedb -u admin -p password --authenticationDatabase admin --quiet --eval "db.speed_views.findOne({data_type: 'reddit_post'})" | head -50
```

---

## 🎯 Success Criteria

This patch plan is complete when:

**Functionality:**
1. ✅ All 35 pytest tests pass (currently: 33/35)
2. ✅ Goal #2 trending endpoint returns viral movies
3. ✅ Goal #3 recommendations return at least basic results
4. ✅ No 500 errors in production usage
5. ✅ Response times meet SLA (< 200ms for 95th percentile)

**Monitoring:**
6. ✅ `/metrics` endpoint exposes Prometheus metrics
7. ✅ All Prometheus targets show "up" status
8. ✅ Custom business metrics (crisis_alerts, viral_detections, recommendation_requests) are tracked

**Dashboards:**
9. ✅ PR Crisis Detection Dashboard visualizes Goal #1 metrics
10. ✅ Viral Content Dashboard visualizes Goal #2 metrics
11. ✅ Recommendation Performance Dashboard visualizes Goal #3 metrics
12. ✅ Business KPI Dashboard provides executive overview
13. ✅ All dashboards load without errors and update in real-time

**Documentation:**
14. ✅ Testing guide accurately reflects API behavior
15. ✅ Dashboard usage documented in TESTING_GUIDE.md
16. ✅ All missing dashboards created and provisioned

---

**Last Updated:** December 15, 2025  
**Next Review:** After Phase 1 completion
