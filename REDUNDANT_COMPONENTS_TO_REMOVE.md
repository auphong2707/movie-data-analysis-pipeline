# Redundant Components to Remove from Serving Layer

**Purpose**: This document identifies all serving layer components that are NOT aligned with the Reddit + TMDB business goals (PR Crisis Detection, Viral Content Identification, Content Recommendation Optimization).

**Business Scope**: The pipeline is specifically designed for:
- **Speed Layer**: Reddit discussions (last 48 hours) - posts, comments, upvotes, sentiment
- **Batch Layer**: TMDB baselines - sentiment baselines, viral thresholds, movie intelligence
- **NO**: TMDB popularity tracking, vote count velocity, revenue/budget analysis, runtime analytics

**Data Reality Check**:
- ✅ **EXISTS**: `sentiment_baseline`, `viral_threshold`, `movie_intelligence` view types in batch_views
- ✅ **EXISTS**: `reddit_post`, `reddit_comment` data types in speed_views with `metrics` object
- ❌ **DOES NOT EXIST**: TMDB popularity time-series, vote count velocity, rating velocity, revenue/budget data

---

## 🗑️ COMPONENTS TO REMOVE

### 1. **Prediction Engine Module** (❌ COMPLETELY REDUNDANT)

**File**: `/layers/serving_layer/query_engine/prediction_engine.py` (338 lines)

**Reason for Removal**: 
- Predicts TMDB popularity/vote count/rating trends using time-series analysis
- **Problem**: We don't collect TMDB popularity velocity data in batch or speed layers
- **Business Misalignment**: Business goals are about Reddit viral detection, not TMDB popularity forecasting
- **Data Gap**: No hourly TMDB stats in speed_views (only Reddit metrics)

**Methods to Remove**:
- `predict_trend()` - Analyzes TMDB popularity velocity/acceleration
- `forecast_popularity()` - Forecasts TMDB popularity score
- `predict_genre_demand()` - Predicts genre demand based on popularity
- `_calculate_velocity()` - Time-series velocity calculation
- `_calculate_acceleration()` - Time-series acceleration calculation

**Impact**: 
- Removes 338 lines of unused code
- Eliminates dependency on non-existent TMDB velocity data
- Saves maintenance burden

---

### 2. **Prediction API Endpoints** (❌ COMPLETELY REDUNDANT)

**File**: `/layers/serving_layer/api/routes/predictions.py` (235 lines)

**Endpoints to Remove**:
```python
GET /predictions/movies/{movie_id}/trend
GET /predictions/movies/{movie_id}/popularity  
GET /predictions/genre/{genre}/demand
```

**Reason for Removal**:
- All three endpoints depend on PredictionEngine which uses non-existent TMDB velocity data
- Business goals focus on Reddit viral detection, not TMDB popularity forecasting
- No speed layer data to support these predictions

**Impact**:
- Removes 235 lines of endpoint code
- Simplifies API surface area
- Eliminates router registration in main.py

---

### 3. **Data Aggregator Module** (⚠️ PARTIALLY REDUNDANT)

**File**: `/layers/serving_layer/query_engine/aggregator.py` (421 lines)

**Methods to Remove**:

#### 3.1 `aggregate_genre_stats()` - ❌ REMOVE
```python
def aggregate_genre_stats(self, genre: str, year: Optional[int], month: Optional[int])
```
**Reason**: 
- Aggregates revenue, budget, runtime from batch_views
- **Data Gap**: batch_views only contains sentiment_baseline, viral_threshold, movie_intelligence (no revenue/budget/runtime)
- Business goals don't require financial or runtime analytics

#### 3.2 `aggregate_temporal_trends()` - ❌ REMOVE
```python
def aggregate_temporal_trends(self, movie_id, genre, metric, window_days)
```
**Reason**:
- Tracks rating/popularity changes over time
- **Data Gap**: No temporal TMDB data in batch_views (all_time aggregations only)
- Business focuses on Reddit real-time trends, not TMDB historical trends

#### 3.3 `calculate_cross_metric_correlation()` - ❌ REMOVE
```python
def calculate_cross_metric_correlation(self, metric_x, metric_y, genre)
```
**Reason**:
- Analyzes correlation between TMDB metrics (rating vs popularity, etc.)
- Not aligned with Reddit viral detection business goals

#### 3.4 `get_top_movers()` - ❌ REMOVE
```python
def get_top_movers(self, metric, hours, limit)
```
**Reason**:
- Identifies movies with highest velocity changes in TMDB metrics
- **Data Gap**: No hourly TMDB velocity data in speed_views

**Impact**:
- Removes ~300 lines of aggregation logic
- Keeps only methods that work with actual batch_views schema

---

### 4. **Analytics API Endpoints** (⚠️ PARTIALLY REDUNDANT)

**File**: `/layers/serving_layer/api/routes/analytics.py` (395 lines)

**Endpoints to Remove**:

#### 4.1 `/analytics/genre/{genre}` - ⚠️ MODIFY (Not Remove)
```python
GET /analytics/genre/{genre}?year=2024&month=1
```
**Current State**: Returns revenue, budget, runtime aggregations from DataAggregator
**Issue**: DataAggregator methods assume data that doesn't exist
**Action**: MODIFY to return only sentiment_baseline and viral_threshold data from batch_views
**Keep**: The endpoint itself (needed for business goal #1)
**Remove**: Calls to `aggregate_genre_stats()` and replace with direct batch_views queries

#### 4.2 `/analytics/trends` - ❌ REMOVE
```python
GET /analytics/trends?movie_id=123&genre=Action&metric=popularity&window=30d
```
**Reason**: 
- Returns temporal trends for TMDB metrics (rating, popularity)
- **Data Gap**: No temporal TMDB data in batch_views
- Business uses Reddit velocity (speed layer), not TMDB temporal trends

#### 4.3 `/analytics/correlation` - ❌ REMOVE
```python
GET /analytics/correlation?genre=Action&metric_x=rating&metric_y=popularity
```
**Reason**:
- Analyzes correlations between TMDB metrics
- Not aligned with Reddit viral detection business goals

#### 4.4 `/analytics/top-movers` - ❌ REMOVE
```python
GET /analytics/top-movers?metric=popularity&hours=24
```
**Reason**:
- Tracks TMDB popularity velocity changes
- **Data Gap**: No hourly TMDB stats in speed_views

**Impact**:
- Removes 3 endpoints completely
- Modifies 1 endpoint to align with actual data schema

---

### 5. **Search API Endpoints** (⚠️ REVIEW NEEDED)

**File**: `/layers/serving_layer/api/routes/search.py` (200 lines)

**Endpoints to Review**:

#### 5.1 `/search/movies` - ⚠️ REVIEW
```python
GET /search/movies?q=Dune&genre=Sci-Fi&rating_min=7.0&sort_by=popularity
```
**Current State**: Searches with rating, year, genre filters
**Issue**: 
- `sort_by=popularity` assumes TMDB popularity tracking (which we don't have)
- `rating_min/max` filters work with batch_views movie_intelligence data
**Action**: 
- KEEP: Text search, genre filter, year range
- REMOVE: `sort_by=popularity` option (replace with `sort_by=sentiment` or `sort_by=tmdb_rating`)
- MODIFY: Query logic to use actual movie_intelligence schema

#### 5.2 `/search/similar/{movie_id}` - ⚠️ REVIEW
```python
GET /search/similar/123?limit=10
```
**Current State**: Finds similar movies based on genre/keywords
**Business Alignment**: Could be useful for recommendation engine
**Action**: KEEP if it works with movie_intelligence schema, REMOVE if it assumes popularity tracking

**Impact**:
- Modify search to align with actual data sources
- Remove unsupported sort options

---

### 6. **View Merger Methods** (⚠️ PARTIALLY REDUNDANT)

**File**: `/layers/serving_layer/query_engine/view_merger.py` (792 lines)

**Methods to Review**:

#### 6.1 `get_temporal_trends()` - ❌ REMOVE
```python
def get_temporal_trends(self, metric, movie_id, genre, window)
```
**Reason**:
- Returns time-series trends for TMDB metrics
- **Data Gap**: No temporal TMDB data in batch_views
- Called by `/analytics/trends` endpoint (which we're removing)

#### 6.2 `merge_popularity_data()` - ❌ REMOVE (if exists)
```python
def merge_popularity_data(self, movie_id)
```
**Reason**:
- Merges TMDB popularity time-series
- **Data Gap**: No popularity velocity tracking in speed_views

#### 6.3 `merge_analytics_views()` - ⚠️ MODIFY
```python
def merge_analytics_views(self, genre, year, month)
```
**Current State**: May call DataAggregator methods that don't work
**Action**: MODIFY to query batch_views directly for sentiment_baseline and viral_threshold

**Impact**:
- Remove methods that query non-existent data
- Simplify view merger to focus on actual business goals

---

### 7. **MongoDB Query Builders** (⚠️ PARTIALLY REDUNDANT)

**File**: `/layers/serving_layer/mongodb/queries.py` (315 lines)

**Methods to Review**:

#### 7.1 `get_batch_temporal_trends()` - ❌ REMOVE
```python
def get_batch_temporal_trends(self, metric, movie_id, genre, window_days)
```
**Reason**:
- Queries temporal TMDB trends from batch_views
- **Data Gap**: batch_views has all_time aggregations only (no daily/weekly/monthly time-series)

#### 7.2 Sorting by `popularity` - ⚠️ MODIFY
```python
sort_map = {
    'popularity': ('avg_popularity', DESCENDING),
    'rating': ('data.vote_average', DESCENDING)
}
```
**Issue**: `avg_popularity` field doesn't exist in actual batch_views schema
**Action**: Remove popularity sorting, use only sentiment/rating sorting

**Impact**:
- Remove query methods that assume wrong schema
- Update sort logic to match actual data

---

### 8. **Trending Endpoints - Popularity References** (⚠️ MODIFY)

**File**: `/layers/serving_layer/api/routes/trending.py`

**Issues to Fix**:

#### 8.1 TMDB Popularity Sorting
```python
# Current code assumes TMDB popularity field
movies = sorted(movies, key=lambda x: x.get('popularity', 0), reverse=True)
```
**Problem**: We don't track TMDB popularity velocity
**Action**: Replace with Reddit viral metrics (upvote velocity, comment velocity)

#### 8.2 Endpoint Response Fields
```python
return {
    'popularity': round(movie['popularity'], 2),  # ❌ TMDB field we don't track
    'vote_average': round(movie['vote_average'], 2)  # ✅ OK from movie_intelligence
}
```
**Action**: Remove `popularity` field, add Reddit viral metrics

**Impact**:
- Change trending logic from TMDB popularity to Reddit viral coefficients
- Align with business goal #2 (Viral Content Identification)

---

### 9. **Grafana Dashboards** (⚠️ REVIEW NEEDED)

**Files**: `/layers/serving_layer/visualization/grafana/dashboards/*.json` (89KB total)

**Dashboards to Review**:

#### 9.1 `system-health-dashboard.json` (17KB)
**Status**: ✅ KEEP (monitors API/MongoDB/Redis health)

#### 9.2 `data-freshness-dashboard.json` (17KB)
**Review Needed**: Check if it queries TMDB popularity fields
**Action**: Modify panels to query Reddit metrics, not TMDB popularity

#### 9.3 `genre-analytics-dashboard.json` (21KB)
**Review Needed**: May query revenue/budget/runtime fields that don't exist
**Action**: Modify to show only sentiment_baseline and viral_threshold data

#### 9.4 `movie-analytics-overview-dashboard.json` (22KB)
**Review Needed**: Likely queries TMDB popularity/vote velocity
**Action**: Replace with Reddit viral metrics (upvote velocity, comment velocity)

#### 9.5 `trending-movies-dashboard.json` (12KB)
**Review Needed**: May sort by TMDB popularity
**Action**: Replace with Reddit viral coefficient sorting

**Impact**:
- Modify 4 dashboards to query actual data schema
- Keep system-health dashboard as-is

---

## 📊 REMOVAL IMPACT SUMMARY

| Component | Lines of Code | Action | Business Impact |
|-----------|--------------|--------|-----------------|
| `prediction_engine.py` | 338 | ❌ DELETE | Removes TMDB forecasting (not a business goal) |
| `routes/predictions.py` | 235 | ❌ DELETE | Removes 3 prediction endpoints |
| `aggregator.py` | ~300 (partial) | ⚠️ REFACTOR | Remove methods querying non-existent data |
| `routes/analytics.py` | ~200 (partial) | ⚠️ REFACTOR | Remove 3 endpoints, modify 1 |
| `routes/search.py` | ~50 (partial) | ⚠️ MODIFY | Remove popularity sorting |
| `routes/trending.py` | ~50 (partial) | ⚠️ MODIFY | Replace TMDB popularity with Reddit metrics |
| `view_merger.py` | ~100 (partial) | ⚠️ REFACTOR | Remove temporal trend methods |
| `queries.py` | ~50 (partial) | ⚠️ MODIFY | Remove temporal queries, fix sorting |
| Grafana Dashboards | 4 dashboards | ⚠️ MODIFY | Update to query actual schema |
| **TOTAL** | **~1,300 lines** | **Mix** | **Aligns serving layer with actual business goals and data reality** |

---

## ✅ WHAT TO KEEP

### Core Business Logic (Aligned with Goals)

1. **ViewMerger Core Methods** ✅
   - `merge_sentiment_data()` - Business Goal #1 (PR Crisis Detection)
   - `merge_viral_data()` - Business Goal #2 (Viral Content Identification) 
   - `merge_movie_views()` - Base merging logic
   - `get_cutoff_time()` - 48-hour Lambda Architecture cutoff

2. **API Endpoints** ✅
   - `GET /movies/{movie_id}` - Movie details with merged sentiment
   - `GET /movies/{movie_id}/sentiment` - Crisis detection endpoint
   - `GET /trending/movies` - Viral content identification (after modifying to use Reddit metrics)
   - `GET /recommendations/similar/{movie_id}` - Content recommendation
   - `GET /analytics/genre/{genre}` - Genre baseline analytics (after modification)
   - `GET /health` - Health checks

3. **Infrastructure** ✅
   - MongoDB client with connection pooling
   - Redis cache manager with TTL strategies
   - CORS and rate limiting middleware
   - Dependency injection pattern
   - Logging and error handling

4. **Recommendation Engine** ✅
   - Content-based filtering with genre/keywords
   - Needs enhancement to add Reddit buzz integration (60% weight)

---

## 🔧 REFACTORING ACTIONS

### Phase 1: Remove Completely Redundant Components (3-4 hours)

1. **Delete Files**:
   ```bash
   rm layers/serving_layer/query_engine/prediction_engine.py
   rm layers/serving_layer/api/routes/predictions.py
   ```

2. **Update main.py**:
   - Remove `predictions` router registration
   ```python
   # DELETE THIS LINE:
   app.include_router(predictions.router, prefix="/api/v1")
   ```

3. **Remove Tests**:
   ```bash
   # Find and remove prediction-related tests
   grep -l "prediction" layers/serving_layer/tests/*.py
   ```

### Phase 2: Refactor aggregator.py (4-5 hours)

1. **Remove Methods**:
   - `aggregate_genre_stats()`
   - `aggregate_temporal_trends()`
   - `calculate_cross_metric_correlation()`
   - `get_top_movers()`

2. **Add New Methods**:
   - `get_sentiment_baseline(genre: str)` - Query batch_views for sentiment_baseline
   - `get_viral_threshold(genre: str, budget_tier: str)` - Query batch_views for viral_threshold
   - `get_movie_intelligence(movie_id: int)` - Query batch_views for movie_intelligence

### Phase 3: Refactor Analytics Endpoints (4-5 hours)

1. **Remove Endpoints**:
   - `GET /analytics/trends`
   - `GET /analytics/correlation`
   - `GET /analytics/top-movers`

2. **Modify `/analytics/genre/{genre}`**:
   ```python
   # OLD: Called aggregate_genre_stats() with revenue/budget
   # NEW: Query batch_views directly for sentiment_baseline + viral_threshold
   
   sentiment_baseline = db.batch_views.find_one({
       'view_type': 'sentiment_baseline',
       'genre': genre
   })
   
   viral_threshold = db.batch_views.find_one({
       'view_type': 'viral_threshold',
       'genre': genre
   })
   ```

### Phase 4: Fix Search and Trending Endpoints (3-4 hours)

1. **Modify `/search/movies`**:
   - Remove `sort_by=popularity` option
   - Add `sort_by=sentiment` and `sort_by=reddit_buzz`
   - Update query logic to use movie_intelligence schema

2. **Modify `/trending/movies`**:
   - Replace TMDB popularity sorting with Reddit viral coefficient
   - Query speed_views for upvote/comment velocity
   - Calculate viral coefficient using viral_threshold from batch_views

### Phase 5: Update Grafana Dashboards (4-5 hours)

1. **Modify Each Dashboard**:
   - Replace popularity queries with Reddit viral metric queries
   - Remove revenue/budget/runtime panels
   - Add sentiment baseline comparison panels
   - Add viral coefficient visualization

2. **Test Dashboards**:
   - Start Grafana: `./start-grafana.sh`
   - Verify each panel queries actual API endpoints
   - Check for empty/error panels

### Phase 6: Update ViewMerger (3-4 hours)

1. **Remove Methods**:
   - `get_temporal_trends()`
   - `merge_popularity_data()` (if exists)

2. **Modify Methods**:
   - `merge_analytics_views()` - Query batch_views directly, remove DataAggregator calls

3. **Add Business Logic** (from SERVING_LAYER_REVISION_PLAN.md):
   - Crisis detection with 3-sigma threshold
   - Viral coefficient calculation
   - Sentiment velocity tracking

### Phase 7: Update MongoDB Queries (2-3 hours)

1. **Remove Methods**:
   - `get_batch_temporal_trends()`

2. **Fix Sorting**:
   ```python
   # OLD:
   sort_map = {
       'popularity': ('avg_popularity', DESCENDING),
       'rating': ('data.vote_average', DESCENDING)
   }
   
   # NEW:
   sort_map = {
       'sentiment': ('avg_sentiment', DESCENDING),
       'rating': ('data.vote_average', DESCENDING),
       'viral_score': ('viral_coefficient', DESCENDING)
   }
   ```

---

## 🎯 BUSINESS ALIGNMENT VERIFICATION

After refactoring, the serving layer will:

### ✅ Business Goal #1: PR Crisis Detection
- **Endpoint**: `GET /movies/{movie_id}/sentiment`
- **Data Flow**: Speed layer Reddit sentiment → ViewMerger → Compare to batch_views sentiment_baseline → Detect 3-sigma drops
- **Dependencies**: ✅ sentiment_baseline in batch_views, ✅ Reddit metrics in speed_views

### ✅ Business Goal #2: Viral Content Identification
- **Endpoint**: `GET /trending/movies`
- **Data Flow**: Speed layer upvote/comment velocity → ViewMerger → Compare to batch_views viral_threshold → Calculate viral coefficient
- **Dependencies**: ✅ viral_threshold in batch_views, ✅ Reddit metrics in speed_views

### ✅ Business Goal #3: Content Recommendation Optimization
- **Endpoint**: `GET /recommendations/similar/{movie_id}`
- **Data Flow**: Recommendation engine → 60% Reddit buzz + 40% TMDB quality → Rank by dual success score
- **Dependencies**: ✅ movie_intelligence in batch_views, ✅ Reddit metrics in speed_views

---

## 📋 VALIDATION CHECKLIST

After removing redundant components:

- [ ] All endpoints query actual batch_views schema (sentiment_baseline, viral_threshold, movie_intelligence)
- [ ] No code references TMDB popularity velocity or time-series data
- [ ] No code references revenue/budget/runtime analytics
- [ ] Trending endpoint uses Reddit viral coefficient, not TMDB popularity
- [ ] Search endpoint removed unsupported sort options
- [ ] Grafana dashboards query actual API endpoints successfully
- [ ] All unit tests pass (remove tests for deleted components)
- [ ] API documentation updated to reflect removed endpoints
- [ ] SERVING_LAYER_REVISION_PLAN.md updated to match new implementation

---

## 🚀 ESTIMATED EFFORT

| Phase | Component | Hours | Priority |
|-------|-----------|-------|----------|
| 1 | Delete prediction engine + endpoints | 3-4 | P0 (High) |
| 2 | Refactor aggregator.py | 4-5 | P0 (High) |
| 3 | Refactor analytics endpoints | 4-5 | P0 (High) |
| 4 | Fix search/trending endpoints | 3-4 | P1 (Medium) |
| 5 | Update Grafana dashboards | 4-5 | P1 (Medium) |
| 6 | Update ViewMerger | 3-4 | P0 (High) |
| 7 | Update MongoDB queries | 2-3 | P1 (Medium) |
| **TOTAL** | **All Phases** | **24-30 hours** | **~1 week sprint** |

**Recommendation**: 
1. Start with Phase 1-3 (delete + refactor core logic) - 11-14 hours
2. Then Phase 6 (ViewMerger business logic) - 3-4 hours
3. Finally Phase 4-5-7 (endpoints + dashboards) - 9-12 hours

This approach ensures the core serving layer aligns with business goals before polishing endpoints and visualizations.

---

## 📝 NOTES

1. **Batch Layer Status**: batch_views contains ONLY sentiment_baseline, viral_threshold, movie_intelligence (no TMDB temporal data)
2. **Speed Layer Status**: speed_views contains ONLY Reddit metrics (no TMDB popularity velocity)
3. **Business Focus**: Reddit viral detection + TMDB baselines, NOT TMDB popularity forecasting
4. **Schema Validation**: Run `docker exec -it serving-mongodb mongosh` to verify actual data structure before refactoring
5. **Testing Strategy**: After each phase, test with actual MongoDB data to ensure queries work

---

**Created**: 2025-12-14  
**Last Updated**: 2025-12-14  
**Status**: Ready for implementation  
**Related Documents**: SERVING_LAYER_REVISION_PLAN.md, README.md, ARCHITECTURE.md
