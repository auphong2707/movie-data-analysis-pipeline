# Serving Layer Architecture

## 🏗️ Overview

The Serving Layer implements the **Query** component of Lambda Architecture, providing a unified interface that merges:
- **Batch Layer** views (historical, accurate data >48 hours)
- **Speed Layer** views (recent, real-time data ≤48 hours)

## 🎯 Key Principles

### 1. 48-Hour Cutoff Strategy

```
Timeline:
├─────────────┬──────────────────┤
   Batch       │   Speed          NOW
   Views       │   Views
   (>48h)      │   (≤48h)
            Cutoff
            Point
```

**Logic:**
- Queries for data **older than 48 hours** → Batch layer
- Queries for data **within 48 hours** → Speed layer
- On overlap → Speed layer takes precedence (fresher)

**Why 48 hours?**
- Speed layer data becomes available within minutes
- Batch layer processes data with 24-48 hour delay
- 48-hour buffer ensures no data gaps
- Balances accuracy (batch) with freshness (speed)

### 2. View Merger Algorithm

```python
def merge_views(movie_id, time_range):
    cutoff = now - 48_hours
    
    # Query both layers
    batch_data = batch_views.find({
        "movie_id": movie_id,
        "computed_at": {"$lt": cutoff}
    })
    
    speed_data = speed_views.find({
        "movie_id": movie_id,
        "hour": {"$gte": cutoff}
    })
    
    # Merge with speed taking precedence
    return merge_and_deduplicate(batch_data, speed_data)
```

### 3. Caching Strategy

```
Request → Cache Check → Hit? → Return
              ↓
            Miss
              ↓
      Query MongoDB → Merge Views → Cache → Return
```

**Cache TTLs:**
- Volatile data (trending, sentiment): 3-5 minutes
- Stable data (analytics): 30-60 minutes
- Very stable (historical stats): 1+ hour

## 📊 Data Flow

```
┌─────────────────────────────────────────────────────┐
│              External Data Sources                   │
│  (TMDB API, Kafka Streams, User Reviews)            │
└────────────┬────────────────────────────────────────┘
             │
    ┌────────┴────────┐
    ↓                 ↓
┌───────────┐    ┌──────────┐
│  Batch    │    │  Speed   │
│  Layer    │    │  Layer   │
│ (Airflow) │    │ (Spark   │
│           │    │ Streaming)│
└─────┬─────┘    └────┬─────┘
      │               │
      ↓               ↓
┌──────────┐    ┌──────────┐
│ batch_   │    │ speed_   │
│ views    │    │ views    │
│(MongoDB) │    │(MongoDB) │
└─────┬────┘    └────┬─────┘
      │              │
      └──────┬───────┘
             ↓
    ┌─────────────────┐
    │  View Merger    │
    │  (Query Engine) │
    └────────┬────────┘
             ↓
    ┌─────────────────┐
    │   Redis Cache   │
    └────────┬────────┘
             ↓
    ┌─────────────────┐
    │  FastAPI REST   │
    │      API        │
    └────────┬────────┘
             │
    ┌────────┴────────┐
    ↓                 ↓
┌──────────┐    ┌──────────┐
│ Superset │    │ Grafana  │
│(Business)│    │(Monitoring)│
└──────────┘    └──────────┘
```

## 🗄️ Data Models

### Batch Views Schema

```javascript
{
  "_id": ObjectId("..."),
  "movie_id": 12345,
  "view_type": "genre_analytics",
  "data": {
    "genre": "Action",
    "year": 2025,
    "month": 10,
    "avg_rating": 7.5,
    "total_movies": 150,
    "total_revenue": 5000000000
  },
  "computed_at": ISODate("2025-10-17T10:00:00Z"),
  "batch_run_id": "batch_2025_10_17_10",
  "version": 1
}
```

**View Types:**
- `genre_analytics` - Genre-level statistics
- `temporal_analytics` - Time-series data
- `movie_details` - Complete movie information
- `actor_collaboration` - Actor network data

### Speed Views Schema

```javascript
{
  "_id": ObjectId("..."),
  "movie_id": 12345,
  "data_type": "sentiment",
  "hour": ISODate("2025-10-17T14:00:00Z"),
  "data": {
    "avg_sentiment": 0.75,
    "review_count": 45,
    "positive_count": 30,
    "negative_count": 10,
    "neutral_count": 5,
    "sentiment_velocity": 0.02
  },
  "synced_at": ISODate("2025-10-17T14:05:00Z"),
  "ttl_expires_at": ISODate("2025-10-19T14:00:00Z")
}
```

**Data Types:**
- `sentiment` - Sentiment analysis results
- `stats` - Statistical aggregations
- `trending` - Trending scores and velocity

## 🔍 Query Patterns

### Pattern 1: Movie Details (Merged)

```python
# Get complete movie view
GET /api/v1/movies/{movie_id}

# Backend logic:
1. Check cache
2. Query batch_views for historical data
3. Query speed_views for recent updates
4. Merge results (speed overrides batch)
5. Cache merged result
6. Return to client
```

### Pattern 2: Trending Movies (Speed Only)

```python
# Get current trending
GET /api/v1/trending/movies?window=6h

# Backend logic:
1. Check cache (5 min TTL)
2. Query speed_views only (recent data)
3. Calculate trending scores
4. Sort by velocity and popularity
5. Cache result
6. Return top N
```

### Pattern 3: Historical Analytics (Batch Only)

```python
# Get historical genre stats
GET /api/v1/analytics/genre/Action?year=2023

# Backend logic:
1. Check cache (30 min TTL)
2. Query batch_views only (historical)
3. Aggregate statistics
4. Cache result
5. Return analytics
```

### Pattern 4: Sentiment Timeline (Merged)

```python
# Get sentiment over time
GET /api/v1/movies/{id}/sentiment?window=30d

# Backend logic:
1. Check cache (3 min TTL)
2. Query batch_views (>48h ago)
3. Query speed_views (≤48h ago)
4. Merge timelines
5. Calculate trends
6. Cache result
7. Return time series
```

## ⚡ Performance Characteristics

### Latency Targets

| Operation | Target (p95) | Strategy |
|-----------|--------------|----------|
| Health Check | <50ms | Cached |
| Movie Details | <100ms | Cached + Indexed |
| Trending | <100ms | Cached + Speed Layer |
| Search | <200ms | Indexed + Paginated |
| Analytics | <500ms | Cached + Pre-aggregated |

### Throughput

- **Target**: 100+ req/s per worker
- **Scaling**: Horizontal (add workers)
- **Bottleneck**: MongoDB query time

### Cache Hit Rates

- **Target**: >70% cache hit rate
- **Monitoring**: Grafana dashboard
- **Tuning**: Adjust TTLs based on volatility

## 🔧 Components

### 1. Query Engine (`query_engine/`)

**view_merger.py:**
- Implements 48-hour cutoff logic
- Merges batch and speed views
- Deduplicates overlapping data

**query_router.py:**
- Routes queries to appropriate layer
- Optimizes query execution
- Handles aggregations

**aggregator.py:**
- Pre-aggregation logic
- Statistical calculations
- Trend analysis

**cache_manager.py:**
- Redis integration
- Cache decorators
- TTL management
- Invalidation strategies

### 2. API Layer (`api/`)

**main.py:**
- FastAPI application
- Lifecycle management
- Middleware configuration

**routes/:**
- `movies.py` - Movie endpoints
- `trending.py` - Trending endpoints
- `analytics.py` - Analytics endpoints
- `search.py` - Search endpoints
- `health.py` - Health checks

**middleware/:**
- `auth.py` - Authentication (JWT, API keys)
- `rate_limit.py` - Rate limiting (token bucket)
- `cors.py` - CORS configuration

### 3. Database Layer (`mongodb/`)

**client.py:**
- Connection pooling
- Health checks
- Client lifecycle

**queries.py:**
- Pre-built query builders
- Common query patterns
- Query optimization

**indexes.py:**
- Index definitions
- Index creation
- Index monitoring

**migrations.py:**
- Schema migrations
- Data transformations
- Version management

## 🎨 Design Patterns

### 1. Dependency Injection

```python
from fastapi import Depends

def get_view_merger():
    db = get_database()
    return ViewMerger(db)

@router.get("/movies/{id}")
async def get_movie(
    id: int,
    merger: ViewMerger = Depends(get_view_merger)
):
    return merger.merge_movie_views(id)
```

### 2. Repository Pattern

```python
class MovieQueries:
    def __init__(self, db):
        self.db = db
    
    def get_by_genre(self, genre, year):
        return self.db.batch_views.find({
            "view_type": "genre_analytics",
            "data.genre": genre,
            "data.year": year
        })
```

### 3. Decorator Pattern (Caching)

```python
@cache_response(ttl_seconds=300)
async def get_trending_movies(limit=20):
    return query_speed_layer(limit)
```

### 4. Strategy Pattern (Query Routing)

```python
class QueryRouter:
    def route(self, query_type, time_range):
        if time_range.is_recent():
            return SpeedLayerStrategy()
        else:
            return BatchLayerStrategy()
```

## 🔐 Security

### Authentication Layers

1. **API Key**: Simple key-based auth
2. **JWT**: Token-based auth with expiration
3. **Rate Limiting**: Token bucket algorithm
4. **CORS**: Origin-based access control

### Data Protection

- **Encryption**: TLS/SSL for transport
- **Input Validation**: Pydantic models
- **SQL Injection**: MongoDB (NoSQL) - no SQL injection risk
- **XSS**: JSON responses (auto-escaped)

## 📈 Scalability

### Horizontal Scaling

```yaml
# Scale API workers
docker-compose up --scale serving-api=4

# Load balancer in front
nginx → [api-1, api-2, api-3, api-4]
```

### Vertical Scaling

- Increase MongoDB connection pool
- Add Redis memory
- Increase worker memory

### Database Scaling

- MongoDB sharding (by movie_id)
- Read replicas for query distribution
- Redis cluster for cache distribution

## 🎯 SLA & Monitoring

### Service Level Objectives (SLO)

- **Availability**: 99.9% uptime
- **Latency**: p95 < 200ms
- **Error Rate**: < 0.1%
- **Data Freshness**: Speed layer lag < 5 minutes

### Metrics

**API Metrics:**
- Request rate (req/s)
- Latency distribution (p50, p95, p99)
- Error rate by endpoint
- Cache hit rate

**Database Metrics:**
- Query latency
- Connection pool usage
- Index usage
- Collection sizes

**System Metrics:**
- CPU usage
- Memory usage
- Network I/O
- Disk I/O

## 🔄 Failure Modes

### Scenario 1: MongoDB Unavailable

**Impact**: API returns errors
**Mitigation**: Connection retries, circuit breaker
**Recovery**: Auto-reconnect when available

### Scenario 2: Redis Unavailable

**Impact**: Slower responses (no cache)
**Mitigation**: API continues without cache
**Recovery**: Cache warms up gradually

### Scenario 3: Speed Layer Lag

**Impact**: Older "recent" data
**Mitigation**: Automatic fallback to batch
**Recovery**: Monitor lag, alert if >10 minutes

### Scenario 4: Batch Layer Stale

**Impact**: Historical data outdated
**Mitigation**: Timestamp checks, stale data warnings
**Recovery**: Batch layer catches up

## 🚀 Future Enhancements

1. **GraphQL API**: More flexible queries
2. **Websocket Support**: Real-time push updates
3. **Multi-region**: Geographic distribution
4. **ML Integration**: Recommendation endpoints
5. **Advanced Caching**: Predictive cache warming
6. **Query Optimization**: Automatic index suggestions

---

**Built with Lambda Architecture principles for optimal balance of batch accuracy and real-time freshness.**
