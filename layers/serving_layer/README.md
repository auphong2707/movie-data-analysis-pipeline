# Serving Layer - Unified Query Interface

## Overview

The **Serving Layer** merges batch accuracy with speed freshness, providing a unified query interface through FastAPI. It combines historical data from the batch layer with recent data from the speed layer, offering the best of both worlds.

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
        │ Query Router  │  • Route by timestamp
        │ & Merger      │  • Merge results
        └───────┬───────┘  • Deduplicate
                │          • Apply business logic
                ↓
        ┌───────────────┐
        │   FastAPI     │  • REST endpoints
        │  REST API     │  • Authentication
        └───────┬───────┘  • Rate limiting
                │          • Response caching
                ↓
        ┌───────────────┐
        │Apache Superset│  • Business dashboards
        │   Grafana     │  • Real-time monitoring
        └───────────────┘  • Custom visualizations
```

---

## Key Characteristics

| Property | Value | Rationale |
|----------|-------|-----------|
| **Query Latency** | <100ms (p95) | Fast user experience |
| **Freshness** | 5 minutes | From speed layer |
| **Historical Depth** | 5 years | From batch layer |
| **Merge Strategy** | 48-hour cutoff | Clean separation |
| **Storage** | MongoDB | Fast random reads |
| **API Framework** | FastAPI | Async, high performance |

---

## Directory Structure

```
serving_layer/
├── README.md                       # This file
│
├── api/                           # REST API endpoints
│   ├── __init__.py
│   ├── main.py                    # FastAPI application
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── movies.py              # Movie endpoints
│   │   ├── sentiment.py           # Sentiment endpoints
│   │   ├── trending.py            # Trending endpoints
│   │   ├── analytics.py           # Analytics endpoints
│   │   └── health.py              # Health checks
│   ├── middleware/
│   │   ├── __init__.py
│   │   ├── auth.py                # Authentication
│   │   ├── rate_limit.py          # Rate limiting
│   │   └── cors.py                # CORS configuration
│   └── README.md                  # API documentation
│
├── query_engine/                  # Query processing
│   ├── __init__.py
│   ├── view_merger.py             # Merge batch + speed
│   ├── query_router.py            # Route queries
│   ├── aggregator.py              # Aggregation logic
│   ├── cache_manager.py           # Redis caching
│   └── README.md                  # Engine documentation
│
├── mongodb/                       # Database layer
│   ├── __init__.py
│   ├── client.py                  # MongoDB client
│   ├── queries.py                 # Query builders
│   ├── indexes.py                 # Index management
│   ├── migrations.py              # Schema migrations
│   └── README.md                  # Database documentation
│
├── visualization/                 # Dashboards
│   ├── superset/
│   │   ├── dashboards/            # Superset dashboard configs
│   │   ├── datasets/              # Dataset definitions
│   │   └── README.md              # Superset setup
│   ├── grafana/
│   │   ├── dashboards/            # Grafana dashboard JSONs
│   │   ├── datasources/           # Data source configs
│   │   └── README.md              # Grafana setup
│   └── README.md                  # Visualization documentation
│
├── config/                        # Configuration
│   ├── api_config.yaml            # API settings
│   ├── mongodb_config.yaml        # MongoDB settings
│   └── cache_config.yaml          # Redis settings
│
└── tests/                         # Unit tests
    ├── test_api_endpoints.py
    ├── test_query_merger.py
    ├── test_cache.py
    └── test_performance.py
```

---

## Query Merge Strategy

### 48-Hour Cutoff Rule

**Logic**:
- Data **older than 48 hours**: Query `batch_views` collection
- Data **within 48 hours**: Query `speed_views` collection
- If overlap: Speed layer takes precedence (fresher data)

**Example**:
```python
# User requests sentiment for movie_id=12345
current_time = datetime.now()
cutoff_time = current_time - timedelta(hours=48)

# Query batch layer for historical data
batch_data = mongo.batch_views.find({
    "movie_id": 12345,
    "computed_at": {"$lt": cutoff_time}
})

# Query speed layer for recent data
speed_data = mongo.speed_views.find({
    "movie_id": 12345,
    "hour": {"$gte": cutoff_time}
})

# Merge results (speed overrides batch for overlaps)
merged_data = merge_by_timestamp(batch_data, speed_data)
```

---

## MongoDB Schema

### Database: `movie_analytics`

---

### Collection: `batch_views`

**Purpose**: Store batch layer pre-computed views

```javascript
{
  "_id": ObjectId("..."),
  "movie_id": 12345,
  "view_type": "genre_analytics",  // genre/trending/temporal/sentiment
  "data": {
    // View-specific data
    "genre": "Action",
    "year": 2025,
    "month": 10,
    "avg_rating": 7.5,
    "total_movies": 150,
    "avg_sentiment": 0.65
  },
  "computed_at": ISODate("2025-10-17T14:00:00Z"),
  "batch_run_id": "batch_2025_10_17_14",
  "version": 1
}
```

**Indexes**:
```javascript
db.batch_views.createIndex({ "movie_id": 1, "view_type": 1 })
db.batch_views.createIndex({ "view_type": 1, "data.genre": 1, "data.year": 1 })
db.batch_views.createIndex({ "computed_at": -1 })
db.batch_views.createIndex({ "view_type": 1, "computed_at": -1 })
```

---

### Collection: `speed_views`

**Purpose**: Store speed layer real-time views

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
    "neutral_count": 5,
    "sentiment_velocity": 0.02
  },
  "synced_at": ISODate("2025-10-17T14:05:00Z"),
  "ttl_expires_at": ISODate("2025-10-19T14:00:00Z")
}
```

**Indexes**:
```javascript
db.speed_views.createIndex({ "movie_id": 1, "view_type": 1, "hour": -1 })
db.speed_views.createIndex({ "view_type": 1, "hour": -1 })
db.speed_views.createIndex({ "ttl_expires_at": 1 }, { expireAfterSeconds: 0 })
```

**TTL Index**: Automatically removes documents after 48 hours

---

### Collection: `cache_metadata`

**Purpose**: Track query cache status

```javascript
{
  "_id": ObjectId("..."),
  "cache_key": "trending_movies_action_2025_10",
  "query_hash": "abc123...",
  "cached_at": ISODate("2025-10-17T14:00:00Z"),
  "expires_at": ISODate("2025-10-17T14:15:00Z"),
  "hit_count": 42,
  "size_bytes": 5120
}
```

---

## FastAPI Endpoints

### Base URL: `http://api.moviepipeline.com/v1`

---

### 1. Movie Endpoints (`/movies`)

#### GET `/movies/{movie_id}`
**Description**: Get detailed movie information

**Response**:
```json
{
  "movie_id": 12345,
  "title": "The Great Movie",
  "release_date": "2025-06-15",
  "genres": ["Action", "Thriller"],
  "vote_average": 7.8,
  "vote_count": 15234,
  "popularity": 89.5,
  "runtime": 142,
  "budget": 150000000,
  "revenue": 500000000,
  "data_source": "batch",  // batch or speed
  "last_updated": "2025-10-17T14:00:00Z"
}
```

---

#### GET `/movies/{movie_id}/sentiment`
**Description**: Get sentiment analysis for a movie

**Query Parameters**:
- `window` (optional): Time window (7d, 30d, all)

**Response**:
```json
{
  "movie_id": 12345,
  "title": "The Great Movie",
  "sentiment": {
    "overall_score": 0.75,  // -1 to 1
    "label": "positive",
    "positive_count": 850,
    "negative_count": 120,
    "neutral_count": 230,
    "total_reviews": 1200,
    "velocity": 0.02,  // sentiment change rate
    "confidence": 0.92
  },
  "breakdown": [
    {
      "date": "2025-10-17",
      "avg_sentiment": 0.78,
      "review_count": 45
    },
    // More daily breakdowns...
  ],
  "data_sources": {
    "batch": "2025-10-15T10:00:00Z",  // up to this time
    "speed": "2025-10-17T14:00:00Z"   // from batch cutoff to now
  }
}
```

---

### 2. Trending Endpoints (`/trending`)

#### GET `/trending/movies`
**Description**: Get currently trending movies

**Query Parameters**:
- `genre` (optional): Filter by genre
- `limit` (default: 20): Number of results
- `window` (optional): Time window (1h, 6h, 24h)

**Response**:
```json
{
  "trending_movies": [
    {
      "rank": 1,
      "movie_id": 12345,
      "title": "Hot New Movie",
      "trending_score": 98.5,
      "velocity": 15.3,  // popularity increase rate
      "acceleration": 2.1,  // velocity change rate
      "genres": ["Action"],
      "popularity": 125.8,
      "vote_average": 8.2
    },
    // More trending movies...
  ],
  "generated_at": "2025-10-17T14:00:00Z",
  "window": "6h",
  "data_source": "speed"
}
```

---

### 3. Analytics Endpoints (`/analytics`)

#### GET `/analytics/genre/{genre}`
**Description**: Get analytics for a specific genre

**Query Parameters**:
- `year` (optional): Filter by year
- `month` (optional): Filter by month

**Response**:
```json
{
  "genre": "Action",
  "year": 2025,
  "month": 10,
  "statistics": {
    "total_movies": 150,
    "avg_rating": 7.5,
    "avg_sentiment": 0.65,
    "total_revenue": 5000000000,
    "avg_budget": 80000000,
    "avg_runtime": 128
  },
  "top_movies": [
    {
      "movie_id": 12345,
      "title": "Top Action Movie",
      "vote_average": 9.1,
      "revenue": 850000000
    },
    // More movies...
  ],
  "trends": {
    "rating_trend": "increasing",  // increasing/stable/decreasing
    "sentiment_trend": "stable",
    "popularity_trend": "increasing"
  },
  "data_source": "batch"
}
```

---

#### GET `/analytics/trends`
**Description**: Get time-series trend analysis

**Query Parameters**:
- `movie_id` (optional): Specific movie
- `genre` (optional): Specific genre
- `metric`: rating, sentiment, popularity
- `window`: 7d, 30d, 90d

**Response**:
```json
{
  "metric": "sentiment",
  "window": "30d",
  "data_points": [
    {
      "date": "2025-09-17",
      "value": 0.62,
      "count": 234
    },
    {
      "date": "2025-09-18",
      "value": 0.65,
      "count": 289
    },
    // More data points...
  ],
  "summary": {
    "avg": 0.68,
    "min": 0.45,
    "max": 0.82,
    "trend": "increasing",
    "change_rate": 0.003  // per day
  }
}
```

---

### 4. Search Endpoints (`/search`)

#### GET `/search/movies`
**Description**: Search movies by various criteria

**Query Parameters**:
- `q`: Search query (title, keywords)
- `genre`: Genre filter
- `year_from`, `year_to`: Year range
- `rating_min`, `rating_max`: Rating range
- `sort_by`: popularity, rating, release_date
- `limit`, `offset`: Pagination

**Response**:
```json
{
  "results": [
    {
      "movie_id": 12345,
      "title": "Matching Movie",
      "genres": ["Action"],
      "release_date": "2025-06-15",
      "vote_average": 7.8,
      "popularity": 89.5
    },
    // More results...
  ],
  "total_results": 450,
  "page": 1,
  "total_pages": 23
}
```

---

### 5. Health Endpoints (`/health`)

#### GET `/health`
**Description**: API health check

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2025-10-17T14:00:00Z",
  "services": {
    "mongodb": {
      "status": "up",
      "latency_ms": 5
    },
    "redis": {
      "status": "up",
      "latency_ms": 2
    },
    "batch_layer": {
      "status": "up",
      "last_update": "2025-10-17T10:00:00Z"
    },
    "speed_layer": {
      "status": "up",
      "last_update": "2025-10-17T13:55:00Z",
      "lag_seconds": 45
    }
  },
  "version": "1.0.0"
}
```

---

## Query Engine

### View Merger (`query_engine/view_merger.py`)

**Purpose**: Merge batch and speed layer views

**Algorithm**:
```python
def merge_views(movie_id, view_type, time_range=None):
    """
    Merge batch and speed views for a movie
    
    Args:
        movie_id: Movie identifier
        view_type: Type of view (sentiment/stats/trending)
        time_range: Optional time range filter
    
    Returns:
        Merged view data
    """
    current_time = datetime.now()
    cutoff_time = current_time - timedelta(hours=48)
    
    # Query batch layer (older than 48h)
    batch_query = {
        "movie_id": movie_id,
        "view_type": view_type,
        "computed_at": {"$lt": cutoff_time}
    }
    
    if time_range:
        batch_query["computed_at"]["$gte"] = time_range["start"]
    
    batch_data = list(mongo.batch_views.find(batch_query).sort("computed_at", -1))
    
    # Query speed layer (within 48h)
    speed_query = {
        "movie_id": movie_id,
        "view_type": view_type,
        "hour": {"$gte": cutoff_time}
    }
    
    if time_range:
        speed_query["hour"]["$lte"] = time_range["end"]
    
    speed_data = list(mongo.speed_views.find(speed_query).sort("hour", -1))
    
    # Merge (speed takes precedence for overlaps)
    merged = []
    speed_times = {doc["hour"] for doc in speed_data}
    
    # Add all speed data
    merged.extend(speed_data)
    
    # Add batch data for non-overlapping times
    for batch_doc in batch_data:
        if batch_doc["computed_at"] not in speed_times:
            merged.append(batch_doc)
    
    # Sort by timestamp (newest first)
    merged.sort(key=lambda x: x.get("hour") or x.get("computed_at"), reverse=True)
    
    return merged
```

**Template**: See `query_engine/view_merger.py`

---

### Cache Manager (`query_engine/cache_manager.py`)

**Purpose**: Redis-based caching for frequently accessed data

**Caching Strategy**:
- Cache trending movies (15-minute TTL)
- Cache popular movie details (5-minute TTL)
- Cache aggregated analytics (30-minute TTL)
- Cache search results (10-minute TTL)

**Implementation**:
```python
import redis
import json
from functools import wraps

redis_client = redis.Redis(host='redis', port=6379, db=0)

def cache_response(ttl_seconds=300):
    """Decorator to cache API responses"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key from function name and arguments
            cache_key = f"{func.__name__}:{json.dumps(args)}:{json.dumps(kwargs)}"
            
            # Try to get from cache
            cached = redis_client.get(cache_key)
            if cached:
                return json.loads(cached)
            
            # Call function and cache result
            result = await func(*args, **kwargs)
            redis_client.setex(cache_key, ttl_seconds, json.dumps(result))
            
            return result
        return wrapper
    return decorator
```

**Template**: See `query_engine/cache_manager.py`

---

## Configuration

### API Configuration (`config/api_config.yaml`)

```yaml
api:
  title: "Movie Analytics API"
  version: "1.0.0"
  host: "0.0.0.0"
  port: 8000
  workers: 4
  
  cors:
    allow_origins:
      - "http://localhost:3000"
      - "https://dashboard.moviepipeline.com"
    allow_methods: ["GET", "POST", "PUT", "DELETE"]
    allow_headers: ["*"]
  
  rate_limiting:
    enabled: true
    requests_per_minute: 100
    burst: 20
  
  authentication:
    enabled: true
    api_key_header: "X-API-Key"
    jwt_secret: "${JWT_SECRET}"
    
  cache:
    enabled: true
    default_ttl_seconds: 300
```

---

### MongoDB Configuration (`config/mongodb_config.yaml`)

```yaml
mongodb:
  uri: "mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/?replicaSet=rs0"
  database: "movie_analytics"
  
  collections:
    batch_views: "batch_views"
    speed_views: "speed_views"
    cache_metadata: "cache_metadata"
  
  connection_pool:
    max_pool_size: 100
    min_pool_size: 10
    max_idle_time_ms: 60000
  
  read_preference: "secondaryPreferred"
  write_concern:
    w: "majority"
    j: true
```

---

## Visualization

### Apache Superset Dashboards

**Dashboards**:
1. **Executive Overview**
   - Total movies, average ratings
   - Revenue trends
   - Genre distribution

2. **Real-time Monitoring**
   - Trending movies (from speed layer)
   - Recent sentiment changes
   - Live popularity charts

3. **Historical Analysis**
   - Year-over-year comparisons
   - Genre performance over time
   - Actor collaboration networks

**Setup**: See `visualization/superset/README.md`

---

### Grafana Dashboards

**Dashboards**:
1. **System Health**
   - API latency (p50, p95, p99)
   - MongoDB query performance
   - Redis cache hit rates

2. **Data Freshness**
   - Batch layer last update
   - Speed layer lag
   - View staleness

3. **Usage Metrics**
   - API request rates
   - Endpoint popularity
   - Error rates

**Setup**: See `visualization/grafana/README.md`

---

## Next Phase Implementation Tasks

### Phase 4A: MongoDB Setup (Week 9)
- [ ] Deploy MongoDB replica set (3 nodes)
- [ ] Create database and collections
- [ ] Create indexes for performance
- [ ] Set up TTL index for speed_views

### Phase 4B: Query Engine (Week 9)
- [ ] Implement `query_engine/view_merger.py`
  - 48-hour cutoff logic
  - Merge algorithm
  - Deduplication

- [ ] Create `query_engine/query_router.py`
  - Route by timestamp
  - Query optimization
  - Aggregation logic

- [ ] Build `query_engine/cache_manager.py`
  - Redis integration
  - Cache decorators
  - TTL management

### Phase 4C: FastAPI Implementation (Week 10)
- [ ] Set up FastAPI project structure
- [ ] Implement movie endpoints
  - GET /movies/{id}
  - GET /movies/{id}/sentiment

- [ ] Create trending endpoints
  - GET /trending/movies
  - Real-time aggregation

- [ ] Build analytics endpoints
  - GET /analytics/genre/{genre}
  - GET /analytics/trends

- [ ] Add search endpoints
  - GET /search/movies
  - Full-text search

- [ ] Implement health check
  - Service status
  - Latency checks

### Phase 4D: Middleware & Security (Week 10)
- [ ] Add authentication middleware
  - API key validation
  - JWT tokens

- [ ] Implement rate limiting
  - Per-user limits
  - Burst handling

- [ ] Configure CORS
  - Allowed origins
  - Credentials support

### Phase 4E: Testing & Documentation (Week 10)
- [ ] Write API tests
  - Endpoint tests
  - Integration tests
  - Performance tests

- [ ] Generate API documentation
  - OpenAPI/Swagger
  - Example requests
  - Response schemas

---

## Testing Strategy

### Unit Tests
```python
# tests/test_query_merger.py
def test_merge_views():
    # Mock batch and speed data
    # Call merge_views
    # Assert correct merging
    pass

def test_48_hour_cutoff():
    # Test boundary conditions
    pass
```

### API Tests
```python
# tests/test_api_endpoints.py
def test_get_movie():
    response = client.get("/movies/12345")
    assert response.status_code == 200
    assert "movie_id" in response.json()

def test_get_sentiment():
    response = client.get("/movies/12345/sentiment")
    assert response.status_code == 200
    assert "sentiment" in response.json()
```

### Performance Tests
```python
# tests/test_performance.py
def test_api_latency():
    # Measure p95 latency
    # Assert < 100ms
    pass

def test_cache_hit_rate():
    # Measure cache effectiveness
    # Assert > 70% hit rate
    pass
```

---

## Monitoring & Alerts

### Key Metrics
- **API Latency**: p50 < 50ms, p95 < 100ms, p99 < 200ms
- **Cache Hit Rate**: > 70%
- **MongoDB Query Time**: p95 < 50ms
- **Data Freshness**: Speed layer lag < 5 minutes

### Alerts
- API p95 latency > 200ms → Warning
- Cache hit rate < 50% → Warning
- MongoDB connection issues → Critical
- Speed layer lag > 10 minutes → Warning

---

## References

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [MongoDB Best Practices](https://docs.mongodb.com/manual/administration/production-notes/)
- [Redis Caching Patterns](https://redis.io/topics/lru-cache)
- [Apache Superset Documentation](https://superset.apache.org/docs/intro)

---

**Next Step**: Review [DEPLOYMENT.md](../../docs/DEPLOYMENT.md) for production deployment
