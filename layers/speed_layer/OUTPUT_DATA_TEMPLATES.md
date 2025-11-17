# Speed Layer – Output Data Templates (TMDB → Kafka → Spark → Cassandra → MongoDB)

This document provides complete output data templates at each stage of the Speed Layer pipeline, from raw TMDB API responses through final MongoDB documents.

---

## 1. TMDB OUTPUT TEMPLATES

### 1.1 Raw TMDB Response (Example)

TMDB API provides movie data through multiple endpoints. Below are the key endpoints used:

**GET /movie/{id}/reviews**
```json
{
  "id": 550,
  "page": 1,
  "results": [
    {
      "author": "John Doe",
      "author_details": {
        "name": "John Doe",
        "username": "johndoe",
        "avatar_path": "/avatar.jpg",
        "rating": 9.0
      },
      "content": "An incredible movie with amazing visuals and a compelling story...",
      "created_at": "2023-11-15T10:30:00.000Z",
      "id": "5f8a9b1c2d3e4f5g6h7i8j9k",
      "updated_at": "2023-11-15T10:30:00.000Z",
      "url": "https://www.themoviedb.org/review/5f8a9b1c2d3e4f5g6h7i8j9k"
    }
  ]
}
```

**GET /movie/{id}**
```json
{
  "adult": false,
  "backdrop_path": "/backdrop.jpg",
  "budget": 63000000,
  "genres": [
    {"id": 18, "name": "Drama"},
    {"id": 53, "name": "Thriller"}
  ],
  "id": 550,
  "original_language": "en",
  "original_title": "Fight Club",
  "overview": "A ticking-time-bomb insomniac and a slippery soap salesman...",
  "popularity": 98.654,
  "poster_path": "/poster.jpg",
  "release_date": "1999-10-15",
  "revenue": 100853753,
  "runtime": 139,
  "title": "Fight Club",
  "vote_average": 8.433,
  "vote_count": 28567
}
```

### 1.2 Cleaned Standardized Format

The producer normalizes TMDB responses into consistent event formats:

**Reviews Normalization**
- Extract `author` from `author_details.username` or `author`
- Extract `rating` from `author_details.rating` (nullable)
- Convert `created_at` to Unix timestamp (milliseconds)
- Generate consistent `review_id` from TMDB `id`

**Ratings Normalization**
- Extract vote statistics: `vote_average`, `vote_count`
- Extract `popularity` metric
- Add event timestamp

**Metadata Normalization**
- Flatten genre objects to string array
- Convert budget/revenue to long integers
- Extract runtime, release_date
- Add event_type for classification

### 1.3 Output Template

```json
{
  "review": {
    "review_id": "<string>",
    "movie_id": "<int>",
    "author": "<string>",
    "content": "<string>",
    "rating": "<double|null>",
    "created_at": "<long:timestamp-millis>",
    "url": "<string>"
  },
  "rating": {
    "movie_id": "<int>",
    "vote_average": "<double>",
    "vote_count": "<int>",
    "popularity": "<double>",
    "timestamp": "<long:timestamp-millis>"
  },
  "metadata": {
    "movie_id": "<int>",
    "title": "<string>",
    "release_date": "<string|null>",
    "genres": ["<string>"],
    "runtime": "<int|null>",
    "budget": "<long|null>",
    "revenue": "<long|null>",
    "timestamp": "<long:timestamp-millis>",
    "event_type": "<string>"
  }
}
```

### 1.4 Example Record

**Review Record**
```json
{
  "review_id": "5f8a9b1c2d3e4f5g6h7i8j9k",
  "movie_id": 550,
  "author": "johndoe",
  "content": "An incredible movie with amazing visuals and a compelling story that keeps you on the edge of your seat.",
  "rating": 9.0,
  "created_at": 1700046600000,
  "url": "https://www.themoviedb.org/review/5f8a9b1c2d3e4f5g6h7i8j9k"
}
```

**Rating Record**
```json
{
  "movie_id": 550,
  "vote_average": 8.433,
  "vote_count": 28567,
  "popularity": 98.654,
  "timestamp": 1700046600000
}
```

**Metadata Record**
```json
{
  "movie_id": 550,
  "title": "Fight Club",
  "release_date": "1999-10-15",
  "genres": ["Drama", "Thriller"],
  "runtime": 139,
  "budget": 63000000,
  "revenue": 100853753,
  "timestamp": 1700046600000,
  "event_type": "metadata_update"
}
```

---

## 2. KAFKA OUTPUT TEMPLATES

### 2.1 movie.reviews

#### Avro Schema

```json
{
  "type": "record",
  "name": "MovieReview",
  "namespace": "com.movieanalytics.tmdb",
  "fields": [
    {"name": "review_id", "type": "string"},
    {"name": "movie_id", "type": "long"},
    {"name": "author", "type": "string"},
    {"name": "content", "type": "string"},
    {"name": "rating", "type": ["null", "double"], "default": null},
    {"name": "created_at", "type": "string"},
    {"name": "url", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "language", "type": ["null", "string"], "default": null}
  ]
}
```

#### Message Template

```json
{
  "key": "<movie_id>",
  "value": {
    "review_id": "<string>",
    "movie_id": "<long>",
    "author": "<string>",
    "content": "<string>",
    "rating": "<double|null>",
    "created_at": "<string:ISO8601>",
    "url": "<string>",
    "timestamp": "<long:epoch-millis>",
    "language": "<string|null>"
  },
  "headers": {
    "source": "tmdb-api",
    "event_type": "review_created"
  }
}
```

#### Example Message

```json
{
  "key": "550",
  "value": {
    "review_id": "5f8a9b1c2d3e4f5g6h7i8j9k",
    "movie_id": 550,
    "author": "johndoe",
    "content": "An incredible movie with amazing visuals and a compelling story that keeps you on the edge of your seat.",
    "rating": 9.0,
    "created_at": "2023-11-15T10:30:00.000Z",
    "url": "https://www.themoviedb.org/review/5f8a9b1c2d3e4f5g6h7i8j9k",
    "timestamp": 1700046600000,
    "language": "en"
  },
  "headers": {
    "source": "tmdb-api",
    "event_type": "review_created"
  }
}
```

### 2.2 movie.ratings

#### Avro Schema

```json
{
  "type": "record",
  "name": "MovieRating",
  "namespace": "com.movieanalytics.tmdb",
  "fields": [
    {"name": "movie_id", "type": "long"},
    {"name": "vote_average", "type": "double"},
    {"name": "vote_count", "type": "long"},
    {"name": "popularity", "type": "double"},
    {"name": "timestamp", "type": "long"}
  ]
}
```

#### Message Template

```json
{
  "key": "<movie_id>",
  "value": {
    "movie_id": "<long>",
    "vote_average": "<double>",
    "vote_count": "<long>",
    "popularity": "<double>",
    "timestamp": "<long:epoch-millis>"
  },
  "headers": {
    "source": "tmdb-api",
    "event_type": "rating_update"
  }
}
```

#### Example Message

```json
{
  "key": "550",
  "value": {
    "movie_id": 550,
    "vote_average": 8.433,
    "vote_count": 28567,
    "popularity": 98.654,
    "timestamp": 1700046600000
  },
  "headers": {
    "source": "tmdb-api",
    "event_type": "rating_update"
  }
}
```

### 2.3 movie.metadata

#### Avro Schema

```json
{
  "type": "record",
  "name": "MovieMetadata",
  "namespace": "com.movieanalytics.tmdb",
  "fields": [
    {"name": "movie_id", "type": "long"},
    {"name": "title", "type": "string"},
    {"name": "release_date", "type": ["null", "string"], "default": null},
    {"name": "popularity", "type": "double"},
    {"name": "vote_average", "type": "double"},
    {"name": "vote_count", "type": "long"},
    {"name": "adult", "type": "boolean", "default": false},
    {"name": "genre_ids", "type": {"type": "array", "items": "int"}},
    {"name": "overview", "type": ["null", "string"], "default": null},
    {"name": "poster_path", "type": ["null", "string"], "default": null},
    {"name": "backdrop_path", "type": ["null", "string"], "default": null},
    {"name": "original_language", "type": ["null", "string"], "default": null},
    {"name": "original_title", "type": ["null", "string"], "default": null},
    {"name": "video", "type": "boolean", "default": false},
    {"name": "timestamp", "type": "long"}
  ]
}
```

#### Message Template

```json
{
  "key": "<movie_id>",
  "value": {
    "movie_id": "<long>",
    "title": "<string>",
    "release_date": "<string|null>",
    "popularity": "<double>",
    "vote_average": "<double>",
    "vote_count": "<long>",
    "adult": "<boolean>",
    "genre_ids": ["<int>"],
    "overview": "<string|null>",
    "poster_path": "<string|null>",
    "backdrop_path": "<string|null>",
    "original_language": "<string|null>",
    "original_title": "<string|null>",
    "video": "<boolean>",
    "timestamp": "<long:epoch-millis>"
  },
  "headers": {
    "source": "tmdb-api",
    "event_type": "metadata_update"
  }
}
```

#### Example Message

```json
{
  "key": "550",
  "value": {
    "movie_id": 550,
    "title": "Fight Club",
    "release_date": "1999-10-15",
    "popularity": 98.654,
    "vote_average": 8.433,
    "vote_count": 28567,
    "adult": false,
    "genre_ids": [18, 53],
    "overview": "A ticking-time-bomb insomniac and a slippery soap salesman channel primal male aggression into a new form of therapy.",
    "poster_path": "/pB8BM7pdSp6B6Ih7QZ4DrQ3PmJK.jpg",
    "backdrop_path": "/hZkgoQYus5vegHoetLkCJzb17zJ.jpg",
    "original_language": "en",
    "original_title": "Fight Club",
    "video": false,
    "timestamp": 1700046600000
  },
  "headers": {
    "source": "tmdb-api",
    "event_type": "metadata_update"
  }
}
```

---

## 3. STREAMING JOBS OUTPUT (Spark Structured Streaming)

### 3.1 review_sentiment_stream

#### Purpose

Real-time sentiment analysis of movie reviews using VADER (Valence Aware Dictionary and sEntiment Reasoner). Aggregates sentiment metrics in 5-minute tumbling windows.

#### Input Topics

- `movie.reviews`

#### Output Schema (SQL + JSON)

**SQL Schema**
```sql
movie_id INT,
hour TIMESTAMP,
window_start TIMESTAMP,
window_end TIMESTAMP,
avg_sentiment DOUBLE,
review_count INT,
positive_count INT,
negative_count INT,
neutral_count INT,
sentiment_velocity DOUBLE
```

**JSON Schema**
```json
{
  "type": "struct",
  "fields": [
    {"name": "movie_id", "type": "integer", "nullable": false},
    {"name": "hour", "type": "timestamp", "nullable": false},
    {"name": "window_start", "type": "timestamp", "nullable": false},
    {"name": "window_end", "type": "timestamp", "nullable": false},
    {"name": "avg_sentiment", "type": "double", "nullable": true},
    {"name": "review_count", "type": "integer", "nullable": false},
    {"name": "positive_count", "type": "integer", "nullable": false},
    {"name": "negative_count", "type": "integer", "nullable": false},
    {"name": "neutral_count", "type": "integer", "nullable": false},
    {"name": "sentiment_velocity", "type": "double", "nullable": true}
  ]
}
```

#### Output Template

```json
{
  "movie_id": "<int>",
  "hour": "<timestamp:YYYY-MM-DD HH:00:00>",
  "window_start": "<timestamp:YYYY-MM-DD HH:MM:SS>",
  "window_end": "<timestamp:YYYY-MM-DD HH:MM:SS>",
  "avg_sentiment": "<double>",
  "review_count": "<int>",
  "positive_count": "<int>",
  "negative_count": "<int>",
  "neutral_count": "<int>",
  "sentiment_velocity": "<double>"
}
```

#### Example Output

```json
{
  "movie_id": 550,
  "hour": "2025-11-16 14:00:00",
  "window_start": "2025-11-16 14:30:00",
  "window_end": "2025-11-16 14:35:00",
  "avg_sentiment": 0.6523,
  "review_count": 23,
  "positive_count": 18,
  "negative_count": 3,
  "neutral_count": 2,
  "sentiment_velocity": 0.045
}
```

### 3.2 movie_stats_aggregation_stream

#### Purpose

Real-time aggregation of movie rating statistics with velocity calculations. Computes hourly metrics including vote averages, counts, popularity, and rating velocity.

#### Input Topics

- `movie.ratings`
- `movie.metadata`

#### Output Schema (SQL + JSON)

**SQL Schema**
```sql
movie_id INT,
hour TIMESTAMP,
vote_average DOUBLE,
vote_count INT,
popularity DOUBLE,
rating_velocity DOUBLE,
last_updated TIMESTAMP
```

**JSON Schema**
```json
{
  "type": "struct",
  "fields": [
    {"name": "movie_id", "type": "integer", "nullable": false},
    {"name": "hour", "type": "timestamp", "nullable": false},
    {"name": "vote_average", "type": "double", "nullable": false},
    {"name": "vote_count", "type": "integer", "nullable": false},
    {"name": "popularity", "type": "double", "nullable": false},
    {"name": "rating_velocity", "type": "double", "nullable": true},
    {"name": "last_updated", "type": "timestamp", "nullable": false}
  ]
}
```

#### Output Template

```json
{
  "movie_id": "<int>",
  "hour": "<timestamp:YYYY-MM-DD HH:00:00>",
  "vote_average": "<double>",
  "vote_count": "<int>",
  "popularity": "<double>",
  "rating_velocity": "<double>",
  "last_updated": "<timestamp:YYYY-MM-DD HH:MM:SS>"
}
```

#### Example Output

```json
{
  "movie_id": 550,
  "hour": "2025-11-16 14:00:00",
  "vote_average": 8.433,
  "vote_count": 28567,
  "popularity": 98.654,
  "rating_velocity": 0.12,
  "last_updated": "2025-11-16 14:45:23"
}
```

### 3.3 trending_movies_stream

#### Purpose

Detect trending/hot movies using velocity and acceleration metrics. Ranks top 100 movies by trending score, calculated from popularity and vote count dynamics.

#### Input Topics

- `movie.ratings`

#### Output Schema (SQL + JSON)

**SQL Schema**
```sql
hour TIMESTAMP,
rank INT,
movie_id INT,
title STRING,
trending_score DOUBLE,
velocity DOUBLE,
acceleration DOUBLE
```

**JSON Schema**
```json
{
  "type": "struct",
  "fields": [
    {"name": "hour", "type": "timestamp", "nullable": false},
    {"name": "rank", "type": "integer", "nullable": false},
    {"name": "movie_id", "type": "integer", "nullable": false},
    {"name": "title", "type": "string", "nullable": false},
    {"name": "trending_score", "type": "double", "nullable": false},
    {"name": "velocity", "type": "double", "nullable": false},
    {"name": "acceleration", "type": "double", "nullable": false}
  ]
}
```

#### Output Template

```json
{
  "hour": "<timestamp:YYYY-MM-DD HH:00:00>",
  "rank": "<int:1-100>",
  "movie_id": "<int>",
  "title": "<string>",
  "trending_score": "<double>",
  "velocity": "<double>",
  "acceleration": "<double>"
}
```

#### Example Output

```json
{
  "hour": "2025-11-16 14:00:00",
  "rank": 1,
  "movie_id": 550,
  "title": "Fight Club",
  "trending_score": 156.327,
  "velocity": 49.327,
  "acceleration": 14.283
}
```

---

## 4. CASSANDRA OUTPUT TABLE TEMPLATES

### 4.1 review_sentiments

#### CQL Schema

```sql
CREATE TABLE IF NOT EXISTS review_sentiments (
    movie_id int,
    hour timestamp,
    window_start timestamp,
    window_end timestamp,
    avg_sentiment double,
    review_count int,
    positive_count int,
    negative_count int,
    neutral_count int,
    sentiment_velocity double,
    PRIMARY KEY (movie_id, hour, window_start)
) WITH default_time_to_live = 172800
  AND CLUSTERING ORDER BY (hour DESC, window_start DESC);
```

#### Primary Key Explanation

- **Partition Key**: `movie_id` – Groups all sentiment data for a specific movie together
- **Clustering Keys**: 
  - `hour` (DESC) – Orders by most recent hour first
  - `window_start` (DESC) – Orders by most recent window within each hour
- **TTL**: 172800 seconds (48 hours) – Data automatically expires after 2 days

**Query Patterns Supported**:
- Get all sentiment windows for a movie
- Get sentiment for a movie within a specific hour
- Get sentiment for a movie within a time range

#### Record Template

```json
{
  "movie_id": "<int>",
  "hour": "<timestamp>",
  "window_start": "<timestamp>",
  "window_end": "<timestamp>",
  "avg_sentiment": "<double>",
  "review_count": "<int>",
  "positive_count": "<int>",
  "negative_count": "<int>",
  "neutral_count": "<int>",
  "sentiment_velocity": "<double>"
}
```

#### Example Row

```json
{
  "movie_id": 550,
  "hour": "2025-11-16 14:00:00",
  "window_start": "2025-11-16 14:30:00",
  "window_end": "2025-11-16 14:35:00",
  "avg_sentiment": 0.6523,
  "review_count": 23,
  "positive_count": 18,
  "negative_count": 3,
  "neutral_count": 2,
  "sentiment_velocity": 0.045
}
```

### 4.2 movie_stats

#### CQL Schema

```sql
CREATE TABLE IF NOT EXISTS movie_stats (
    movie_id int,
    hour timestamp,
    vote_average double,
    vote_count int,
    popularity double,
    rating_velocity double,
    last_updated timestamp,
    PRIMARY KEY (movie_id, hour)
) WITH default_time_to_live = 172800
  AND CLUSTERING ORDER BY (hour DESC);
```

#### Primary Key Explanation

- **Partition Key**: `movie_id` – Groups all statistics for a specific movie
- **Clustering Key**: `hour` (DESC) – Orders by most recent hour first
- **TTL**: 172800 seconds (48 hours) – Data expires after 2 days

**Query Patterns Supported**:
- Get latest stats for a movie
- Get stats for a movie within a time range
- Get stats for a movie at a specific hour

#### Record Template

```json
{
  "movie_id": "<int>",
  "hour": "<timestamp>",
  "vote_average": "<double>",
  "vote_count": "<int>",
  "popularity": "<double>",
  "rating_velocity": "<double>",
  "last_updated": "<timestamp>"
}
```

#### Example Row

```json
{
  "movie_id": 550,
  "hour": "2025-11-16 14:00:00",
  "vote_average": 8.433,
  "vote_count": 28567,
  "popularity": 98.654,
  "rating_velocity": 0.12,
  "last_updated": "2025-11-16 14:45:23"
}
```

### 4.3 trending_movies

#### CQL Schema

```sql
CREATE TABLE IF NOT EXISTS trending_movies (
    hour timestamp,
    rank int,
    movie_id int,
    title text,
    trending_score double,
    velocity double,
    acceleration double,
    PRIMARY KEY (hour, rank, movie_id)
) WITH default_time_to_live = 172800
  AND CLUSTERING ORDER BY (rank ASC, movie_id ASC);
```

#### Primary Key Explanation

- **Partition Key**: `hour` – Groups all trending movies for a specific hour
- **Clustering Keys**: 
  - `rank` (ASC) – Orders movies by rank (1 = most trending)
  - `movie_id` (ASC) – Ensures uniqueness when movies have same rank
- **TTL**: 172800 seconds (48 hours) – Data expires after 2 days

**Query Patterns Supported**:
- Get top 100 trending movies for current hour
- Get trending rank for a specific movie in an hour
- Get trending movies within a rank range (e.g., top 10)

#### Record Template

```json
{
  "hour": "<timestamp>",
  "rank": "<int:1-100>",
  "movie_id": "<int>",
  "title": "<string>",
  "trending_score": "<double>",
  "velocity": "<double>",
  "acceleration": "<double>"
}
```

#### Example Row

```json
{
  "hour": "2025-11-16 14:00:00",
  "rank": 1,
  "movie_id": 550,
  "title": "Fight Club",
  "trending_score": 156.327,
  "velocity": 49.327,
  "acceleration": 14.283
}
```

---

## 5. MONGODB SYNC OUTPUT TEMPLATES

### 5.1 speed_views (Unified Collection)

#### Document Structure

MongoDB receives denormalized views from Cassandra for unified serving layer access. Three document types stored in same collection, differentiated by `view_type` field.

**Collection**: `speed_views`
**Database**: `moviedb`

#### Document Template

**Review Sentiments View**
```json
{
  "_id": "<ObjectId>",
  "view_type": "review_sentiment",
  "movie_id": "<int>",
  "hour": "<ISODate>",
  "window_start": "<ISODate>",
  "window_end": "<ISODate>",
  "avg_sentiment": "<double>",
  "review_count": "<int>",
  "positive_count": "<int>",
  "negative_count": "<int>",
  "neutral_count": "<int>",
  "sentiment_velocity": "<double>",
  "synced_at": "<ISODate>",
  "ttl_expires_at": "<ISODate>"
}
```

**Movie Stats View**
```json
{
  "_id": "<ObjectId>",
  "view_type": "movie_stats",
  "movie_id": "<int>",
  "hour": "<ISODate>",
  "vote_average": "<double>",
  "vote_count": "<int>",
  "popularity": "<double>",
  "rating_velocity": "<double>",
  "last_updated": "<ISODate>",
  "synced_at": "<ISODate>",
  "ttl_expires_at": "<ISODate>"
}
```

**Trending Movies View**
```json
{
  "_id": "<ObjectId>",
  "view_type": "trending",
  "hour": "<ISODate>",
  "rank": "<int>",
  "movie_id": "<int>",
  "title": "<string>",
  "trending_score": "<double>",
  "velocity": "<double>",
  "acceleration": "<double>",
  "synced_at": "<ISODate>",
  "ttl_expires_at": "<ISODate>"
}
```

#### Example Document

**Review Sentiment Document**
```json
{
  "_id": ObjectId("654a3b2c1d8e9f0a1b2c3d4e"),
  "view_type": "review_sentiment",
  "movie_id": 550,
  "hour": ISODate("2025-11-16T14:00:00Z"),
  "window_start": ISODate("2025-11-16T14:30:00Z"),
  "window_end": ISODate("2025-11-16T14:35:00Z"),
  "avg_sentiment": 0.6523,
  "review_count": 23,
  "positive_count": 18,
  "negative_count": 3,
  "neutral_count": 2,
  "sentiment_velocity": 0.045,
  "synced_at": ISODate("2025-11-16T14:35:12Z"),
  "ttl_expires_at": ISODate("2025-11-18T14:35:12Z")
}
```

**Movie Stats Document**
```json
{
  "_id": ObjectId("654a3b2c1d8e9f0a1b2c3d4f"),
  "view_type": "movie_stats",
  "movie_id": 550,
  "hour": ISODate("2025-11-16T14:00:00Z"),
  "vote_average": 8.433,
  "vote_count": 28567,
  "popularity": 98.654,
  "rating_velocity": 0.12,
  "last_updated": ISODate("2025-11-16T14:45:23Z"),
  "synced_at": ISODate("2025-11-16T14:45:30Z"),
  "ttl_expires_at": ISODate("2025-11-18T14:45:30Z")
}
```

**Trending Movie Document**
```json
{
  "_id": ObjectId("654a3b2c1d8e9f0a1b2c3d50"),
  "view_type": "trending",
  "hour": ISODate("2025-11-16T14:00:00Z"),
  "rank": 1,
  "movie_id": 550,
  "title": "Fight Club",
  "trending_score": 156.327,
  "velocity": 49.327,
  "acceleration": 14.283,
  "synced_at": ISODate("2025-11-16T14:50:05Z"),
  "ttl_expires_at": ISODate("2025-11-18T14:50:05Z")
}
```

#### TTL Notes

- **TTL Duration**: 48 hours (172800 seconds)
- **TTL Field**: `ttl_expires_at` (ISODate)
- **TTL Index**: Created on `ttl_expires_at` with `expireAfterSeconds: 0`
- **Sync Frequency**: Every 5 minutes via `CassandraToMongoSync` connector
- **Cleanup**: MongoDB automatically removes expired documents
- **Purpose**: Maintains fresh speed layer data while preventing unbounded growth

**MongoDB TTL Index Creation**:
```javascript
db.speed_views.createIndex(
  { "ttl_expires_at": 1 },
  { expireAfterSeconds: 0 }
)
```

### 5.2 trending_movies (Dedicated Collection)

#### Document Structure

Optional dedicated collection for trending movies to support high-performance queries without filtering by `view_type`.

**Collection**: `trending_movies`
**Database**: `moviedb`

#### Document Template

```json
{
  "_id": "<ObjectId>",
  "hour": "<ISODate>",
  "rank": "<int:1-100>",
  "movie_id": "<int>",
  "title": "<string>",
  "trending_score": "<double>",
  "velocity": "<double>",
  "acceleration": "<double>",
  "synced_at": "<ISODate>",
  "ttl_expires_at": "<ISODate>"
}
```

#### Example Document

```json
{
  "_id": ObjectId("654a3b2c1d8e9f0a1b2c3d51"),
  "hour": ISODate("2025-11-16T14:00:00Z"),
  "rank": 1,
  "movie_id": 550,
  "title": "Fight Club",
  "trending_score": 156.327,
  "velocity": 49.327,
  "acceleration": 14.283,
  "synced_at": ISODate("2025-11-16T14:50:05Z"),
  "ttl_expires_at": ISODate("2025-11-18T14:50:05Z")
}
```

#### TTL Notes

- **TTL Duration**: 48 hours (172800 seconds)
- **TTL Index**: `{ "ttl_expires_at": 1 }, { expireAfterSeconds: 0 }`
- **Compound Index**: `{ "hour": -1, "rank": 1 }` for sorted queries
- **Use Case**: High-frequency trending queries without joins
- **Trade-off**: Duplicates data but improves query performance for trending endpoint

---

## Pipeline Data Flow Summary

```
TMDB API
    ↓ (HTTP/REST)
Kafka Producer (tmdb_stream_producer.py)
    ↓ (Avro serialization)
Kafka Topics (movie.reviews, movie.ratings, movie.metadata)
    ↓ (Avro messages)
Spark Structured Streaming Jobs
    ├─ review_sentiment_stream.py (VADER analysis)
    ├─ movie_aggregation_stream.py (Stats + velocity)
    └─ trending_detection_stream.py (Trending scores)
    ↓ (Batch writes)
Cassandra Tables (speed_layer keyspace)
    ├─ review_sentiments
    ├─ movie_stats
    └─ trending_movies
    ↓ (Sync connector)
MongoDB Collections (moviedb database)
    ├─ speed_views (unified)
    └─ trending_movies (dedicated)
    ↓ (REST API)
Serving Layer (FastAPI)
```

**Key Characteristics**:
- **Latency**: End-to-end < 10 seconds (TMDB → MongoDB)
- **Window Size**: 5-minute tumbling windows for aggregations
- **TTL**: 48 hours across all speed layer storage
- **Serialization**: Avro with Schema Registry for Kafka
- **Partitioning**: Movie-based partitioning in Cassandra
- **Indexing**: Time-based and movie-based indexes in MongoDB
