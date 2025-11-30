# Schema Requirements - MongoDB Collections

## 📋 Tổng Quan

Document này mô tả **chi tiết format và features** cần có trong collections `batch_views` và `speed_views` để Serving Layer hoạt động đúng với các API endpoints.

---

## 🗄️ Collection: `batch_views`

Collection này lưu **pre-computed views từ Batch Layer** (dữ liệu lịch sử > 48 giờ).

### Common Fields (Tất cả documents)

```javascript
{
  "_id": ObjectId("..."),
  "movie_id": 12345,              // REQUIRED - TMDB movie ID
  "view_type": "...",             // REQUIRED - Loại view (xem bên dưới)
  "computed_at": ISODate("..."),  // REQUIRED - Thời điểm tính toán
  "batch_run_id": "...",          // OPTIONAL - ID của batch job
  "version": 1                    // OPTIONAL - Schema version
}
```

---

### View Type 1: `movie_details`

**Mục đích**: Thông tin chi tiết về phim

**Schema**:
```javascript
{
  "movie_id": 12345,
  "view_type": "movie_details",
  "data": {
    "title": "The Great Movie",           // REQUIRED
    "release_date": "2025-06-15",         // REQUIRED
    "genres": ["Action", "Thriller"],     // REQUIRED - Array of strings
    "vote_average": 7.8,                  // REQUIRED - Float
    "vote_count": 15234,                  // REQUIRED - Integer
    "popularity": 89.5,                   // REQUIRED - Float
    "runtime": 142,                       // OPTIONAL - Minutes
    "budget": 150000000,                  // OPTIONAL - USD
    "revenue": 500000000,                 // OPTIONAL - USD
    "overview": "Movie description...",   // OPTIONAL
    "original_language": "en"             // OPTIONAL
  },
  "computed_at": ISODate("2025-10-15T10:00:00Z")
}
```

**Sử dụng trong API**:
- `GET /movies/{movie_id}` - Lấy thông tin phim
- `GET /trending/movies` - Enrich metadata (title, genres)

**Required Fields**:
- ✅ `movie_id`
- ✅ `view_type` = "movie_details"
- ✅ `data.title`
- ✅ `data.genres` (array)
- ✅ `data.vote_average`
- ✅ `data.vote_count`
- ✅ `data.popularity`

---

### View Type 2: `sentiment`

**Mục đích**: Phân tích sentiment từ reviews (dữ liệu lịch sử)

**Schema**:
```javascript
{
  "movie_id": 12345,
  "view_type": "sentiment",
  "data": {
    "avg_sentiment": 0.75,        // REQUIRED - Score từ -1 (negative) đến 1 (positive)
    "review_count": 1200,         // REQUIRED - Tổng số reviews
    "positive_count": 850,        // REQUIRED - Số reviews positive
    "negative_count": 120,        // REQUIRED - Số reviews negative
    "neutral_count": 230,         // REQUIRED - Số reviews neutral
    "sentiment_velocity": 0.02,   // OPTIONAL - Tốc độ thay đổi sentiment
    "confidence": 0.92            // OPTIONAL - Độ tin cậy của phân tích
  },
  "computed_at": ISODate("2025-10-15T10:00:00Z")
}
```

**Sử dụng trong API**:
- `GET /movies/{movie_id}/sentiment` - Phân tích sentiment

**Required Fields**:
- ✅ `movie_id`
- ✅ `view_type` = "sentiment"
- ✅ `data.avg_sentiment` (float -1 to 1)
- ✅ `data.review_count` (integer)
- ✅ `data.positive_count` (integer)
- ✅ `data.negative_count` (integer)
- ✅ `data.neutral_count` (integer)

---

### View Type 3: `genre_analytics`

**Mục đích**: Thống kê theo genre

**Schema**:
```javascript
{
  "view_type": "genre_analytics",
  "genre": "Action",                  // REQUIRED
  "year": 2025,                       // REQUIRED
  "month": 10,                        // OPTIONAL (1-12)
  "total_movies": 150,                // REQUIRED
  "avg_rating": 7.5,                  // REQUIRED
  "avg_sentiment": 0.65,              // REQUIRED
  "avg_popularity": 75.2,             // REQUIRED
  "total_revenue": 5000000000,        // OPTIONAL
  "avg_budget": 80000000,             // OPTIONAL
  "avg_runtime": 128,                 // OPTIONAL
  "top_movies": [                     // OPTIONAL
    {
      "movie_id": 12345,
      "title": "Top Action Movie",
      "vote_average": 9.1,
      "revenue": 850000000
    }
  ],
  "computed_at": ISODate("2025-10-15T10:00:00Z")
}
```

**Sử dụng trong API**:
- `GET /analytics/genre/{genre}` - Thống kê theo genre
- `GET /analytics/overview` - Tổng quan tất cả genres

**Required Fields**:
- ✅ `view_type` = "genre_analytics"
- ✅ `genre` (string)
- ✅ `year` (integer)
- ✅ `total_movies` (integer)
- ✅ `avg_rating` (float)
- ✅ `avg_sentiment` (float)
- ✅ `avg_popularity` (float)

---

### View Type 4: `temporal_trends`

**Mục đích**: Xu hướng theo thời gian

**Schema**:
```javascript
{
  "view_type": "temporal_trends",
  "data": {
    "metric": "rating",           // REQUIRED - rating/sentiment/popularity
    "value": 7.5,                 // REQUIRED - Giá trị metric
    "count": 234,                 // REQUIRED - Số lượng data points
    "date": "2025-10-15",         // REQUIRED - Ngày
    "genre": "Action",            // OPTIONAL - Filter by genre
    "movie_id": 12345            // OPTIONAL - Filter by movie
  },
  "computed_at": ISODate("2025-10-15T10:00:00Z")
}
```

**Sử dụng trong API**:
- `GET /analytics/trends` - Phân tích xu hướng

**Required Fields**:
- ✅ `view_type` = "temporal_trends"
- ✅ `data.metric` (string: rating/sentiment/popularity)
- ✅ `data.value` (float)
- ✅ `data.count` (integer)
- ✅ `data.date` (string ISO date)

---

### Indexes cho `batch_views`

```javascript
// Primary indexes
db.batch_views.createIndex({ "movie_id": 1, "view_type": 1 })
db.batch_views.createIndex({ "view_type": 1, "computed_at": -1 })
db.batch_views.createIndex({ "computed_at": -1 })

// Genre analytics indexes
db.batch_views.createIndex({ "view_type": 1, "genre": 1, "year": 1 })

// Temporal trends indexes
db.batch_views.createIndex({ "view_type": 1, "data.metric": 1, "computed_at": 1 })
```

---

## ⚡ Collection: `speed_views`

Collection này lưu **real-time data từ Speed Layer** (dữ liệu trong 48 giờ gần nhất).

### Common Fields (Tất cả documents)

```javascript
{
  "_id": ObjectId("..."),
  "movie_id": 12345,              // REQUIRED - TMDB movie ID
  "data_type": "...",             // REQUIRED - Loại data (stats/sentiment)
  "hour": ISODate("..."),         // REQUIRED - Timestamp (rounded to hour)
  "synced_at": ISODate("..."),    // OPTIONAL - Thời điểm sync từ Cassandra
  "ttl_expires_at": ISODate("...") // REQUIRED - TTL expiration (48h sau)
}
```

---

### Data Type 1: `stats`

**Mục đích**: Thống kê real-time về phim

**Schema**:
```javascript
{
  "movie_id": 12345,
  "data_type": "stats",
  "hour": ISODate("2025-10-17T14:00:00Z"),
  "stats": {
    "vote_average": 7.8,              // REQUIRED - Rating hiện tại
    "vote_count": 15234,              // REQUIRED - Số votes
    "popularity": 89.5,               // REQUIRED - Popularity score
    "rating_velocity": 0.05,          // REQUIRED - Tốc độ thay đổi rating
    "popularity_velocity": 2.3,       // OPTIONAL - Tốc độ thay đổi popularity
    "vote_velocity": 120,             // OPTIONAL - Tốc độ tăng vote
    "trending_score": 98.5            // OPTIONAL - Điểm trending tổng hợp
  },
  "synced_at": ISODate("2025-10-17T14:05:00Z"),
  "ttl_expires_at": ISODate("2025-10-19T14:00:00Z")
}
```

**Sử dụng trong API**:
- `GET /movies/{movie_id}` - Stats real-time
- `GET /movies/{movie_id}/stats` - Lịch sử stats gần đây
- `GET /trending/movies` - Tính trending score

**Required Fields**:
- ✅ `movie_id`
- ✅ `data_type` = "stats"
- ✅ `hour` (ISODate)
- ✅ `stats.vote_average` (float)
- ✅ `stats.vote_count` (integer)
- ✅ `stats.popularity` (float)
- ✅ `stats.rating_velocity` (float)
- ✅ `ttl_expires_at` (ISODate)

---

### Data Type 2: `sentiment`

**Mục đích**: Sentiment analysis real-time

**Schema**:
```javascript
{
  "movie_id": 12345,
  "data_type": "sentiment",
  "hour": ISODate("2025-10-17T14:00:00Z"),
  "data": {
    "avg_sentiment": 0.75,        // REQUIRED - Score -1 to 1
    "review_count": 45,           // REQUIRED - Số reviews trong giờ này
    "positive_count": 30,         // REQUIRED
    "negative_count": 10,         // REQUIRED
    "neutral_count": 5,           // REQUIRED
    "sentiment_velocity": 0.02    // REQUIRED - Tốc độ thay đổi sentiment
  },
  "synced_at": ISODate("2025-10-17T14:05:00Z"),
  "ttl_expires_at": ISODate("2025-10-19T14:00:00Z")
}
```

**Sử dụng trong API**:
- `GET /movies/{movie_id}/sentiment` - Real-time sentiment

**Required Fields**:
- ✅ `movie_id`
- ✅ `data_type` = "sentiment"
- ✅ `hour` (ISODate)
- ✅ `data.avg_sentiment` (float -1 to 1)
- ✅ `data.review_count` (integer)
- ✅ `data.positive_count` (integer)
- ✅ `data.negative_count` (integer)
- ✅ `data.neutral_count` (integer)
- ✅ `data.sentiment_velocity` (float)
- ✅ `ttl_expires_at` (ISODate)

---

### Indexes cho `speed_views`

```javascript
// Primary indexes
db.speed_views.createIndex({ "movie_id": 1, "data_type": 1, "hour": -1 })
db.speed_views.createIndex({ "data_type": 1, "hour": -1 })

// TTL index (auto-delete after 48 hours)
db.speed_views.createIndex(
  { "ttl_expires_at": 1 }, 
  { expireAfterSeconds: 0 }
)

// Trending queries
db.speed_views.createIndex({ "data_type": 1, "stats.trending_score": -1 })
```

---

## 📊 API Endpoint Mapping

### GET `/movies/{movie_id}`

**Cần từ batch_views**:
```javascript
{
  "view_type": "movie_details",
  "movie_id": ...,
  "data": {
    "title": "...",
    "genres": [...],
    "vote_average": ...,
    "popularity": ...
  }
}
```

**Cần từ speed_views**:
```javascript
{
  "data_type": "stats",
  "movie_id": ...,
  "stats": {
    "vote_average": ...,
    "vote_count": ...,
    "popularity": ...
  }
}
```

---

### GET `/movies/{movie_id}/sentiment`

**Cần từ batch_views**:
```javascript
{
  "view_type": "sentiment",
  "movie_id": ...,
  "data": {
    "avg_sentiment": ...,
    "review_count": ...,
    "positive_count": ...,
    "negative_count": ...,
    "neutral_count": ...
  }
}
```

**Cần từ speed_views**:
```javascript
{
  "data_type": "sentiment",
  "movie_id": ...,
  "data": {
    "avg_sentiment": ...,
    "review_count": ...,
    "positive_count": ...,
    "negative_count": ...,
    "neutral_count": ...,
    "sentiment_velocity": ...
  }
}
```

---

### GET `/trending/movies`

**Cần từ speed_views**:
```javascript
{
  "data_type": "stats",
  "movie_id": ...,
  "hour": ISODate("..."),
  "stats": {
    "popularity": ...,
    "vote_average": ...,
    "vote_count": ...,
    "rating_velocity": ...,
    "trending_score": ...  // Hoặc tính từ các metrics khác
  }
}
```

**Enrich metadata từ batch_views**:
```javascript
{
  "view_type": "movie_details",
  "movie_id": ...,
  "data": {
    "title": "...",
    "genres": [...]
  }
}
```

---

### GET `/analytics/genre/{genre}`

**Cần từ batch_views**:
```javascript
{
  "view_type": "genre_analytics",
  "genre": "Action",
  "year": 2025,
  "total_movies": ...,
  "avg_rating": ...,
  "avg_sentiment": ...,
  "avg_popularity": ...
}
```

**Bổ sung từ speed_views** (aggregate on-the-fly):
```javascript
// Aggregate all movies in last 48h for recent stats
{
  "data_type": "stats",
  "stats": {
    "vote_average": ...,
    "popularity": ...
  }
}
```

---

### GET `/analytics/trends`

**Cần từ batch_views**:
```javascript
{
  "view_type": "temporal_trends",
  "data": {
    "metric": "rating", // or "sentiment" or "popularity"
    "value": ...,
    "count": ...,
    "date": "..."
  }
}
```

---

## 🔄 48-Hour Cutoff Strategy

### Logic

```
Current Time: 2025-10-17 14:00:00
Cutoff Time:  2025-10-15 14:00:00 (48 hours ago)

Query Strategy:
├── Historical Data (> 48h ago)
│   └── Source: batch_views
│       └── WHERE computed_at < cutoff_time
│
└── Recent Data (≤ 48h)
    └── Source: speed_views
        └── WHERE hour >= cutoff_time
```

### Merge Logic

```python
# 1. Query cả 2 sources
batch_data = batch_views.find({
    "movie_id": 12345,
    "computed_at": {"$lt": cutoff_time}
})

speed_data = speed_views.find({
    "movie_id": 12345,
    "hour": {"$gte": cutoff_time}
})

# 2. Merge: Speed takes precedence
merged = []
merged.extend(speed_data)  # Add all speed data first

for batch_doc in batch_data:
    if batch_doc.timestamp not in speed_timestamps:
        merged.append(batch_doc)  # Only add if not overlapping

# 3. Sort by timestamp (newest first)
merged.sort(key=lambda x: x['timestamp'], reverse=True)
```

---

## ✅ Checklist: Batch Layer Output

Batch Layer cần export vào MongoDB các documents sau:

- [ ] **movie_details** view
  - movie_id, title, genres, vote_average, popularity
  
- [ ] **sentiment** view
  - movie_id, avg_sentiment, review counts
  
- [ ] **genre_analytics** view
  - genre, year, statistics
  
- [ ] **temporal_trends** view
  - metric, value, count, date

---

## ✅ Checklist: Speed Layer Output

Speed Layer cần sync từ Cassandra sang MongoDB:

- [ ] **stats** data type
  - movie_id, hour, vote_average, popularity, velocities
  
- [ ] **sentiment** data type  
  - movie_id, hour, avg_sentiment, review counts, velocity

- [ ] **TTL setup**
  - Tất cả documents phải có `ttl_expires_at` = hour + 48h

---

## 🎯 Validation Queries

### Kiểm tra Batch Views có đủ data không

```javascript
// Check movie_details
db.batch_views.find({
  "view_type": "movie_details"
}).limit(5)

// Check sentiment
db.batch_views.find({
  "view_type": "sentiment"
}).limit(5)

// Check genre analytics
db.batch_views.find({
  "view_type": "genre_analytics"
}).limit(5)

// Check temporal trends
db.batch_views.find({
  "view_type": "temporal_trends"
}).limit(5)
```

### Kiểm tra Speed Views có data gần đây không

```javascript
// Check recent stats (last 48h)
db.speed_views.find({
  "data_type": "stats",
  "hour": {"$gte": new Date(Date.now() - 48*60*60*1000)}
}).limit(5)

// Check recent sentiment
db.speed_views.find({
  "data_type": "sentiment",
  "hour": {"$gte": new Date(Date.now() - 48*60*60*1000)}
}).limit(5)

// Count by data_type
db.speed_views.aggregate([
  {$group: {_id: "$data_type", count: {$sum: 1}}}
])
```

---

## 📝 Sample Data Examples

### Batch Views Sample

```javascript
// Insert sample movie_details
db.batch_views.insertOne({
  "movie_id": 550,
  "view_type": "movie_details",
  "data": {
    "title": "Fight Club",
    "release_date": "1999-10-15",
    "genres": ["Drama"],
    "vote_average": 8.4,
    "vote_count": 26280,
    "popularity": 61.416,
    "runtime": 139
  },
  "computed_at": new Date("2025-10-15T10:00:00Z"),
  "batch_run_id": "batch_2025_10_15"
})

// Insert sample sentiment
db.batch_views.insertOne({
  "movie_id": 550,
  "view_type": "sentiment",
  "data": {
    "avg_sentiment": 0.82,
    "review_count": 5430,
    "positive_count": 4820,
    "negative_count": 310,
    "neutral_count": 300
  },
  "computed_at": new Date("2025-10-15T10:00:00Z")
})
```

### Speed Views Sample

```javascript
// Insert sample stats
db.speed_views.insertOne({
  "movie_id": 550,
  "data_type": "stats",
  "hour": new Date("2025-10-17T14:00:00Z"),
  "stats": {
    "vote_average": 8.43,
    "vote_count": 26350,
    "popularity": 65.2,
    "rating_velocity": 0.03,
    "popularity_velocity": 3.8
  },
  "synced_at": new Date("2025-10-17T14:05:00Z"),
  "ttl_expires_at": new Date("2025-10-19T14:00:00Z")
})

// Insert sample sentiment
db.speed_views.insertOne({
  "movie_id": 550,
  "data_type": "sentiment",
  "hour": new Date("2025-10-17T14:00:00Z"),
  "data": {
    "avg_sentiment": 0.85,
    "review_count": 12,
    "positive_count": 10,
    "negative_count": 1,
    "neutral_count": 1,
    "sentiment_velocity": 0.03
  },
  "synced_at": new Date("2025-10-17T14:05:00Z"),
  "ttl_expires_at": new Date("2025-10-19T14:00:00Z")
})
```

---

## 🚀 Next Steps

1. **Batch Layer Team**: Implement export logic theo schema trên
2. **Speed Layer Team**: Implement Cassandra → MongoDB sync theo schema trên
3. **Testing**: Validate với sample data trước khi run production
4. **Monitoring**: Set up alerts cho data freshness và completeness

---

**Last Updated**: 2025-11-16
