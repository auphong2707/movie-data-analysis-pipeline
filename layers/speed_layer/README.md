# Speed Layer - Reddit Real-Time Analytics

**Status**: ✅ **IMPLEMENTED** (Reddit-based streaming pipeline)

## Overview

The speed layer processes real-time movie discussions from Reddit to provide up-to-the-minute viral metrics and sentiment analysis. Data is automatically expired after 48 hours using Cassandra TTL.

## Architecture

```
Reddit JSON Endpoints (r/movies, r/boxoffice, r/TrueFilm)
    ↓ (30-second polling, no auth)
Reddit Producer (Python + requests)
    ↓ (Kafka: reddit.posts, reddit.comments)
Spark Structured Streaming
    ↓ (5-minute tumbling windows)
    ├─ VADER Sentiment Analysis
    ├─ Viral Metrics Calculation
    └─ Movie Title Extraction
    ↓
Cassandra (48h TTL)
    ↓ (5-minute sync)
MongoDB (speed_views collection)
```

## Quick Start

### 1. No Authentication Required! 🎉

This implementation uses Reddit's public JSON endpoints (`.json` trick), so **no API credentials are needed**.

How it works: `reddit.com/r/movies` → `reddit.com/r/movies.json` returns raw JSON data.

### 2. Configure (Optional)

```bash
cd layers/speed_layer
cp .env.example .env
# Optional: Customize REDDIT_USER_AGENT or Kafka settings
```

### 3. Run

```bash
docker-compose -f docker-compose.speed.yml up -d
```

### 4. Monitor

```bash
# View logs
docker-compose -f docker-compose.speed.yml logs -f reddit-producer
docker-compose -f docker-compose.speed.yml logs -f reddit-sentiment-stream

# Run monitoring script
docker exec -it reddit-producer python monitor.py
```

## Components

### 1. Reddit Producer (`reddit_stream_producer.py`)
- **No authentication required** (uses `.json` endpoint)
- Polls Reddit every 30 seconds
- Monitors r/movies, r/boxoffice, r/TrueFilm
- Rate limit: 1 request per 2 seconds (self-imposed)
- Publishes to Kafka

### 2. Spark Streaming (`reddit_sentiment_stream.py`)
- Consumes from reddit.posts and reddit.comments
- 5-minute tumbling windows
- VADER sentiment analysis
- Viral metrics calculation
- Movie title extraction from text

### 3. Cassandra Storage
- Tables: reddit_post_metrics, reddit_comment_metrics, speed_views
- 48-hour TTL (automatic cleanup)
- Partitioned by (movie_title, hour)

### 4. MongoDB Sync
- Syncs Cassandra → MongoDB every 5 minutes
- Target: moviedb.speed_views collection

## Metrics

**Viral Metrics**:
- Upvote velocity (upvotes/sec)
- Comment velocity (comments/sec)  
- Award velocity (awards/sec)
- Viral score (weighted combination)

**Sentiment**:
- Post sentiment (VADER compound score)
- Comment sentiment
- Combined sentiment

## Data Volume

- **r/movies**: ~500-1K posts/day, 10K-30K comments/day
- **r/boxoffice**: ~100-300 posts/day, 5K-15K comments/day
- **r/TrueFilm**: ~50-100 posts/day, 2K-5K comments/day

**Total**: ~2K-4K posts/day, 20K-60K comments/day

## Testing

```bash
# Check Kafka topics
docker exec -it speed-kafka kafka-topics --list --bootstrap-server localhost:9092

# Query Cassandra
docker exec -it speed-cassandra cqlsh -e "SELECT * FROM speed_layer.speed_views LIMIT 10;"

# Check MongoDB
docker exec -it mongodb mongosh --eval "db.speed_views.find().limit(5)"
```

## Troubleshooting

**No data in Cassandra**:
- Check Reddit producer logs
- Verify Reddit is not blocking requests (check User-Agent)
- Check Kafka topics have messages
- Try reducing polling frequency if rate limited

**High Kafka lag**:
- Scale Spark streaming (increase executors)
- Check Cassandra write performance

**Movie extraction issues**:
- Check logs for regex patterns
- Verify potential_movies field is populated
- Adjust extraction logic if needed
