# Serving Layer - Movie Social Engagement Analytics

## 🎯 Three Business Purposes

This serving layer implements Lambda Architecture to serve three business goals:

1. **PR Crisis Detection** - Detect sentiment drops > 3σ below genre baseline
2. **Viral Content Identification** - Identify breakout content for marketing amplification  
3. **Content Recommendation** - Dual-success ranking (60% Reddit buzz + 40% TMDB quality)

## 📚 How to Test

**→ See [TESTING_GUIDE.md](TESTING_GUIDE.md) for complete testing instructions**

The testing guide shows you how to:
- Test each of the 3 business purposes
- Verify API responses and calculations
- Run automated test suites
- Troubleshoot common issues

## 🚀 Quick Start

### Start Services

```bash
# 1. Start MongoDB
cd /home/veil/Documents/GitHub/movie-data-analysis-pipeline
docker-compose up -d serving-mongodb

# 2. Start serving layer (API, Redis, Prometheus)
cd layers/serving_layer
docker-compose -f docker-compose.serving.yml up -d

# 3. Verify all services are running
docker ps --filter "name=serving-"
```

### Access Services

- **API:** http://localhost:8000
- **API Docs:** http://localhost:8000/docs (Swagger UI)
- **Health Check:** http://localhost:8000/api/v1/health
- **Prometheus:** http://localhost:9090
- **Grafana:** http://localhost:3000 (admin/admin)

### Quick Test

```bash
# Test Goal #1: Crisis Detection
curl "http://localhost:8000/api/v1/movies/The%20Flash/sentiment" | jq .

# Test Goal #2: Viral Detection
curl "http://localhost:8000/api/v1/trending/movies?limit=5" | jq .

# Test Goal #3: Recommendations
curl "http://localhost:8000/api/v1/recommendations?genre=Action&limit=5" | jq .
```

## 🏗️ Architecture

```
MongoDB batch_views          MongoDB speed_views
(TMDB Historical)            (Reddit Last 48h)
• Sentiment baselines        • Live sentiment
• Viral thresholds      →    • Upvote velocity
• Movie metadata             • Subreddit spread
        │                            │
        └─────────┬──────────────────┘
                  ↓
          ViewMerger (48h cutoff)
          • Crisis Detection
          • Viral Scoring  
          • Dual-Success Algorithm
                  ↓
          FastAPI REST API (Port 8000)
                  ↓
          Monitoring (Prometheus + Grafana)
```

## 🧪 Run Tests

```bash
# Run all tests (33 tests)
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py -v

# Run by business goal
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py::TestCrisisDetection -v
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py::TestViralScoring -v
docker exec serving-api python -m pytest /app/tests/test_api_endpoints.py::TestDualSuccessRecommendations -v
```

## 📊 Key Features

| Feature | Implementation | Performance |
|---------|---------------|-------------|
| **Crisis Detection** | 3-sigma threshold | <100ms response |
| **Viral Scoring** | Velocity/threshold coefficient | <100ms response |
| **Recommendations** | 60% Reddit + 40% TMDB | <200ms response |
| **Data Freshness** | 48-hour cutoff | 5-min sync lag |
| **Cache Hit Rate** | Redis LRU | >70% target |
| **API Throughput** | Async FastAPI | >1000 req/s |

## 🔗 Resources

- **TESTING_GUIDE.md** - Comprehensive testing instructions (→ START HERE)
- **API Documentation** - http://localhost:8000/docs
- **Grafana Dashboards** - 5 pre-built dashboards at port 3000
- **Prometheus Metrics** - http://localhost:9090

---

**Status:** ✅ Production Ready | **Last Updated:** December 15, 2025
