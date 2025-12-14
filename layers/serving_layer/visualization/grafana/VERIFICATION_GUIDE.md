# Grafana Dashboard Verification Guide

## Overview
This guide helps verify that all 5 Grafana dashboards work correctly with the refactored serving layer API endpoints.

## Prerequisites
- Docker and Docker Compose installed
- Serving layer API running on port 8000
- MongoDB and Redis accessible

## Starting Grafana

```bash
cd /home/veil/Documents/GitHub/movie-data-analysis-pipeline/layers/serving_layer/visualization/grafana
./start-grafana.sh
```

Access Grafana at: http://localhost:3000
- Default credentials: admin/admin

## Dashboard Verification Checklist

### 1. System Health Dashboard (`system-health-dashboard.json`)
**Status:** ✅ Should work as-is

**Panels to verify:**
- [ ] API request rate
- [ ] API latency (p50, p95, p99)
- [ ] Error rate
- [ ] Active connections
- [ ] MongoDB connection status
- [ ] Redis cache status

**Expected datasource:** Prometheus + API datasource

---

### 2. Data Freshness Dashboard (`data-freshness-dashboard.json`)
**Status:** ⚠️ May need modifications

**Panels to verify:**
- [ ] Batch layer last update time
- [ ] Speed layer sync lag
- [ ] Data staleness alerts

**Potential issues:**
- May query removed `/analytics/trends` endpoint
- Check if it queries TMDB popularity fields

**Action items:**
- Update panels to use `/analytics/genre/{genre}` for batch data
- Query speed layer freshness via custom metrics

---

### 3. Genre Analytics Dashboard (`genre-analytics-dashboard.json`)
**Status:** ⚠️ Needs modification

**Current queries likely broken:**
- Revenue/budget aggregations (removed in Phase 6)
- TMDB popularity tracking (not in our schema)

**Replacement queries:**
- `GET /analytics/genre/{genre}` - Returns sentiment_baseline + viral_threshold
- Query Prometheus for business metrics

**Action items:**
- Remove revenue/budget panels
- Add sentiment baseline visualization
- Add viral threshold comparison panels

---

### 4. Movie Analytics Overview Dashboard (`movie-analytics-overview.json`)
**Status:** ⚠️ Needs modification

**Likely issues:**
- Queries TMDB popularity velocity (doesn't exist)
- May use removed `/analytics/trends` endpoint

**Replacement strategy:**
- Use Prometheus metrics for crisis_alerts_total
- Use `/trending/movies` for viral content (now uses Reddit viral coefficient)
- Use custom business metrics

**Action items:**
- Replace popularity panels with viral coefficient panels
- Add crisis detection rate visualization
- Add dual-success recommendation metrics

---

### 5. Trending Movies Dashboard (`trending-movies.json`)
**Status:** ⚠️ Needs modification

**API endpoint changed:**
- Old: `GET /trending/movies` (TMDB popularity sorting)
- New: `GET /trending/movies` (Reddit viral coefficient)

**Response schema changed:**
```json
{
  "viral_movies": [{
    "viral_metrics": {
      "viral_coefficient": 1.67,
      "upvote_velocity": 500.2,
      "comment_velocity": 82.5
    },
    "reddit_stats": {
      "total_upvotes": 12000,
      "subreddit_count": 8
    }
  }]
}
```

**Action items:**
- Update panels to display viral_coefficient instead of popularity
- Add Reddit engagement metrics (upvotes, comments, subreddits)
- Visualize viral threshold comparison

---

## Testing Each Dashboard

### Step 1: Verify API Endpoints
```bash
# Test crisis detection
curl http://localhost:8000/api/v1/movies/Dune%202/sentiment

# Test viral detection
curl http://localhost:8000/api/v1/trending/movies?limit=10

# Test analytics
curl http://localhost:8000/api/v1/analytics/genre/Action

# Test metrics endpoint
curl http://localhost:8000/metrics
```

### Step 2: Check Prometheus Targets
1. Open http://localhost:9090/targets
2. Verify all targets are UP:
   - fastapi (serving-api:8000)
   - mongodb (mongodb-exporter:9216)
   - redis (redis-exporter:9121)

### Step 3: Test Each Dashboard
1. Open Grafana: http://localhost:3000
2. Navigate to Dashboards
3. For each dashboard:
   - Check for panel errors (red border)
   - Verify data is loading
   - Check time range selector works
   - Test dashboard variables/filters

### Step 4: Fix Broken Panels

If a panel shows "No data":
1. Click panel title → Edit
2. Check the query syntax
3. Update datasource if needed
4. Modify query to use correct endpoints:
   - Remove references to `/analytics/trends`
   - Remove popularity/revenue queries
   - Add viral_coefficient queries

---

## Dashboard Modification Guide

### Updating API Queries in Grafana

**Old query (broken):**
```
GET /analytics/trends?metric=popularity&window=30d
```

**New query (works):**
```
GET /trending/movies?viral_coefficient_threshold=1.0&limit=20
```

### Adding Prometheus Queries

**Crisis alerts:**
```promql
rate(crisis_alerts_total[1h])
```

**Viral detections by genre:**
```promql
rate(viral_detections_total[1h]) by (genre)
```

**API latency:**
```promql
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))
```

---

## Common Issues and Solutions

### Issue 1: "Cannot connect to datasource"
**Solution:**
- Check docker-compose network configuration
- Verify API is running: `docker ps | grep serving-api`
- Check API logs: `docker logs serving-api`

### Issue 2: "No data points" in panels
**Solution:**
- Verify time range is correct (last 24 hours)
- Check if API endpoint exists: `curl http://localhost:8000/docs`
- Test query directly: `curl http://localhost:8000/api/v1/[endpoint]`

### Issue 3: Panel shows old data structure
**Solution:**
- Edit panel → Update query to use new response schema
- Check REDUNDANT_COMPONENTS_TO_REMOVE.md for schema changes
- Refer to SERVING_LAYER_REVISION_PLAN.md for new endpoints

---

## Verification Commands

```bash
# Check all containers are running
docker-compose -f docker-compose.serving.yml ps

# Check Grafana logs
docker logs serving-grafana

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Test API health
curl http://localhost:8000/api/v1/health

# View Prometheus metrics
curl http://localhost:8000/metrics | grep crisis_alerts
curl http://localhost:8000/metrics | grep viral_detections
```

---

## Phase 11 Completion Criteria

- [ ] All 5 dashboards load without errors
- [ ] System health dashboard shows real-time metrics
- [ ] Genre analytics shows sentiment baseline + viral threshold
- [ ] Movie overview shows business metrics (crisis, viral, recommendations)
- [ ] Trending dashboard displays Reddit viral coefficient
- [ ] No references to removed endpoints (/analytics/trends)
- [ ] No queries for non-existent fields (popularity, revenue, budget)
- [ ] Prometheus datasource working
- [ ] API datasource (Infinity) working

---

## Next Steps After Verification

1. Document any dashboard modifications made
2. Export updated dashboard JSONs
3. Commit changes to git
4. Update visualization/README.md with new dashboard descriptions
5. Create screenshots of working dashboards for documentation
