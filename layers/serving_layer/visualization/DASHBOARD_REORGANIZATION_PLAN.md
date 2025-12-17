# Dashboard Reorganization Plan

**Date:** December 17, 2025  
**Purpose:** Reorganize Grafana dashboards to align with the three business goals of the Movie Social Engagement Analytics Pipeline

---

## 🎯 Current State Analysis

### Existing Dashboards (9 total):
1. `business-kpi.json` - Mixed business metrics
2. `data-freshness-dashboard.json` - Batch/speed layer sync monitoring
3. `genre-analytics-dashboard.json` - Genre-specific analytics
4. `movie-analytics-overview.json` - High-level movie metrics (⚠️ uses non-existent `/api/v1/analytics/overview` endpoint)
5. `pr-crisis-detection.json` - Sentiment crisis alerts
6. `recommendation-performance.json` - Recommendation system metrics
7. `system-health-dashboard.json` - API/DB/Cache health
8. `trending-movies.json` - Trending content tracking
9. `viral-content.json` - Viral content identification

### Issues with Current Setup:
- ❌ Fragmented: Related metrics split across multiple dashboards
- ❌ Redundancy: Similar panels duplicated (e.g., API health in multiple places)
- ❌ Missing alignment: Dashboards don't clearly map to business goals
- ❌ Broken dashboard: `movie-analytics-overview.json` queries non-existent endpoint
- ❌ Naming confusion: "Business KPI" is too vague
- ❌ Hard to navigate: 9 dashboards is too many for focused decision-making

---

## 🎯 Proposed New Structure (4 Dashboards)

### **Dashboard #1: 🚨 PR Crisis Detection & Sentiment Monitoring**

**Purpose:** Real-time monitoring of movie sentiment to detect PR crises before they escalate

**Target Users:** PR teams, marketing managers, crisis response teams

**Key Metrics:**
- Sentiment score vs. baseline comparison
- Sentiment velocity (rate of change)
- Crisis alert status (warning/critical)
- Sentiment breakdown by hour (last 48h)
- Movies in crisis state (table)
- Genre-specific sentiment baselines

**Panels to Migrate:**

| Source Dashboard | Panel Name | Metric/Query | Priority |
|------------------|------------|--------------|----------|
| `pr-crisis-detection.json` | Current Sentiment Score | Overall sentiment gauge | HIGH |
| `pr-crisis-detection.json` | Sentiment vs Baseline | Time series comparison | HIGH |
| `pr-crisis-detection.json` | Crisis Alerts Summary | `crisis_alerts_total{severity}` | HIGH |
| `pr-crisis-detection.json` | Sentiment Velocity | Rate of change calculation | HIGH |
| `pr-crisis-detection.json` | Movies in Crisis | Table of flagged movies | HIGH |
| `business-kpi.json` | Business Alerts Summary | `crisis_alerts_total` by severity | MEDIUM |
| `genre-analytics-dashboard.json` | Sentiment by Genre | Genre baseline comparison | MEDIUM |
| `system-health-dashboard.json` | MongoDB Latency | DB performance for alerts | LOW |

**New Panels to Add:**
- ⭐ Sentiment drop alerts (last 24h) - Counter with threshold
- ⭐ Average response time to crisis - Histogram showing PR team reaction time
- ⭐ Crisis severity distribution - Pie chart (warning/critical/resolved)
- ⭐ Top 5 movies with sentiment drops - Ranked table
- ⭐ Sentiment volatility index - Standard deviation over time
- ⭐ Crisis resolution timeline - Time series showing crisis lifecycle

**Data Sources:**
- Prometheus: `crisis_alerts_total`, `sentiment_drop_events`, `avg_sentiment`
- Infinity (API): `/api/v1/movies/{id}/sentiment` for detailed breakdowns

**Layout:**
```
Row 1: [Current Crisis Count] [Avg Sentiment Score] [Crisis Alerts (24h)] [Avg Response Time]
Row 2: [Sentiment vs Baseline (Time Series - Full Width)]
Row 3: [Sentiment Velocity] [Crisis Severity Distribution]
Row 4: [Movies in Crisis State (Table - Full Width)]
Row 5: [Sentiment by Genre Baseline] [Top 5 Sentiment Drops]
```

---

### **Dashboard #2: 🔥 Viral Content Identification & Tracking**

**Purpose:** Identify breakout viral content for marketing amplification opportunities

**Target Users:** Marketing teams, social media managers, content strategists

**Key Metrics:**
- Viral coefficient (velocity vs. threshold)
- Cross-subreddit spread
- Engagement velocity (upvotes/comments/awards per hour)
- Viral status breakdown (VIRAL/TRENDING/NORMAL)
- Top viral movies ranking
- Viral trend prediction

**Panels to Migrate:**

| Source Dashboard | Panel Name | Metric/Query | Priority |
|------------------|------------|--------------|----------|
| `viral-content.json` | Viral Coefficient Over Time | `viral_coefficient` time series | HIGH |
| `viral-content.json` | Movies Exceeding Threshold | Count of viral movies | HIGH |
| `viral-content.json` | Cross-Subreddit Spread | Heatmap of subreddit activity | HIGH |
| `viral-content.json` | Viral Status Breakdown | Pie chart (VIRAL/TRENDING/NORMAL) | HIGH |
| `trending-movies.json` | Top Trending Movies | Ranked table by engagement | HIGH |
| `trending-movies.json` | Engagement Velocity | Upvote/comment velocity trends | HIGH |
| `trending-movies.json` | Trending Score Distribution | Histogram of trending scores | MEDIUM |
| `business-kpi.json` | Request Rate (for trending endpoint) | `http_requests_total{endpoint="/trending"}` | LOW |

**New Panels to Add:**
- ⭐ Viral acceleration - Second derivative showing momentum
- ⭐ Subreddit diversity score - Entropy measure of spread
- ⭐ Viral half-life prediction - ML prediction of viral decay
- ⭐ Engagement type breakdown - Stacked bar (upvotes/comments/awards)
- ⭐ Viral content by genre - Compare viral potential across genres
- ⭐ Marketing opportunity score - Combined metric (viral coef × sentiment × spread)

**Data Sources:**
- Prometheus: `viral_detections_total`, `upvote_velocity`, `comment_velocity`
- Infinity (API): `/api/v1/trending/movies` for detailed rankings

**Layout:**
```
Row 1: [Viral Movies Count] [Avg Viral Coefficient] [Subreddit Spread] [Marketing Opportunities]
Row 2: [Viral Coefficient Over Time (Time Series - Full Width)]
Row 3: [Engagement Velocity (Upvotes)] [Engagement Velocity (Comments)]
Row 4: [Cross-Subreddit Spread Heatmap - Full Width]
Row 5: [Top 10 Viral Movies (Table)] [Viral Status Breakdown (Pie)]
Row 6: [Viral Content by Genre] [Marketing Opportunity Score]
```

---

### **Dashboard #3: 🎯 Recommendation Performance & Optimization**

**Purpose:** Monitor and optimize the dual-success recommendation algorithm (60% Reddit + 40% TMDB)

**Target Users:** Data scientists, product managers, content curators

**Key Metrics:**
- Dual-success score distribution
- Reddit buzz vs TMDB quality scatter plot
- Recommendations served by genre
- Click-through rate (if available)
- Recommendation freshness (speed layer contribution)
- Algorithm performance metrics

**Panels to Migrate:**

| Source Dashboard | Panel Name | Metric/Query | Priority |
|------------------|------------|--------------|----------|
| `recommendation-performance.json` | Dual-Success Score Distribution | Histogram of scores | HIGH |
| `recommendation-performance.json` | Recommendations by Genre | Bar chart by genre | HIGH |
| `recommendation-performance.json` | Reddit vs TMDB Weight | Pie chart showing 60/40 split | HIGH |
| `recommendation-performance.json` | Recommendation Response Time | `http_request_duration{endpoint="/recommendations"}` | HIGH |
| `recommendation-performance.json` | Cache Hit Rate | Cache effectiveness for recs | MEDIUM |
| `business-kpi.json` | Top Endpoints (filter for /recommendations) | Request count | MEDIUM |
| `genre-analytics-dashboard.json` | Genre Distribution | Genre popularity | LOW |

**New Panels to Add:**
- ⭐ Reddit vs TMDB scatter plot - Visual 2D distribution
- ⭐ Dual-success score by genre - Compare algorithm effectiveness
- ⭐ Speed layer freshness - % of recommendations using real-time data
- ⭐ Recommendation diversity - Genre entropy in results
- ⭐ A/B test results - Compare different weight configurations
- ⭐ User engagement metrics - Click-through, dwell time (if tracked)
- ⭐ Algorithm bias detection - Check for genre/age bias

**Data Sources:**
- Prometheus: `recommendation_requests_total`, `dual_success_score_bucket`
- Infinity (API): `/api/v1/recommendations` for live recommendations

**Layout:**
```
Row 1: [Total Recommendations] [Avg Dual-Success Score] [Cache Hit Rate] [P95 Response Time]
Row 2: [Dual-Success Score Distribution (Histogram - Full Width)]
Row 3: [Reddit vs TMDB Scatter Plot] [Algorithm Weight Breakdown]
Row 4: [Recommendations by Genre (Bar Chart - Full Width)]
Row 5: [Speed Layer Freshness] [Recommendation Diversity]
Row 6: [Dual-Success Score by Genre] [A/B Test Results]
```

---

### **Dashboard #4: ⚙️ System Health & Infrastructure Monitoring**

**Purpose:** Monitor API performance, database health, and data pipeline sync

**Target Users:** DevOps engineers, SREs, backend developers

**Key Metrics:**
- API request rate, latency, error rate
- MongoDB connection health, query latency
- Redis cache hit rate, memory usage
- Batch layer last update timestamp
- Speed layer last update timestamp
- Data staleness (seconds since last update)

**Panels to Migrate:**

| Source Dashboard | Panel Name | Metric/Query | Priority |
|------------------|------------|--------------|----------|
| `system-health-dashboard.json` | API Request Rate | `rate(http_requests_total[5m])` | HIGH |
| `system-health-dashboard.json` | API Response Time (P95) | `http_request_duration_seconds{quantile="0.95"}` | HIGH |
| `system-health-dashboard.json` | API Success Rate | `http_requests_total{status="200"}` | HIGH |
| `system-health-dashboard.json` | MongoDB Latency | MongoDB ping time | HIGH |
| `system-health-dashboard.json` | Redis Cache Hit Rate | Cache effectiveness | HIGH |
| `system-health-dashboard.json` | Memory Usage | `process_resident_memory_bytes` | MEDIUM |
| `data-freshness-dashboard.json` | Batch Layer Last Update | Timestamp of last batch run | HIGH |
| `data-freshness-dashboard.json` | Speed Layer Last Update | Timestamp of last speed update | HIGH |
| `data-freshness-dashboard.json` | Data Staleness | Time since last update | HIGH |
| `data-freshness-dashboard.json` | Movies with Speed Data | Count of movies with real-time data | MEDIUM |
| `data-freshness-dashboard.json` | Speed/Batch Coverage Ratio | % of movies with speed data | MEDIUM |
| `business-kpi.json` | Total Movies (Batch) | Batch layer document count | MEDIUM |
| `business-kpi.json` | Speed Layer Documents | Speed layer document count | MEDIUM |
| `business-kpi.json` | API Health | `up{job="fastapi"}` | HIGH |

**New Panels to Add:**
- ⭐ Request rate by endpoint - Stacked area chart
- ⭐ Error rate by endpoint - Identify problem endpoints
- ⭐ Database connection pool - Active/idle connections
- ⭐ Cache memory usage - Redis memory tracking
- ⭐ Data pipeline lag - Batch vs speed layer sync delay
- ⭐ API availability (uptime %) - SLA tracking
- ⭐ Slowest endpoints (P99) - Performance bottleneck identification

**Data Sources:**
- Prometheus: All metrics from `http_*`, `mongodb_*`, `redis_*`, `process_*`
- Infinity (API): `/api/v1/health` for aggregated health status

**Layout:**
```
Row 1: [API Uptime %] [Request Rate] [P95 Latency] [Error Rate]
Row 2: [API Request Rate by Endpoint (Stacked Area - Full Width)]
Row 3: [API Response Time (P50/P95/P99)] [Success Rate vs Error Rate]
Row 4: [MongoDB Latency] [MongoDB Connections] [Redis Hit Rate] [Redis Memory]
Row 5: [Batch Layer Status] [Speed Layer Status] [Data Staleness]
Row 6: [Total Movies (Batch)] [Speed Layer Documents] [Speed/Batch Coverage]
Row 7: [Request Rate by Endpoint (Table)] [Slowest Endpoints (P99)]
```

---

## 📊 Optional Dashboard #5: Analytics Overview (Executive Summary)

**Purpose:** High-level summary for executives and stakeholders

**Target Users:** Executives, product owners, stakeholders

**Key Metrics:**
- Total movies tracked
- Active discussions (last 48h)
- Average sentiment (all movies)
- Total crisis alerts (last 7 days)
- Total viral detections (last 7 days)
- Total recommendations served (last 7 days)

**Panels to Migrate:**

| Source Dashboard | Panel Name | Metric/Query | Priority |
|------------------|------------|--------------|----------|
| `movie-analytics-overview.json` | Total Movies Tracked | Batch layer count | HIGH |
| `movie-analytics-overview.json` | Movies with Active Discussions | Speed layer count | HIGH |
| `movie-analytics-overview.json` | Average Sentiment (All) | Overall sentiment score | HIGH |
| `genre-analytics-dashboard.json` | Sentiment by Genre | Genre comparison | MEDIUM |
| `genre-analytics-dashboard.json` | Reddit Engagement by Genre | Engagement metrics | MEDIUM |
| `genre-analytics-dashboard.json` | Genre Distribution | Pie chart | LOW |

**New Panels to Add:**
- ⭐ Business goal KPI summary - 3 single-stats (crisis/viral/recs)
- ⭐ Weekly trend comparison - Week-over-week changes
- ⭐ Top performing genres - Ranked by combined metrics
- ⭐ Data freshness indicator - Traffic light (green/yellow/red)
- ⭐ System health summary - Aggregated uptime/performance

**Note:** ⚠️ This dashboard requires creating `/api/v1/analytics/overview` endpoint or removing panels that depend on it.

---

## 🗑️ Dashboards to Archive/Remove

### Archive (move to `archive/` folder):
1. `business-kpi.json` - Too generic, metrics redistributed
2. `data-freshness-dashboard.json` - Merged into System Health
3. `genre-analytics-dashboard.json` - Merged into Analytics Overview (if created)
4. `movie-analytics-overview.json` - ⚠️ Broken (uses non-existent endpoint), merge salvageable panels
5. `pr-crisis-detection.json` - Replaced by new Goal #1 dashboard
6. `recommendation-performance.json` - Replaced by new Goal #3 dashboard
7. `trending-movies.json` - Merged into Goal #2 dashboard
8. `viral-content.json` - Merged into Goal #2 dashboard

### Keep `system-health-dashboard.json` as base:
- Will be enhanced with data freshness panels

---

## 🚀 Implementation Plan

### Phase 1: Preparation (Day 1)
- [x] Analyze current dashboards
- [x] Create this reorganization plan
- [ ] Review plan with stakeholders
- [ ] Get approval to proceed

### Phase 2: Dashboard Creation (Day 2-3)
- [ ] Create Dashboard #1: PR Crisis Detection
  - Extract panels from `pr-crisis-detection.json`
  - Add crisis alert panels from `business-kpi.json`
  - Add new panels (sentiment volatility, response time)
  - Test with live data

- [ ] Create Dashboard #2: Viral Content Identification
  - Merge `viral-content.json` + `trending-movies.json`
  - Add new panels (viral acceleration, marketing opportunity)
  - Test with live data

- [ ] Create Dashboard #3: Recommendation Performance
  - Enhance `recommendation-performance.json`
  - Add scatter plot, A/B test panels
  - Test with live data

- [ ] Create Dashboard #4: System Health & Monitoring
  - Enhance `system-health-dashboard.json`
  - Merge in `data-freshness-dashboard.json` panels
  - Add new panels (endpoint breakdown, connection pools)
  - Test with live data

### Phase 3: Testing & Validation (Day 4)
- [ ] Generate test traffic for all endpoints
- [ ] Verify all panels load data correctly
- [ ] Check Prometheus queries are optimized
- [ ] Test dashboard filters and variables
- [ ] Verify alerts trigger correctly
- [ ] Performance test (load time < 3s)

### Phase 4: Documentation & Training (Day 5)
- [ ] Update `TESTING_GUIDE.md` with new dashboard names
- [ ] Create dashboard user guides (1 page per dashboard)
- [ ] Record demo videos showing each dashboard
- [ ] Train users on new layout

### Phase 5: Cutover & Cleanup (Day 6)
- [ ] Archive old dashboards to `archive/` folder
- [ ] Update Grafana dashboard provisioning config
- [ ] Restart Grafana to load new dashboards
- [ ] Verify old dashboards are accessible in archive
- [ ] Update all documentation links

---

## 📋 Dashboard Naming Convention

### New Dashboard Files:
```
1-crisis-detection.json          # Goal #1: PR Crisis Detection
2-viral-content.json             # Goal #2: Viral Content Identification  
3-recommendation-performance.json # Goal #3: Recommendation Performance
4-system-health.json             # System Health & Monitoring
5-analytics-overview.json        # Optional: Executive Summary
```

### New Dashboard Titles (in Grafana UI):
```
1. 🚨 PR Crisis Detection & Sentiment Monitoring
2. 🔥 Viral Content Identification & Tracking
3. 🎯 Recommendation Performance & Optimization
4. ⚙️ System Health & Infrastructure
5. 📊 Analytics Overview (Executive Summary)
```

---

## 🎯 Success Criteria

### After Reorganization:
- ✅ Only 4-5 dashboards (down from 9)
- ✅ Each dashboard clearly maps to a business goal or system concern
- ✅ No duplicate panels across dashboards
- ✅ All panels load data without errors
- ✅ Dashboard load time < 3 seconds
- ✅ All Prometheus queries optimized (< 1s execution)
- ✅ Documentation updated
- ✅ Users can find relevant metrics in < 30 seconds

---

## 🔄 Migration Mapping Table

| Old Dashboard | Panels | → New Dashboard | Notes |
|---------------|--------|-----------------|-------|
| `pr-crisis-detection.json` | All panels | #1 Crisis Detection | Primary source |
| `viral-content.json` | All panels | #2 Viral Content | Primary source |
| `trending-movies.json` | All panels | #2 Viral Content | Merge with viral |
| `recommendation-performance.json` | All panels | #3 Recommendations | Enhanced version |
| `system-health-dashboard.json` | All panels | #4 System Health | Primary source |
| `data-freshness-dashboard.json` | All panels | #4 System Health | Merge with health |
| `business-kpi.json` | Crisis alerts | #1 Crisis Detection | Partial |
| `business-kpi.json` | API metrics | #4 System Health | Partial |
| `business-kpi.json` | Request rate | #2, #3, #4 | Split by endpoint |
| `genre-analytics-dashboard.json` | Genre metrics | #5 Overview (optional) | Executive view |
| `movie-analytics-overview.json` | ⚠️ Broken | Archive | Fix endpoint first |

---

## ⚠️ Known Issues to Address

### Issue #1: Missing API Endpoint
**Problem:** `movie-analytics-overview.json` uses `/api/v1/analytics/overview` which doesn't exist

**Options:**
1. Create the endpoint in serving layer API
2. Replace with existing endpoints (`/health`, `/trending`, etc.)
3. Archive the dashboard and salvage working panels

**Recommendation:** Option 2 - Replace with existing endpoints

### Issue #2: Prometheus Job Name Confusion
**Problem:** Some dashboards use `job="serving-api"` instead of `job="fastapi"`

**Solution:** ✅ Already fixed in `business-kpi.json`, verify other dashboards

### Issue #3: Datasource UID Mismatch
**Problem:** `movie-analytics-overview.json` uses `uid="ef4brkrnlgetcc"` instead of `uid="infinity"`

**Solution:** ✅ Already fixed with sed replacement

### Issue #4: Empty Columns Arrays
**Problem:** Infinity datasource panels with `columns: []` show "No data"

**Solution:** ✅ Already fixed in `business-kpi.json`, apply pattern to other dashboards

---

## 📝 Next Steps

1. **Review this plan** - Get stakeholder approval
2. **Choose option** - 4 dashboards or 5 (with Analytics Overview)?
3. **Prioritize panels** - Which panels are must-have vs nice-to-have?
4. **Create endpoints** - Decide whether to implement `/api/v1/analytics/overview`
5. **Begin implementation** - Start with Dashboard #4 (System Health) as it's most stable

---

## 🤔 Questions for Decision

1. **Analytics Overview Dashboard:** Create it (5 dashboards) or skip it (4 dashboards)?
2. **Missing Endpoint:** Implement `/api/v1/analytics/overview` or replace with existing endpoints?
3. **Archive Strategy:** Keep old dashboards in archive folder or delete completely?
4. **Alert Migration:** Move alert rules to new dashboards or keep centralized?
5. **Custom Panels:** Any custom visualizations needed beyond standard Grafana panels?

---

**Status:** ⏳ Awaiting approval to proceed with implementation

**Estimated Effort:** 3-4 days (1 developer)

**Risk Level:** LOW (dashboards can be rolled back easily)
