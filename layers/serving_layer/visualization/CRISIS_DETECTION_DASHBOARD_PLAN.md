# PR Crisis Detection Section Plan

**Goal:** Real-time monitoring of sentiment deviations to alert PR teams of potential crises

**Data Source:** Crisis Detection API (`/api/v1/crisis-detection/*`)

---

## Section 1: Crisis Alert Overview

**Purpose:** Real-time crisis monitoring and severity tracking

### Panels

| Panel | Type | Metric | Source Endpoint |
|-------|------|--------|-----------------|
| **Active Alerts** | Stat | Total crisis count | `/alerts?severity=critical` |
| **Critical Alerts** | Stat (red) | σ < -4.0 count | `/alerts?severity=critical` |
| **High Alerts** | Stat (orange) | -4.0 ≤ σ < -3.0 count | `/alerts?severity=high` |
| **Warning Alerts** | Stat (yellow) | -3.0 ≤ σ < -2.0 count | `/alerts?severity=warning` |
| **Alert Severity Distribution** | Pie Chart | Breakdown by severity | `/monitoring` → `severity_counts` |
| **Movies in Crisis** | Table | Movie, sentiment, σ, severity | `/alerts?limit=20` |
| **Top Declining Movies** | Bar Chart | Sentiment velocity (Δ/hr) | `/monitoring` → `top_declining_movies` |

**Refresh:** 1 minute  
**Variables:** `severity` (critical/high/warning), `genre` (optional filter)

---

## Section 2: Baseline Comparisons

**Purpose:** Understand historical context and baseline deviations

### Panels

| Panel | Type | Metric | Source Endpoint |
|-------|------|--------|-----------------|
| **Genre Baselines** | Table | Genre, avg sentiment, σ, movies | `/baselines/genre/{genre}` |
| **Sentiment Distribution** | Box Plot | Min, Q1, median, Q3, max | `/baselines/genre/{genre}` → `percentiles` |
| **Crisis Threshold Line** | Graph | Baseline - 3σ by genre | Calculated from baselines |
| **Franchise Health** | Heatmap | Franchise × sentiment deviation | `/baselines/franchise/{franchise}` |
| **Year-over-Year Trends** | Time Series | Avg sentiment by release year | `/baselines/year/{year}` |

**Refresh:** 5 minutes  
**Variables:** `genre`, `franchise`, `year`

---

## Section 3: Movie Deep Dive

**Purpose:** Detailed analysis of individual movie sentiment

### Panels

| Panel | Type | Metric | Source Endpoint |
|-------|------|--------|-----------------|
| **Current Sentiment** | Gauge | Current sentiment score | `/movies/{movie_id}/sentiment` → `current_sentiment` |
| **Baseline Comparison** | Horizontal Bar | Current vs franchise/genre/year | `/movies/{movie_id}/sentiment` → `baseline_alternatives` |
| **Deviation Sigma** | Stat | σ value (colored by severity) | `/movies/{movie_id}/sentiment` → `deviation_analysis.using_baseline.sigma` |
| **Sentiment Trend** | Time Series | Hourly sentiment (48h) | Speed layer breakdown |
| **Data Source Indicator** | Status | Batch vs Speed layer | `/movies/{movie_id}/sentiment` → `sentiment_source` |
| **Confidence Score** | Progress Bar | Data reliability (0-1) | Speed layer confidence |

**Refresh:** 30 seconds  
**Variables:** `movie_id` (dropdown from active alerts)

---

## Section 4: PR Team Action Center

**Purpose:** Actionable insights for immediate response

### Panels

| Panel | Type | Metric | Source Endpoint |
|-------|------|--------|-----------------|
| **Movies Needing Response** | Alert List | Critical + high alerts | `/alerts?severity=critical,high` |
| **Average Sentiment Trend** | Sparkline | Overall avg sentiment (24h) | `/monitoring` → `average_sentiment` |
| **Movies Tracked** | Stat | Total movies monitored | `/monitoring` → `total_movies_tracked` |
| **Response Time** | Stat | Time since last update | `/monitoring` → `last_updated` |
| **Genre Risk Matrix** | Scatter Plot | Sentiment vs deviation by genre | Combined alerts + baselines |
| **Alert Timeline** | Timeline | Crisis events over time | Historical alerts (if stored) |

**Refresh:** 30 seconds  
**Alerts:** Trigger notification when critical alerts > 0

---

## Implementation Notes

### Data Sources Configuration

#### Option 1: Infinity Plugin (Recommended for JSON APIs)

**Install Plugin:**
```bash
docker exec -it serving-grafana grafana-cli plugins install yesoreyeram-infinity-datasource
docker restart serving-grafana
```

**Data Source Settings:**
```yaml
Name: Crisis Detection API
Type: Infinity
URL: http://serving-api:8000/api/v1/crisis-detection
Parser: Backend
Authentication: No Auth
Headers:
  - Accept: application/json
```

**Example Query (Alerts Panel):**
```json
{
  "type": "json",
  "source": "url",
  "url": "/alerts?limit=20",
  "method": "GET",
  "root_selector": "alerts",
  "columns": [
    {"selector": "movie_title", "text": "Movie", "type": "string"},
    {"selector": "current_sentiment", "text": "Sentiment", "type": "number"},
    {"selector": "deviation_sigma", "text": "Sigma", "type": "number"},
    {"selector": "severity", "text": "Severity", "type": "string"}
  ]
}
```

#### Option 2: Prometheus (Recommended for Time-Series Metrics)

**Expose Prometheus Metrics in API:**

Add to `layers/serving_layer/api/main.py`:
```python
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app

# Define metrics
crisis_alerts_total = Counter(
    'crisis_alerts_total',
    'Total crisis alerts triggered',
    ['severity', 'genre']
)

current_deviation_sigma = Gauge(
    'movie_deviation_sigma',
    'Current deviation sigma per movie',
    ['movie_id', 'movie_title', 'genre']
)

sentiment_score = Gauge(
    'movie_sentiment_score',
    'Current sentiment score per movie',
    ['movie_id', 'movie_title']
)

# Mount metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
```

**Prometheus Data Source Settings:**
```yaml
Name: Serving Layer Metrics
Type: Prometheus
URL: http://serving-api:8000/metrics
Scrape Interval: 15s
```

**Example PromQL Queries:**
```promql
# Critical alerts count
count(movie_deviation_sigma < -4.0)

# Average sentiment by genre
avg(movie_sentiment_score) by (genre)

# Alert rate (last 5m)
rate(crisis_alerts_total[5m])

# Movies in crisis
movie_deviation_sigma < -3.0
```

#### Recommended Hybrid Approach

- **Infinity:** Use for structured data (tables, lists, baselines)
  - Section 1: Alert tables
  - Section 2: Baseline comparisons
  - Section 3: Movie details

- **Prometheus:** Use for time-series metrics (gauges, counters)
  - Section 1: Alert counts over time
  - Section 3: Sentiment trends
  - Section 4: Real-time monitoring

### Key Transformations

1. **Severity Color Coding:**
   - Critical: Red (`σ < -4.0`)
   - High: Orange (`-4.0 ≤ σ < -3.0`)
   - Warning: Yellow (`-3.0 ≤ σ < -2.0`)
   - Normal: Green (`σ ≥ -2.0`)

2. **Alert Priority Sorting:**
   - Sort by `deviation_sigma` ascending (most negative first)
   - Secondary sort by `current_sentiment` ascending

3. **Time Range Handling:**
   - Speed layer: Last 48 hours
   - Baselines: Historical data
   - Refresh rate: Balance freshness vs API load

### Alerting Rules

```yaml
# Critical Alert Threshold
alert: CriticalSentimentDrop
expr: deviation_sigma < -4.0
for: 5m
annotations:
  summary: "{{ movie_title }} in critical crisis (σ={{ deviation_sigma }})"
  
# High Alert Threshold
alert: HighSentimentDrop
expr: deviation_sigma < -3.0 AND deviation_sigma >= -4.0
for: 10m
annotations:
  summary: "{{ movie_title }} showing high risk (σ={{ deviation_sigma }})"
```

---

## Section Priority

1. **Section 1: Crisis Alert Overview** ← Start here (most critical)
2. **Section 4: PR Team Action Center** ← Immediate response
3. **Section 3: Movie Deep Dive** ← Investigation
4. **Section 2: Baseline Comparisons** ← Context analysis

---

## Next Steps

1. Create JSON API data source in Grafana
2. Build Section 1 (Crisis Alert Overview) first
3. Test with live API endpoints
4. Add alerting rules for critical thresholds
5. Expand to remaining dashboards
