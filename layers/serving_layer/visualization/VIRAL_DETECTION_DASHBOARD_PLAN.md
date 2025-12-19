# 🎯 Viral Content Detection Dashboard Plan

## Goal
Identify breakout content by comparing real-time Reddit engagement velocity against historical viral thresholds to enable marketing amplification.

---

## 📊 Dashboard Layout (2. Viral Content Detection)

### **Row 1: Top-Level KPIs** (4 Stat Panels)
- **🔥 Viral Movies** (viral_coefficient ≥ 0.3)
  - API: `GET /viral-detection/trending?viral_threshold=0.3&limit=100`
  - Metric: `count` field from response
  - Color: Green → Red (thresholds: 0=green, 5=yellow, 10=orange, 20=red)

- **📈 Trending Movies** (0.15 ≤ viral_coefficient < 0.3)
  - API: `GET /viral-detection/trending?viral_threshold=0.15&limit=100`
  - Metric: Filter `movies` where `0.15 ≤ viral_coefficient < 0.3`, count them
  - Color: Green → Orange

- **🌱 Growing Movies** (0.05 ≤ viral_coefficient < 0.15)
  - API: `GET /viral-detection/trending?viral_threshold=0.05&limit=100`
  - Metric: Filter `movies` where `0.05 ≤ viral_coefficient < 0.15`, count them
  - Color: Green → Yellow

- **💡 Marketing Opportunities** (O ≥ 0.060)
  - API: `GET /viral-detection/opportunities?min_opportunity_score=0.060&limit=100`
  - Metric: `count` field from response
  - Color: Purple → Blue

---

### **Row 2: Status Distribution & Top Viral Content**

#### Left Half (12 cols): **📊 Viral Status Distribution** (Pie Chart)
- API: `GET /viral-detection/trending?limit=100&viral_threshold=0.0`
- Data Transformation:
  - Group by `movies[].viral_metrics.viral_status`
  - Count: `viral`, `trending`, `growing`, `stable`
- Colors:
  - `viral`: dark-red
  - `trending`: dark-orange  
  - `growing`: dark-yellow
  - `stable`: dark-green
- Legend: Right side, show values + percentages

#### Right Half (12 cols): **🔥 Top Viral Movies** (Bar Chart - Horizontal)
- API: `GET /viral-detection/trending?limit=10&viral_threshold=0.0`
- X-axis: `movies[].viral_metrics.viral_coefficient`
- Y-axis: `movies[].movie_title`
- Color: Continuous gradient (green → red based on value)
- Show values on bars
- Sort: Descending by viral_coefficient (pre-sorted by API)

---

### **Row 3: Marketing Opportunities**

**🎯 Top Marketing Opportunities** (Table - Full Width)
- API: `GET /viral-detection/opportunities?limit=20&min_opportunity_score=0.010`
- Columns:
  1. **Movie** (`movie_title`) - 300px
  2. **Viral Coefficient** (`viral_coefficient`) - 140px, 3 decimals
  3. **Opportunity Score** (`opportunity_score`) - 140px, 3 decimals
  4. **Recommended Action** (`recommended_action`) - 160px
  5. **Recency** (`factors.recency`) - 100px, 3 decimals
  6. **Momentum** (`factors.momentum`) - 100px, 2 decimals
  7. **Urgency** (`factors.reach`) - 100px, 3 decimals
  8. **Est. Reach** (`estimated_reach`) - 120px, 0 decimals
  9. **Age (hrs)** (`age_hours`) - 100px, 1 decimal

- Sort: By `opportunity_score` descending
- Color Coding (Cell background):
  - `amplify_immediately`: dark-red
  - `monitor_closely`: dark-orange
  - `organic_growth`: dark-yellow
  - `evaluate`: light-gray

---

### **Row 4: Genre Thresholds Context**

**📐 Genre Thresholds Reference** (Table - Full Width)
- API: `GET /viral-detection/thresholds`
- Columns:
  1. **Genre** (`thresholds[].genre`) - 200px
  2. **Threshold (avg_popularity)** (`thresholds[].threshold_used_in_calculation`) - 200px, 2 decimals
  3. **Movie Count** (`thresholds[].movie_count`) - 150px
  4. **Type** - Fixed "avg_popularity" - 150px

- Sort: By genre alphabetically
- Note below table: "avg_popularity is used as denominator in viral coefficient calculation"

---

## 🔧 Technical Specifications

### Datasource
- **Type**: `yesoreyeram-infinity-datasource`
- **UID**: `infinity`
- **Base URL**: `http://serving-api:8000/api/v1`

### Refresh Rate
- **Auto-refresh**: 1 minute (real-time Reddit engagement)
- **Manual refresh intervals**: 30s, 1m, 5m, 15m, 30m

### Time Range
- **Default**: Last 6 hours
- **Note**: speed_views has 48h TTL, so data is inherently recent

### Variables (Optional Filters)
1. **Genre Filter** (`$genre`)
   - Type: Custom/Multi-select
   - Include All: Yes
   - Usage: Filter trending/opportunities by genre

2. **Viral Threshold** (`$viral_threshold`)
   - Type: Custom/Single-select
   - Options: 0.0, 0.05, 0.15, 0.3
   - Default: 0.0
   - Usage: Minimum viral coefficient filter

---

## 📋 API Endpoint Mappings

| Panel | Endpoint | Key Fields |
|-------|----------|------------|
| Viral Movies KPI | `/viral-detection/trending?viral_threshold=0.3` | `count` |
| Trending Movies KPI | `/viral-detection/trending?viral_threshold=0.15` | `count` (filtered) |
| Growing Movies KPI | `/viral-detection/trending?viral_threshold=0.05` | `count` (filtered) |
| Opportunities KPI | `/viral-detection/opportunities?min_opportunity_score=0.060` | `count` |
| Status Distribution | `/viral-detection/trending?limit=100` | `movies[].viral_metrics.viral_status` |
| Top Viral Bar Chart | `/viral-detection/trending?limit=10` | `movie_title`, `viral_coefficient` |
| Opportunities Table | `/viral-detection/opportunities?limit=20` | All opportunity fields |
| Thresholds Table | `/viral-detection/thresholds` | `thresholds[].*` |

---

## 🎨 Color Scheme

### Viral Status Colors
- **Viral** (≥ 0.3): `dark-red` / `#C4162A`
- **Trending** (0.15-0.3): `dark-orange` / `#FF780A`
- **Growing** (0.05-0.15): `dark-yellow` / `#FADE2A`
- **Stable** (< 0.05): `dark-green` / `#73BF69`

### Action Colors
- **Amplify Immediately**: `dark-red`
- **Monitor Closely**: `dark-orange`
- **Organic Growth**: `dark-yellow`
- **Evaluate**: `light-gray`

### Gradient for Bar Charts
- Mode: `continuous-RdYlGn` (reversed: green=low, red=high)

---

## 📝 Dashboard Metadata

```json
{
  "title": "2. Viral Content Detection - Marketing Intelligence",
  "uid": "viral-content-detection",
  "tags": ["viral-detection", "marketing", "reddit-engagement"],
  "refresh": "1m",
  "timezone": "browser"
}
```

---

## 🚀 Implementation Steps

1. **Create Dashboard JSON** (`2-viral-content-detection.json`)
   - Copy structure from `1-crisis-alert-overview.json`
   - Update datasource to Infinity with viral detection endpoints

2. **Configure Panels** (8 total)
   - Row 1: 4 stat panels (KPIs)
   - Row 2: 1 pie chart + 1 bar chart
   - Row 3: 1 table (opportunities)
   - Row 4: 1 table (thresholds reference)

3. **Test API Connectivity**
   - Verify all endpoints return valid JSON
   - Test with various query parameters

4. **Add Variables** (Optional)
   - Genre filter
   - Viral threshold selector

5. **Deploy to Grafana**
   - Place in `layers/serving_layer/visualization/grafana/dashboards/`
   - Import via Grafana UI or provisioning

---

## 📊 Expected User Experience

**Use Case**: Marketing team identifies viral content for amplification

1. **At-a-glance**: Top row shows counts of viral/trending/growing movies + opportunities
2. **Status Overview**: Pie chart shows distribution of viral statuses
3. **Top Performers**: Bar chart shows highest viral coefficient movies
4. **Action Items**: Opportunities table with clear recommended actions sorted by urgency
5. **Context**: Threshold table explains what "viral" means per genre

**Key Insight**: Movies with high viral coefficient AND high opportunity score need immediate marketing push!

---

## 🔍 Data Freshness

- **speed_views**: 48h TTL (auto-expires old data)
- **movie_intelligence**: Batch layer (daily updates)
- **viral_thresholds**: Batch layer (daily updates)
- **Dashboard refresh**: 1 minute

All visualizations show near-real-time Reddit engagement within 48h window.
