# API Routes & Dashboard Reorganization Plan

**Date:** December 18, 2025  
**Purpose:** Reorganize API routes and Grafana dashboards to align with three business goals

---

## 🎯 Business Goals

### Goal #1: PR Crisis Detection
- **Metric:** Sentiment deviation from baseline (3σ threshold)
- **Users:** PR teams, marketing managers

### Goal #2: Viral Content Identification
- **Metric:** Viral coefficient (velocity / threshold)
- **Users:** Marketing teams, social media managers

### Goal #3: Content Recommendation
- **Metric:** Dual-success score (60% Reddit + 40% TMDB)
- **Users:** Content curators, product managers

---

## 📁 New API Routes Structure

```
/api/v1/
├── /health                          # System health checks
│
├── /crisis-detection/               # 🚨 GOAL #1: PR Crisis Detection
│   ├── /movies/{id}/sentiment       # Sentiment analysis for specific movie
│   ├── /movies/by-title/{title}/sentiment  # Sentiment by title
│   ├── /alerts                      # Active crisis alerts
│   ├── /alerts/{alert_id}           # Specific alert details
│   ├── /baselines/genre/{genre}     # Genre sentiment baselines
│   ├── /baselines/franchise/{franchise}  # Franchise sentiment baselines
│   ├── /baselines/year/{year}       # Year sentiment baselines
│   └── /monitoring                  # Real-time sentiment monitoring dashboard data
│
├── /viral-detection/                # 🔥 GOAL #2: Viral Content
│   ├── /trending                    # Top viral movies
│   ├── /trending/genre/{genre}      # Viral movies by genre
│   ├── /movies/{id}/viral-score     # Viral score for specific movie
│   ├── /thresholds                  # Viral thresholds by genre/budget/season
│   ├── /velocity/{id}               # Engagement velocity for movie
│   └── /opportunities               # Marketing amplification opportunities
│
├── /recommendations/                # 🎯 GOAL #3: Content Recommendation
│   ├── /dual-success                # Dual-success recommendations (main)
│   ├── /dual-success/genre/{genre}  # Filtered by genre
│   ├── /similar/{id}                # Content-based similar movies
│   ├── /reddit-buzz                 # Top Reddit buzz movies
│   ├── /tmdb-quality                # Top TMDB quality movies
│   └── /personalized                # Personalized recommendations (future)
│
└── /utilities/                      # Supporting endpoints
    ├── /movies/{id}                 # Movie details (batch + speed merge)
    ├── /movies/by-title/{title}     # Movie lookup by title
    ├── /search                      # General movie search
    └── /genres                      # Available genres list
```

---

## 🧮 API Endpoint Formulas & Calculations

### Goal #1: Crisis Detection - Mathematical Definitions

#### 1.1 `GET /crisis-detection/movies/{id}/sentiment`

**Purpose:** Get sentiment analysis with deviation from baseline

**Formula:**
```
Current Sentiment (S_current):
  S_current = merge_sentiment(S_batch, S_speed)
  where:
    - If movie was discussed in last 48h: S_current = S_speed (real-time Reddit sentiment)
    - Else: S_current = S_batch (historical sentiment from review analysis)

Baseline Calculation:
  The API fetches ALL available baseline types and returns complete analysis:
  
  1. Franchise Baseline (if movie.franchise IS NOT NULL):
     baseline_franchise = sentiment_baselines.find_one({
       "franchise": movie.franchise,
       "genre": null,
       "year": null
     })
  
  2. Genre Baseline (if movie.genre IS NOT NULL):
     baseline_genre = sentiment_baselines.find_one({
       "genre": movie.genre,
       "franchise": null,
       "year": null
     })
  
  3. Year Baseline (if movie.release_year IS NOT NULL):
     baseline_year = sentiment_baselines.find_one({
       "year": movie.release_year,
       "genre": null,
       "franchise": null
     })

Deviation Score (σ) - Calculated for EACH baseline:
  For each baseline type:
    σ = (S_current - baseline.avg_sentiment) / baseline.sentiment_stddev
  where:
    - baseline.sentiment_stddev = stddev of sentiment in that baseline context
    - Negative σ indicates sentiment drop
  
  Primary Baseline Selection for Display:
    The API chooses one baseline to highlight in "baseline_used" field:
    - If franchise available: use franchise
    - Else if genre available: use genre
    - Else: use year
    
    NOTE: This is for UI display only. All baselines are calculated and returned.
    No baseline is prioritized for crisis detection - all are evaluated equally.

Crisis Threshold:
  is_crisis = σ < -3.0  (3 standard deviations below baseline)
  severity = {
    "critical" if σ < -4.0
    "high" if -4.0 ≤ σ < -3.0
    "warning" if -3.0 ≤ σ < -2.0
    "normal" if σ ≥ -2.0
  }
```

**Response Schema:**
```json
{
  "movie_id": 298618,
  "movie_title": "The Flash",
  "current_sentiment": -0.15,
  "sentiment_source": "speed_layer",
  "baseline_used": {
    "type": "franchise",
    "value": "DC Extended Universe",
    "avg_sentiment": 0.05,
    "sentiment_stddev": 0.034,
    "movie_count": 15
  },
  "baseline_alternatives": {
    "franchise": {
      "available": true,
      "value": "DC Extended Universe",
      "avg_sentiment": 0.05,
      "sentiment_stddev": 0.034,
      "movie_count": 15
    },
    "genre": {
      "available": true,
      "value": "Action",
      "avg_sentiment": 0.12,
      "sentiment_stddev": 0.045,
      "movie_count": 320
    },
    "year": {
      "available": true,
      "value": 2023,
      "avg_sentiment": 0.08,
      "sentiment_stddev": 0.038,
      "movie_count": 145
    }
  },
  "deviation_analysis": {
    "using_baseline": {
      "type": "franchise",
      "deviation_sigma": -5.88,
      "is_crisis": true,
      "severity": "critical"
    },
    "all_baselines": {
      "franchise": {
        "deviation_sigma": -5.88,
        "is_crisis": true,
        "severity": "critical"
      },
      "genre": {
        "deviation_sigma": -6.00,
        "is_crisis": true,
        "severity": "critical"
      },
      "year": {
        "deviation_sigma": -6.05,
        "is_crisis": true,
        "severity": "critical"
      }
    },
    "comparison_note": "Crisis detected across all baseline types (all >3σ below baseline)"
  },
  "last_updated": "2025-12-18T10:30:00Z"
}
```

#### 1.2 `GET /crisis-detection/alerts`

**Purpose:** List all movies currently in crisis state (σ < -3.0)

**Query Logic:**
```python
# Step 1: Get all movies with recent discussions (speed layer)
# Note: speed_views is a MongoDB collection, not Cassandra
cutoff_time = datetime.utcnow() - timedelta(hours=48)
speed_movies = db.speed_views.find({
    "window_start": {"$gte": cutoff_time}
})

# Step 2: Match with batch layer and calculate deviation
alerts = []
for speed_movie in speed_movies:
    # Normalize title for batch layer matching
    normalized_title = normalize_movie_title(speed_movie['movie_title'])
    
    # Find matching movie in batch layer
    batch_movie = movie_intelligence.find_one({
        "title": {"$regex": f"^{re.escape(normalized_title)}$", "$options": "i"}
    })
    
    if not batch_movie:
        continue
    
    S_current = speed_movie['metrics']['avg_sentiment']  # -1.0 to 1.0
    
    # Get baseline using priority fallback
    baseline = get_sentiment_baseline(batch_movie)  # franchise → genre → year
    
    if not baseline:
        continue
    
    S_baseline = baseline["avg_sentiment"]
    σ_baseline = baseline["sentiment_stddev"]
    baseline_type = baseline.get("baseline_type", "unknown")
    
    # Calculate deviation
    σ = (S_current - S_baseline) / σ_baseline
    
    if σ < -3.0:
        alerts.append({
            "movie_id": batch_movie["movie_id"],
            "movie_title": batch_movie["title"],
            "current_sentiment": S_current,
            "baseline_sentiment": S_baseline,
            "baseline_type": baseline_type,
            "deviation_sigma": round(σ, 2),
            "severity": get_severity(σ),
            "alert_timestamp": speed_movie['window_start'],
            "data_age_hours": (datetime.utcnow() - speed_movie['window_start']).total_seconds() / 3600
        })

# Step 3: Sort by severity (most negative σ first)
return sorted(alerts, key=lambda x: x["deviation_sigma"])
```

**Filters:**
- `severity`: "critical" | "high" | "warning"
- `genre`: Filter by genre
- `limit`: Max results (default 20)

#### 1.3 `GET /crisis-detection/baselines/genre/{genre}`

**Purpose:** Get sentiment baseline statistics for a genre

**Formula:**
```sql
SELECT 
  genre,
  AVG(avg_sentiment) as baseline_sentiment,
  STDDEV(avg_sentiment) as stddev_sentiment,
  COUNT(*) as sample_size,
  MIN(avg_sentiment) as min_sentiment,
  MAX(avg_sentiment) as max_sentiment,
  PERCENTILE(avg_sentiment, 0.25) as q1_sentiment,
  PERCENTILE(avg_sentiment, 0.50) as median_sentiment,
  PERCENTILE(avg_sentiment, 0.75) as q3_sentiment
FROM sentiment_baselines
WHERE genre = {genre}
GROUP BY genre
```

**Response Schema:**
```json
{
  "genre": "Action",
  "baseline_sentiment": 0.12,
  "stddev_sentiment": 0.045,
  "sample_size": 1543,
  "percentiles": {
    "min": -0.35,
    "q1": 0.04,
    "median": 0.13,
    "q3": 0.21,
    "max": 0.58
  },
  "crisis_threshold": -0.015,
  "data_range": {
    "start_date": "2020-01-01",
    "end_date": "2025-12-18"
  }
}
```

#### 1.4 `GET /crisis-detection/baselines/franchise/{franchise}`

**Purpose:** Get sentiment baseline statistics for a franchise

**Query:**
```javascript
db.sentiment_baselines.aggregate([
  {
    $match: {
      franchise: franchise,
      genre: null,
      year: null
    }
  },
  {
    $group: {
      _id: "$franchise",
      baseline_sentiment: { $avg: "$avg_sentiment" },
      stddev_sentiment: { $stdDev: "$avg_sentiment" },
      sample_size: { $sum: "$movie_count" },
      min_sentiment: { $min: "$avg_sentiment" },
      max_sentiment: { $max: "$avg_sentiment" }
    }
  }
])
```

**Response Schema:**
```json
{
  "franchise": "DC Extended Universe",
  "baseline_sentiment": 0.05,
  "stddev_sentiment": 0.034,
  "sample_size": 15,
  "percentiles": {
    "min": -0.08,
    "q1": 0.02,
    "median": 0.05,
    "q3": 0.08,
    "max": 0.12
  },
  "crisis_threshold": -0.052
}
```

#### 1.5 `GET /crisis-detection/baselines/year/{year}`

**Purpose:** Get sentiment baseline statistics for a release year

**Query:**
```javascript
db.sentiment_baselines.aggregate([
  {
    $match: {
      year: parseInt(year),
      genre: null,
      franchise: null
    }
  },
  {
    $group: {
      _id: "$year",
      baseline_sentiment: { $avg: "$avg_sentiment" },
      stddev_sentiment: { $stdDev: "$avg_sentiment" },
      sample_size: { $sum: "$movie_count" },
      min_sentiment: { $min: "$avg_sentiment" },
      max_sentiment: { $max: "$avg_sentiment" }
    }
  }
])
```

**Response Schema:**
```json
{
  "year": 2023,
  "baseline_sentiment": 0.08,
  "stddev_sentiment": 0.038,
  "sample_size": 145,
  "percentiles": {
    "min": -0.12,
    "q1": 0.05,
    "median": 0.08,
    "q3": 0.11,
    "max": 0.18
  },
  "crisis_threshold": -0.034
}
```

#### 1.6 `GET /crisis-detection/monitoring`

**Purpose:** Real-time dashboard data for monitoring

**Aggregation:**
```python
# Count movies in each severity category
severity_counts = {
  "critical": count(σ < -4.0),
  "high": count(-4.0 ≤ σ < -3.0),
  "warning": count(-3.0 ≤ σ < -2.0),
  "normal": count(σ ≥ -2.0)
}

# Average response time to crisis
avg_response_time = AVG(
  alert_acknowledged_time - alert_created_time
  WHERE severity IN ["critical", "high"]
)

# Sentiment velocity (rate of change)
for movie in active_movies:
  velocity = (S_current - S_1h_ago) / 1h  # sentiment change per hour
```

---

### Goal #2: Viral Detection - Mathematical Definitions

#### 2.1 `GET /viral-detection/trending`

**Purpose:** Get top viral movies ranked by viral coefficient

**Viral Coefficient Formula:**
```
Viral Coefficient (V):
  V = velocity / threshold
  where:
    velocity = engagement_rate over time window
    threshold = viral_thresholds.threshold_value for context

Engagement Velocity (velocity):
  velocity = (upvotes_48h / 48) + (comments_48h / 48) * comment_weight
  where:
    - upvotes_48h = total upvotes in last 48 hours
    - comments_48h = total comments in last 48 hours
    - comment_weight = 2.0 (comments valued 2x upvotes)

Context-Aware Threshold:
  threshold = viral_thresholds.find_one({
    "genre": movie.genre,
    "budget_tier": movie.budget_tier,  # Use existing budget_tier field
    "season": get_season(current_date)
  }).threshold_value

Budget Tiers (from schema):
  - "indie": Low-budget independent films
  - "mid": Mid-range budget films
  - "blockbuster": High-budget major productions
  - "unknown": Budget information not available

Seasons (Northern Hemisphere):
  - "winter": Dec, Jan, Feb
  - "spring": Mar, Apr, May
  - "summer": Jun, Jul, Aug (blockbuster season)
  - "fall": Sep, Oct, Nov

Viral Status:
  status = {
    "viral" if V ≥ 1.5
    "trending" if 1.0 ≤ V < 1.5
    "growing" if 0.5 ≤ V < 1.0
    "stable" if V < 0.5
  }
```

**Ranking Algorithm:**
```python
# Query MongoDB speed_views collection (synced from Cassandra)
cutoff_time = datetime.utcnow() - timedelta(hours=48)
speed_data = db.speed_views.find(
    {
        "window_start": {"$gte": cutoff_time}
    }
).sort("metrics.viral_score", -1)

movies = []
for row in speed_data:
    # Velocity metrics are in the 'metrics' subdocument
    velocity = row['metrics']['upvote_velocity'] + (row['metrics']['comment_velocity'] * 2.0)
    
    # Match with batch layer to get genre/budget
    normalized_title = normalize_movie_title(row.movie_title)
    batch_movie = movie_intelligence.find_one({
        "title": {"$regex": f"^{re.escape(normalized_title)}$", "$options": "i"}
    })
    
    if not batch_movie:
        continue
    
    # Get threshold for genre only (single dimension)
    # Note: genre in movie_intelligence is already a single string, not array
    genre = batch_movie.get("genre", "")
    threshold_doc = db.viral_thresholds.find_one({
        "genre": genre,
        "budget_tier": None,
        "season": None
    })
    
    if not threshold_doc:
        # Fallback to global threshold (all null dimensions)
        threshold_doc = db.viral_thresholds.find_one({
            "genre": None,
            "budget_tier": None,
            "season": None
        })
    
    if not threshold_doc:
        continue
    
    threshold = threshold_doc["viral_threshold"]
    
    # Calculate viral coefficient using metrics from speed_views
    viral_score = row['metrics']['viral_score']
    V = viral_score / threshold
    
    movies.append({
        "movie_id": batch_movie.get("movie_id"),
        "movie_title": batch_movie.get("title"),
        "genre": genre,
        "viral_coefficient": V,
        "upvote_velocity": row['metrics']['upvote_velocity'],
        "comment_velocity": row['metrics']['comment_velocity'],
        "viral_score": viral_score,
        "viral_status": "viral" if V >= 1.0 else "trending"
    })

# Sort by viral coefficient descending
return sorted(movies, key=lambda x: x["viral_coefficient"], reverse=True)
```

**Query Parameters:**
- `genre`: Filter by genre
- `limit`: Max results (default 20)
- `viral_threshold`: Min viral coefficient (default 1.0)
- `window`: Time window in hours (default 48)

#### 2.2 `GET /viral-detection/movies/{id}/viral-score`

**Purpose:** Get detailed viral metrics for specific movie

**Implementation Note - Trending Trajectory:**
The `trending_trajectory` field requires historical rank tracking. Since rank data is not stored in MongoDB, it must be calculated on-demand:
1. Calculate current viral coefficients for all movies → rank them → find current rank
2. Query 24h old data from `speed_views` → calculate 24h ago viral coefficients → rank them → find 24h ago rank
3. Compare ranks to get `rank_change`
4. Compare current velocity to 24h ago velocity to determine `velocity_trend` (accelerating/decelerating/stable)

**Alternative Implementation:** Cache daily rankings in Redis with TTL=7 days for faster lookups.

**Response Schema:**
```json
{
  "movie_id": 298618,
  "movie_title": "The Flash",
  "viral_coefficient": 2.3,
  "viral_status": "viral",
  "engagement_metrics": {
    "upvote_velocity": 125.4,
    "comment_velocity": 45.2,
    "total_velocity": 215.8,
    "time_window": "48h"
  },
  "threshold_context": {
    "genre": "Action",
    "budget_tier": "blockbuster",
    "season": "summer",
    "threshold_value": 93.8
  },
  "reddit_metrics": {
    "total_upvotes": 2450,
    "total_comments": 876,
    "total_awards": 12,
    "avg_sentiment": 0.65,
    "data_points": 15
  },
  "trending_trajectory": {
    "current_rank": 2,
    "rank_24h_ago": 5,
    "rank_change": +3,
    "velocity_trend": "accelerating"
  }
}
```

#### 2.3 `GET /viral-detection/thresholds`

**Purpose:** Get viral thresholds by context (genre, budget, season)

**Query Logic:**
```python
# viral_thresholds schema: EXACTLY ONE dimension per document
# Valid: {"genre": "Action", "budget_tier": null, "season": null}
# Invalid: {"genre": "Action", "budget_tier": "high", "season": "summer"}

# Priority: genre > budget_tier > season (query ONE dimension only)
if genre:
    threshold = viral_thresholds.find_one({
        "genre": genre,
        "budget_tier": None,
        "season": None
    })
    if threshold:
        return {
            "dimension": "genre",
            "value": genre,
            "viral_threshold": threshold["viral_threshold"],
            "avg_popularity": threshold["avg_popularity"],
            "movie_count": threshold["movie_count"]
        }

elif budget_tier:
    threshold = viral_thresholds.find_one({
        "genre": None,
        "budget_tier": budget_tier,
        "season": None
    })
    if threshold:
        return {
            "dimension": "budget_tier",
            "value": budget_tier,
            "viral_threshold": threshold["viral_threshold"],
            "budget_tier_coefficient": threshold.get("budget_tier_coefficient", 2.5),
            "movie_count": threshold["movie_count"]
        }

elif season:
    threshold = viral_thresholds.find_one({
        "genre": None,
        "budget_tier": None,
        "season": season
    })
    if threshold:
        return {
            "dimension": "season",
            "value": season,
            "viral_threshold": threshold["viral_threshold"],
            "seasonal_threshold": threshold.get("seasonal_threshold"),
            "movie_count": threshold["movie_count"]
        }

# If no filter, return all genre thresholds
else:
    genre_thresholds = viral_thresholds.find({
        "genre": {"$ne": None},
        "budget_tier": None,
        "season": None
    })
    return [
        {
            "genre": t["genre"],
            "viral_threshold": t["viral_threshold"],
            "avg_popularity": t["avg_popularity"]
        }
        for t in genre_thresholds
    ]
```

#### 2.4 `GET /viral-detection/opportunities`

**Purpose:** Identify marketing amplification opportunities

**Opportunity Score Formula:**
```
Opportunity Score (O):
  O = V * recency_factor * momentum_factor * reach_factor
  where:
    V = viral_coefficient (from 2.1)
    
    recency_factor = exp(-age_hours / 24)
      - Decays exponentially: fresher content = higher opportunity
      - Half-life of 24 hours
    
    momentum_factor = velocity_now / velocity_24h_ago
      - Accelerating trends get boost
      - >1.5 = accelerating, <0.7 = decelerating
    
    reach_factor = log10(total_impressions) / log10(1000)
      - Normalized reach based on total impressions
      - Accounts for current audience size

Recommendation Logic:
  recommended_action = {
    "amplify_immediately" if O ≥ 5.0 and momentum_factor ≥ 1.5
    "monitor_closely" if 3.0 ≤ O < 5.0
    "organic_growth" if 1.5 ≤ O < 3.0
    "no_action" if O < 1.5
  }

Estimated Reach:
  estimated_reach = current_velocity * amplification_multiplier * time_horizon
  where:
    amplification_multiplier = 3.0 (assumed 3x with marketing push)
    time_horizon = 7 days
```

**Query:**
```python
opportunities = []
for movie in get_trending_movies(min_viral_coefficient=1.5):
    # Calculate factors
    # Get earliest discussion time from speed_views
    earliest_window = db.speed_views.find_one(
        {"movie_title": movie.title},
        sort=[("window_start", 1)]
    )
    age_hours = (now - earliest_window['window_start']).total_seconds() / 3600 if earliest_window else 48
    recency_factor = exp(-age_hours / 24)
    
    # Get current velocity (most recent window)
    current_window = db.speed_views.find_one(
        {"movie_title": movie.title},
        sort=[("window_start", -1)]
    )
    velocity_now = current_window['metrics']['viral_score'] if current_window else 0
    
    # Get velocity from 24h ago
    time_24h_ago = now - timedelta(hours=24)
    past_window = db.speed_views.find_one(
        {
            "movie_title": movie.title,
            "window_start": {"$gte": time_24h_ago - timedelta(hours=1), "$lt": time_24h_ago + timedelta(hours=1)}
        }
    )
    velocity_24h_ago = past_window['metrics']['viral_score'] if past_window else velocity_now
    momentum_factor = velocity_now / velocity_24h_ago if velocity_24h_ago > 0 else 1.0
    
    # Calculate impressions proxy from available metrics
    total_impressions = (
        movie.speed_metrics['total_upvotes'] + 
        movie.speed_metrics['total_comments'] * 5 + 
        movie.speed_metrics['total_awards'] * 10
    )
    reach_factor = log10(total_impressions) / log10(1000)
    
    # Calculate opportunity score
    O = movie.viral_coefficient * recency_factor * momentum_factor * reach_factor
    
    if O >= 1.5:  # Minimum threshold
        opportunities.append({
            "movie_id": movie.movie_id,
            "movie_title": movie.title,
            "viral_coefficient": movie.viral_coefficient,
            "opportunity_score": O,
            "recommended_action": get_recommendation(O, momentum_factor),
            "estimated_reach": velocity_now * 3.0 * 7 * 24,  # 7 days in hours
            "factors": {
                "recency": recency_factor,
                "momentum": momentum_factor,
                "reach": reach_factor
            }
        })

return sorted(opportunities, key=lambda x: x["opportunity_score"], reverse=True)
```

---

### Goal #3: Recommendations - Mathematical Definitions

#### 3.1 `GET /recommendations/dual-success`

**Purpose:** Dual-success recommendations (60% Reddit buzz + 40% TMDB quality)

**Dual-Success Score Formula:**
```
Dual-Success Score (D):
  D = 0.6 * Reddit_Score + 0.4 * TMDB_Score
  where both scores are normalized to 0-100 scale

Reddit Buzz Score (0-100):
  Reddit_Score = normalize(
    log10(total_engagement + 1) * recency_weight
  )
  where:
    total_engagement = upvotes + comments * 2 + awards * 10
    Note: crossposts data not available in speed_views
    
    recency_weight = {
      1.0 if discussed in last 24h
      0.8 if discussed in last 48h
      0.6 if discussed in last 7d
      0.4 if discussed in last 30d
      0.2 otherwise
    }
    
    normalize(x) = (x - min(x)) / (max(x) - min(x)) * 100

TMDB Quality Score (0-100):
  TMDB_Score = normalize(
    (vote_average / 10) * 0.7 + 
    (log10(vote_count + 1) / 6) * 0.3
  ) * 100
  where:
    vote_average = TMDB rating (0-10)
    vote_count = number of votes (popularity proxy)
    
    Weights:
    - 70% from average rating (quality)
    - 30% from vote count (credibility)

Minimum Thresholds (applied AFTER scoring):
  - min_rating: vote_average ≥ 6.0 (default)
  - min_reddit_engagement: total_engagement ≥ 10
  - min_vote_count: vote_count ≥ 100
```

**Ranking Algorithm:**
```python
recommendations = []

# Get movies from both layers
batch_movies = movie_intelligence.find({"vote_average": {"$gte": min_rating}})
# Query speed_views collection with correct field name
cutoff_time = datetime.utcnow() - timedelta(days=30)
speed_movies = db.speed_views.find({"window_start": {"$gte": cutoff_time}})

# Merge on movie_title (speed_views uses movie_title, not movie_id)
merged_movies = merge_on_movie_title(batch_movies, speed_movies)

for movie in merged_movies:
    # Calculate Reddit Score
    if movie.has_speed_data:
        # Get aggregated metrics from speed_views
        upvotes = movie.speed_metrics['total_upvotes']
        comments = movie.speed_metrics['total_comments']
        awards = movie.speed_metrics['total_awards']
        total_engagement = upvotes + comments * 2 + awards * 10
        
        age_hours = (now - movie.last_window_start).hours
        recency_weight = get_recency_weight(age_hours)
        
        reddit_raw = log10(total_engagement + 1) * recency_weight
    else:
        reddit_raw = 0
    
    # Calculate TMDB Score
    quality_component = (movie.vote_average / 10) * 0.7
    popularity_component = (log10(movie.vote_count + 1) / 6) * 0.3
    tmdb_raw = quality_component + popularity_component
    
    # Store for normalization
    movie.reddit_raw = reddit_raw
    movie.tmdb_raw = tmdb_raw

# Normalize scores to 0-100
reddit_min, reddit_max = min(reddit_raw), max(reddit_raw)
tmdb_min, tmdb_max = min(tmdb_raw), max(tmdb_raw)

for movie in merged_movies:
    reddit_score = (movie.reddit_raw - reddit_min) / (reddit_max - reddit_min) * 100
    tmdb_score = (movie.tmdb_raw - tmdb_min) / (tmdb_max - tmdb_min) * 100
    
    dual_success_score = 0.6 * reddit_score + 0.4 * tmdb_score
    
    # Apply minimum thresholds
    if (movie.vote_average >= min_rating and 
        movie.total_engagement >= 10 and
        movie.vote_count >= 100):
        
        recommendations.append({
            "rank": None,  # Assigned after sorting
            "movie_id": movie.movie_id,
            "movie_title": movie.title,
            "genre": movie.genre,  # Single genre field (string)
            "dual_success_score": round(dual_success_score, 1),
            "reddit_buzz_score": round(reddit_score, 1),
            "tmdb_quality_score": round(tmdb_score, 1),
            "vote_average": movie.vote_average,
            "vote_count": movie.vote_count,
            "reddit_mentions": movie.discussion_count,
            "speed_layer_contribution": reddit_score > 0
        })

# Sort by dual-success score
sorted_recs = sorted(recommendations, key=lambda x: x["dual_success_score"], reverse=True)

# Assign ranks
for i, rec in enumerate(sorted_recs):
    rec["rank"] = i + 1

return sorted_recs[:limit]
```

#### 3.2 `GET /recommendations/similar/{id}`

**Purpose:** Content-based similarity using cosine similarity

**Cosine Similarity Formula:**
```
Similarity Score (sim):
  sim(movie_A, movie_B) = cosine_similarity(vec_A, vec_B)
  where:
    vec = feature vector combining:
      - Genre matching (1-hot encoding)
      - Director matching (binary feature)
      - Franchise matching (binary feature)
      - Budget tier similarity
      - Release year proximity

Cosine Similarity:
  sim = (vec_A · vec_B) / (||vec_A|| * ||vec_B||)
  Result range: [-1, 1], where 1 = identical, 0 = orthogonal, -1 = opposite

Feature Vector Construction:
  vec = [genre_match, director_match, franchise_match, budget_tier_similarity, year_proximity]
  
  genre_match (binary):
    - 1 if same genre, 0 otherwise
  
  director_match (binary):
    - 1 if same director, 0 otherwise
  
  franchise_match (binary):
    - 1 if same franchise, 0 otherwise
    
  budget_tier_similarity (0-1):
    - 1 if same tier, 0.5 if adjacent tier, 0 otherwise
    
  year_proximity (0-1):
    - 1 if same year, decreasing with year difference

Sentiment-Aware Boost (optional):
  final_score = sim * sentiment_boost
  where:
    sentiment_boost = {
      1.2 if both movies have positive sentiment (>0.3)
      1.0 if neutral
      0.8 if either has negative sentiment (<-0.3)
    }
    Note: avg_sentiment range is -1.0 to 1.0, not TMDB rating scale
```

**Implementation:**
```python
def get_similar_movies(movie_id, limit=10):
    # Get target movie
    target = movie_intelligence.find_one({"movie_id": movie_id})
    
    # Build target feature vector
    target_vec = build_feature_vector(target)
    
    # Get candidate movies (same genre or nearby release year)
    candidates = movie_intelligence.find({
        "movie_id": {"$ne": movie_id},
        "$or": [
            {"genre": target.genre},  # Direct string match (genre is a string field)
            {"release_year": {"$gte": target.release_year - 3, "$lte": target.release_year + 3}}
        ]
    })
    
    similarities = []
    for candidate in candidates:
        candidate_vec = build_feature_vector(candidate)
        
        # Calculate cosine similarity
        sim = cosine_similarity(target_vec, candidate_vec)
        
        # Apply sentiment boost
        target_sentiment = get_current_sentiment(target.movie_id)
        candidate_sentiment = get_current_sentiment(candidate.movie_id)
        sentiment_boost = get_sentiment_boost(target_sentiment, candidate_sentiment)
        
        final_score = sim * sentiment_boost
        
        similarities.append({
            "movie_id": candidate.movie_id,
            "movie_title": candidate.title,
            "similarity_score": round(final_score, 3),
            "shared_genre": target.genre if target.genre == candidate.genre else None,
            "release_year_diff": abs(target.release_year - candidate.release_year),
            "vote_average": candidate.vote_average
        })
    
    # Sort by similarity score
    return sorted(similarities, key=lambda x: x["similarity_score"], reverse=True)[:limit]
```

#### 3.3 `GET /recommendations/reddit-buzz`

**Purpose:** Pure Reddit buzz ranking (Reddit component only)

**Reddit Buzz Score (Isolated):**
```
Reddit_Score = weighted_engagement * recency_decay * volume_multiplier

weighted_engagement:
  W = upvotes + comments * 2 + awards * 10
  where:
    - awards = Reddit awards/gildings (high engagement signal)
    - Note: crossposts data not available in speed_views

recency_decay:
  decay = exp(-age_hours / 24)
  where:
    - Exponential decay with 24h half-life
    - Prioritizes fresh discussions

volume_multiplier:
  multiplier = 1 + log10(post_count + 1)
  where:
    - More data points = higher confidence
    - log scale to prevent over-weighting
    - Note: post_count is number of aggregated posts/comments in window
```

**Ranking:**
```python
reddit_rankings = []

# Query MongoDB speed_views collection
cutoff_time = datetime.utcnow() - timedelta(days=7)
speed_data = db.speed_views.find(
    {
        "window_start": {"$gte": cutoff_time}
    }
).sort("window_start", -1)

for row in speed_data:
    # Calculate weighted engagement from aggregated metrics
    metrics = row['metrics']
    W = (metrics['total_upvotes'] + 
         metrics['total_comments'] * 2 + 
         metrics['total_awards'] * 10)
    
    # Recency decay
    age_hours = (datetime.utcnow() - row['window_start']).total_seconds() / 3600
    decay = exp(-age_hours / 24)
    
    # Note: Cross-subreddit spread not available in speed_views aggregated data
    # Using post_count as proxy for spread
    multiplier = 1 + log10(metrics['post_count'] + 1) / 10  # Scaled down
    
    reddit_score = W * decay * multiplier
    
    reddit_rankings.append({
        "rank": None,
        "movie_title": row['movie_title'],
        "reddit_buzz_score": round(reddit_score, 1),
        "total_engagement": W,
        "post_count": metrics['post_count'],
        "total_comments": metrics['total_comments'],
        "hours_since_last_window": round(age_hours, 1),
        "viral_score": metrics['viral_score']  # From speed_views calculation
    })

# Sort and rank
sorted_rankings = sorted(reddit_rankings, key=lambda x: x["reddit_buzz_score"], reverse=True)
for i, item in enumerate(sorted_rankings):
    item["rank"] = i + 1

return sorted_rankings[:limit]
```

#### 3.4 `GET /recommendations/tmdb-quality`

**Purpose:** Pure TMDB quality ranking (TMDB component only)

**TMDB Quality Score (Isolated):**
```
TMDB_Score = weighted_rating * popularity_factor * freshness_bonus

weighted_rating (Bayesian average):
  WR = (v / (v + m)) * R + (m / (v + m)) * C
  where:
    v = vote_count for the movie
    m = minimum votes threshold (e.g., 100)
    R = vote_average for the movie
    C = mean vote across all movies (e.g., 7.0)
  
  This prevents movies with few votes from ranking too high

popularity_factor:
  P = log10(vote_count + 1) / 6
  Normalized to [0, 1] range
  Assumption: max vote_count ≈ 1M (log10(1M) ≈ 6)

freshness_bonus (for recent releases):
  bonus = {
    1.1 if released in last 6 months
    1.05 if released in last 1 year
    1.0 otherwise
  }

Final Score:
  TMDB_Score = WR * (0.7 + 0.3 * P) * freshness_bonus
  Result range: approximately [0, 10]
```

**Ranking:**
```python
tmdb_rankings = []

# Calculate mean rating across dataset
C = movie_intelligence.aggregate([{"$group": {"_id": null, "avg": {"$avg": "$vote_average"}}}])[0]["avg"]
m = 100  # Minimum votes threshold

for movie in movie_intelligence.find({"vote_count": {"$gte": m}}):
    v = movie.vote_count
    R = movie.vote_average
    
    # Weighted rating (Bayesian average)
    WR = (v / (v + m)) * R + (m / (v + m)) * C
    
    # Popularity factor
    P = log10(v + 1) / 6
    
    # Freshness bonus
    months_since_release = (now - movie.release_date).days / 30
    if months_since_release <= 6:
        freshness_bonus = 1.1
    elif months_since_release <= 12:
        freshness_bonus = 1.05
    else:
        freshness_bonus = 1.0
    
    # Final score
    tmdb_score = WR * (0.7 + 0.3 * P) * freshness_bonus
    
    tmdb_rankings.append({
        "rank": None,
        "movie_id": movie.movie_id,
        "movie_title": movie.title,
        "genre": movie.genre,  # ✅ Correct field: 'genre' not 'genres'
        "tmdb_quality_score": round(tmdb_score, 2),
        "vote_average": R,
        "vote_count": v,
        "weighted_rating": round(WR, 2),
        "popularity_factor": round(P, 3),
        "release_date": movie.release_date
    })

# Sort by TMDB quality score
sorted_rankings = sorted(tmdb_rankings, key=lambda x: x["tmdb_quality_score"], reverse=True)
for i, item in enumerate(sorted_rankings):
    item["rank"] = i + 1

return sorted_rankings[:limit]
```

---

### Utility Endpoints - Mathematical Definitions

#### U.1 `GET /utilities/movies/{id}`

**Purpose:** Merge batch and speed layer data for complete movie view

**Merge Logic (48-hour cutoff):**
```python
def get_movie_details(movie_id):
    # Get batch layer data (always present)
    batch_data = movie_intelligence.find_one({"movie_id": movie_id})
    
    # Get speed layer data (if exists)
    speed_data = speed_layer.find_one({
        "movie_id": movie_id,
        "last_discussion_time": {"$gte": now - 48h}
    })
    
    # Merge strategy
    merged = {
        # TMDB fields (from batch layer)
        "movie_id": batch_data.movie_id,
        "title": batch_data.title,
        "genre": batch_data.genre,
        "release_date": batch_data.release_date,
        "vote_average": batch_data.vote_average,
        "vote_count": batch_data.vote_count,
        
        # Sentiment (speed layer if fresh, else batch)
        "avg_sentiment": speed_data['metrics']['avg_sentiment'] if speed_data else batch_data.avg_sentiment,
        "sentiment_source": "speed_layer" if speed_data else "batch_layer",
        
        # Reddit metrics (speed layer aggregated data only)
        "reddit_metrics": {
            "total_upvotes": speed_data['metrics']['total_upvotes'] if speed_data else 0,
            "total_comments": speed_data['metrics']['total_comments'] if speed_data else 0,
            "total_awards": speed_data['metrics']['total_awards'] if speed_data else 0,
            "viral_score": speed_data['metrics']['viral_score'] if speed_data else 0,
            "last_window": speed_data['window_start'] if speed_data else None,
            "data_type": speed_data['data_type'] if speed_data else None
        },
        
        # Additional batch layer metadata
        "popularity": batch_data.popularity,
        "budget_tier": batch_data.budget_tier,
        "director": batch_data.director,
        "runtime": batch_data.runtime
    }
    
    return merged
```

#### U.2 `GET /utilities/search`

**Purpose:** Search movies with text matching and filters

**Search Algorithm:**
```python
def search_movies(q, genre, year_from, year_to, limit):
    query_filters = []
    
    # Text search (if provided)
    if q:
        query_filters.append({
            "$or": [
                {"title": {"$regex": q, "$options": "i"}},  # Case-insensitive title search
                {"director": {"$regex": q, "$options": "i"}},  # Search by director
                {"franchise": {"$regex": q, "$options": "i"}}  # Search by franchise
            ]
        })
    
    # Genre filter
    if genre:
        query_filters.append({"genre": genre})  # Direct string match
    
    # Year range filter
    if year_from or year_to:
        year_filter = {}
        if year_from:
            year_filter["$gte"] = year_from
        if year_to:
            year_filter["$lte"] = year_to
        query_filters.append({"release_year": year_filter})
    
    # Combine filters
    final_query = {"$and": query_filters} if query_filters else {}
    
    # Execute search with ranking
    results = movie_intelligence.find(final_query).sort([
        ("vote_average", -1),  # Higher rated first
        ("vote_count", -1)     # More popular second
    ]).limit(limit)
    
    return list(results)
```

---

### Summary: Key Formulas Quick Reference

| Endpoint | Primary Formula | Range |
|----------|----------------|-------|
| Crisis Detection | `σ = (S_current - S_baseline) / σ_baseline` | -∞ to +∞ (crisis if < -3.0) |
| Viral Coefficient | `V = velocity / threshold` | 0 to ∞ (viral if > 1.0) |
| Dual-Success Score | `D = 0.6 * Reddit_Score + 0.4 * TMDB_Score` | 0 to 100 |
| Cosine Similarity | `sim = vec_A · vec_B / (‖vec_A‖ * ‖vec_B‖)` | -1 to 1 |
| Reddit Buzz | `R = W * exp(-t/24) * (1 + log₁₀(post_count))` | 0 to ∞ |
| TMDB Quality | `Q = WR * (0.7 + 0.3P) * freshness_bonus` | 0 to 10 |

---

## 🔄 API Route Migration Mapping

### Current → New Mapping

| Current Route | New Route | Goal | Notes |
|---------------|-----------|------|-------|
| `GET /movies/{id}/sentiment` | `GET /crisis-detection/movies/{id}/sentiment` | #1 | Direct move |
| `GET /movies/by-title/{title}/sentiment` | `GET /crisis-detection/movies/by-title/{title}/sentiment` | #1 | Direct move |
| *(new)* | `GET /crisis-detection/alerts` | #1 | List active crisis alerts |
| *(new)* | `GET /crisis-detection/baselines/genre/{genre}` | #1 | Get genre sentiment baseline |
| *(new)* | `GET /crisis-detection/monitoring` | #1 | Dashboard data endpoint |
| `GET /trending/movies` | `GET /viral-detection/trending` | #2 | Rename for clarity |
| *(new)* | `GET /viral-detection/movies/{id}/viral-score` | #2 | Get movie's viral coefficient |
| *(new)* | `GET /viral-detection/thresholds` | #2 | Get viral thresholds by context |
| *(new)* | `GET /viral-detection/opportunities` | #2 | Marketing opportunities |
| `GET /recommendations` | `GET /recommendations/dual-success` | #3 | Explicit dual-success naming |
| `GET /recommendations/movies/{id}/similar` | `GET /recommendations/similar/{id}` | #3 | Shorter path |
| `GET /recommendations/genres/{genre}` | `GET /recommendations/dual-success/genre/{genre}` | #3 | Clearer hierarchy |
| *(new)* | `GET /recommendations/reddit-buzz` | #3 | Reddit-only ranking |
| *(new)* | `GET /recommendations/tmdb-quality` | #3 | TMDB-only ranking |
| `GET /movies/{id}` | `GET /utilities/movies/{id}` | - | Support endpoint |
| `GET /search/movies` | `GET /utilities/search` | - | Support endpoint |

---

## 📊 Dashboard Reorganization (Goal-Aligned)

### New Dashboard Structure

```
grafana/dashboards/
├── 1-crisis-detection.json           # 🚨 GOAL #1: PR Crisis Detection
├── 2-viral-content.json              # 🔥 GOAL #2: Viral Content  
├── 3-recommendation-performance.json # 🎯 GOAL #3: Recommendations
└── 4-system-health.json              # System monitoring
```

---

## 📋 Dashboard #1: PR Crisis Detection & Sentiment Monitoring

**File:** `1-crisis-detection.json`  
**Goal:** Goal #1 - Detect sentiment drops > 3σ below baseline

### API Endpoints Used

| Endpoint | Purpose | Panel(s) |
|----------|---------|----------|
| `GET /crisis-detection/alerts` | List active crisis alerts | Crisis Alerts Summary, Active Alerts Table |
| `GET /crisis-detection/movies/{id}/sentiment` | Movie sentiment details | Current Sentiment Score, Sentiment vs Baseline |
| `GET /crisis-detection/baselines/genre/{genre}` | Genre baseline comparison | Sentiment by Genre Baseline |
| `GET /crisis-detection/monitoring` | Real-time monitoring data | Sentiment Velocity, Movies in Crisis State |
| Prometheus: `crisis_alerts_total` | Crisis alert counter | Crisis Count Gauge, Alerts by Severity |
| Prometheus: `sentiment_score` | Current sentiment from Reddit | Real-time Sentiment Gauge |
| Prometheus: `sentiment_baseline` | Historical baseline from TMDB | Baseline Comparison Chart |
| Prometheus: `movies_in_crisis` | Count of movies in crisis | Crisis State Counter |

### Dashboard Panels

#### Row 1: KPI Summary (Single Stats)
```json
[
  {
    "title": "Movies in Crisis",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "sum(movies_in_crisis)",
    "thresholds": [0, 1, 5],
    "colors": ["green", "yellow", "red"]
  },
  {
    "title": "Critical Alerts (24h)",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "increase(crisis_alerts_total{severity='critical'}[24h])",
    "thresholds": [0, 1, 3]
  },
  {
    "title": "Average Sentiment Score",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "avg(sentiment_score)",
    "decimals": 2,
    "unit": "none"
  },
  {
    "title": "Avg Response Time",
    "type": "stat",
    "datasource": "Infinity",
    "endpoint": "/crisis-detection/monitoring",
    "field": "avg_response_time_minutes",
    "unit": "m"
  }
]
```

#### Row 2: Sentiment Trends (Time Series)
```json
[
  {
    "title": "Sentiment vs Baseline Over Time",
    "type": "timeseries",
    "datasource": "Prometheus",
    "queries": [
      {
        "expr": "sentiment_score{movie_id='$movie_id'}",
        "legendFormat": "Current Sentiment"
      },
      {
        "expr": "sentiment_baseline{movie_id='$movie_id'}",
        "legendFormat": "Baseline"
      },
      {
        "expr": "sentiment_baseline{movie_id='$movie_id'} - 3 * stddev(sentiment_score{movie_id='$movie_id'})",
        "legendFormat": "Crisis Threshold (-3σ)"
      }
    ],
    "span": 12
  }
]
```

#### Row 3: Crisis Analytics
```json
[
  {
    "title": "Sentiment Velocity (Change Rate)",
    "type": "graph",
    "datasource": "Prometheus",
    "query": "rate(sentiment_score[1h])",
    "span": 6
  },
  {
    "title": "Crisis Severity Breakdown",
    "type": "piechart",
    "datasource": "Prometheus",
    "query": "sum by (severity) (crisis_alerts_total)",
    "span": 6
  }
]
```

#### Row 4: Active Crisis Monitoring
```json
[
  {
    "title": "Movies in Crisis State",
    "type": "table",
    "datasource": "Infinity",
    "endpoint": "/crisis-detection/alerts",
    "columns": [
      {"field": "movie_title", "header": "Movie"},
      {"field": "current_sentiment", "header": "Current"},
      {"field": "baseline_sentiment", "header": "Baseline"},
      {"field": "deviation_sigma", "header": "σ Deviation"},
      {"field": "severity", "header": "Severity"},
      {"field": "alert_timestamp", "header": "Alert Time"}
    ],
    "span": 12
  }
]
```

#### Row 5: Baseline Comparisons
```json
[
  {
    "title": "Sentiment by Genre Baseline",
    "type": "bargauge",
    "datasource": "Infinity",
    "endpoint": "/crisis-detection/baselines/genre/$genre",
    "field": "avg_sentiment",
    "span": 6
  },
  {
    "title": "Top 5 Sentiment Drops",
    "type": "table",
    "datasource": "Infinity",
    "endpoint": "/crisis-detection/alerts?sort=deviation&limit=5",
    "span": 6
  }
]
```

### Dashboard Variables
```json
{
  "variables": [
    {
      "name": "movie_id",
      "type": "custom",
      "query": "298618,402431,748783",
      "label": "Movie",
      "multi": false
    },
    {
      "name": "genre",
      "type": "query",
      "datasource": "Infinity",
      "query": "/utilities/genres",
      "label": "Genre"
    },
    {
      "name": "severity",
      "type": "custom",
      "options": ["all", "warning", "critical"],
      "label": "Severity Filter"
    }
  ]
}
```

---

## 📋 Dashboard #2: Viral Content Identification & Tracking

**File:** `2-viral-content.json`  
**Goal:** Goal #2 - Identify viral content for marketing amplification

### API Endpoints Used

| Endpoint | Purpose | Panel(s) |
|----------|---------|----------|
| `GET /viral-detection/trending` | Top viral movies | Viral Movies Ranking Table |
| `GET /viral-detection/movies/{id}/viral-score` | Movie viral coefficient | Viral Coefficient Gauge |
| `GET /viral-detection/thresholds` | Viral thresholds by context | Threshold Comparison Charts |
| `GET /viral-detection/opportunities` | Marketing opportunities | Opportunities Table |
| Prometheus: `viral_detections_total` | Viral detection counter | Viral Movies Count |
| Prometheus: `upvote_velocity` | Reddit upvote rate | Engagement Velocity Chart |
| Prometheus: `comment_velocity` | Reddit comment rate | Engagement Velocity Chart |
| Prometheus: `viral_coefficient` | Viral score metric | Viral Coefficient Over Time |

### Dashboard Panels

#### Row 1: KPI Summary
```json
[
  {
    "title": "Viral Movies (>1.0)",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "count(viral_coefficient > 1.0)",
    "thresholds": [0, 5, 10],
    "colors": ["red", "yellow", "green"]
  },
  {
    "title": "Avg Viral Coefficient",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "avg(viral_coefficient)",
    "decimals": 2
  },
  {
    "title": "Total Reddit Engagement",
    "type": "stat",
    "datasource": "Infinity",
    "endpoint": "/viral-detection/trending?limit=1",
    "field": "total_upvotes",
    "unit": "upvotes"
  },
  {
    "title": "Marketing Opportunities",
    "type": "stat",
    "datasource": "Infinity",
    "endpoint": "/viral-detection/opportunities",
    "field": "count"
  }
]
```

#### Row 2: Viral Coefficient Tracking
```json
[
  {
    "title": "Viral Coefficient Over Time",
    "type": "timeseries",
    "datasource": "Prometheus",
    "queries": [
      {
        "expr": "viral_coefficient{movie_id='$movie_id'}",
        "legendFormat": "{{movie_title}}"
      },
      {
        "expr": "1.0",
        "legendFormat": "Viral Threshold"
      },
      {
        "expr": "1.5",
        "legendFormat": "High Viral Threshold"
      }
    ],
    "span": 12
  }
]
```

#### Row 3: Engagement Metrics
```json
[
  {
    "title": "Upvote Velocity",
    "type": "graph",
    "datasource": "Prometheus",
    "query": "rate(upvote_velocity{genre='$genre'}[1h])",
    "span": 6
  },
  {
    "title": "Comment Velocity",
    "type": "graph",
    "datasource": "Prometheus",
    "query": "rate(comment_velocity{genre='$genre'}[1h])",
    "span": 6
  }
]
```

#### Row 4: Viral Spread Heatmap
```json
[
  {
    "title": "Cross-Subreddit Spread Heatmap",
    "type": "heatmap",
    "datasource": "Infinity",
    "endpoint": "/viral-detection/trending?limit=20",
    "xField": "movie_title",
    "yField": "subreddit_name",
    "valueField": "engagement_score",
    "span": 12
  }
]
```

#### Row 5: Viral Movies Ranking
```json
[
  {
    "title": "Top 10 Viral Movies",
    "type": "table",
    "datasource": "Infinity",
    "endpoint": "/viral-detection/trending?limit=10",
    "columns": [
      {"field": "rank", "header": "#"},
      {"field": "movie_title", "header": "Movie"},
      {"field": "genre", "header": "Genre"},
      {"field": "viral_coefficient", "header": "Viral Coeff"},
      {"field": "upvote_velocity", "header": "Upvotes/h"},
      {"field": "comment_velocity", "header": "Comments/h"},
      {"field": "viral_score", "header": "Viral Score"},
      {"field": "viral_status", "header": "Status"}
    ],
    "span": 8
  },
  {
    "title": "Viral Status Breakdown",
    "type": "piechart",
    "datasource": "Infinity",
    "endpoint": "/viral-detection/trending?limit=50",
    "field": "viral_status",
    "span": 4
  }
]
```

#### Row 6: Marketing Opportunities
```json
[
  {
    "title": "Marketing Amplification Opportunities",
    "type": "table",
    "datasource": "Infinity",
    "endpoint": "/viral-detection/opportunities",
    "columns": [
      {"field": "movie_title", "header": "Movie"},
      {"field": "viral_coefficient", "header": "Viral Coeff"},
      {"field": "opportunity_score", "header": "Opportunity"},
      {"field": "recommended_action", "header": "Action"},
      {"field": "estimated_reach", "header": "Est. Reach"}
    ],
    "span": 12
  }
]
```

### Dashboard Variables
```json
{
  "variables": [
    {
      "name": "genre",
      "type": "query",
      "datasource": "Infinity",
      "query": "/utilities/genres",
      "includeAll": true,
      "label": "Genre"
    },
    {
      "name": "viral_threshold",
      "type": "custom",
      "options": ["0.5", "1.0", "1.5", "2.0"],
      "current": "1.0",
      "label": "Min Viral Coefficient"
    },
    {
      "name": "time_window",
      "type": "custom",
      "options": ["6h", "12h", "24h", "48h"],
      "current": "48h",
      "label": "Time Window"
    }
  ]
}
```

---

## 📋 Dashboard #3: Recommendation Performance & Optimization

**File:** `3-recommendation-performance.json`  
**Goal:** Goal #3 - Optimize dual-success recommendations (60% Reddit + 40% TMDB)

### API Endpoints Used

| Endpoint | Purpose | Panel(s) |
|----------|---------|----------|
| `GET /recommendations/dual-success` | Dual-success recommendations | Top Recommendations Table |
| `GET /recommendations/dual-success/genre/{genre}` | Genre-filtered recommendations | Genre Performance |
| `GET /recommendations/reddit-buzz` | Reddit-only rankings | Reddit Component Analysis |
| `GET /recommendations/tmdb-quality` | TMDB-only rankings | TMDB Component Analysis |
| `GET /recommendations/similar/{id}` | Content similarity | Similar Movies Panel |
| Prometheus: `recommendation_requests_total` | Request counter | Recommendations Served |
| Prometheus: `dual_success_score` | Dual-success score histogram | Score Distribution |
| Prometheus: `http_request_duration_seconds{endpoint="/recommendations"}` | Response time | Performance Metrics |

### Dashboard Panels

#### Row 1: KPI Summary
```json
[
  {
    "title": "Total Recommendations (24h)",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "increase(recommendation_requests_total[24h])",
    "unit": "short"
  },
  {
    "title": "Avg Dual-Success Score",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "histogram_quantile(0.50, dual_success_score_bucket)",
    "decimals": 1,
    "thresholds": [0, 60, 80]
  },
  {
    "title": "Cache Hit Rate",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "rate(cache_hits_total{endpoint='/recommendations'}[5m]) / rate(cache_requests_total{endpoint='/recommendations'}[5m])",
    "unit": "percentunit",
    "decimals": 1
  },
  {
    "title": "P95 Response Time",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket{endpoint='/recommendations'}[5m]))",
    "unit": "s",
    "thresholds": [0, 0.2, 0.5]
  }
]
```

#### Row 2: Dual-Success Score Distribution
```json
[
  {
    "title": "Dual-Success Score Distribution",
    "type": "histogram",
    "datasource": "Prometheus",
    "query": "dual_success_score",
    "bucketSize": 10,
    "xAxisLabel": "Dual-Success Score (0-100)",
    "yAxisLabel": "Count",
    "span": 12
  }
]
```

#### Row 3: Component Analysis
```json
[
  {
    "title": "Reddit vs TMDB Component Scatter Plot",
    "type": "scatter",
    "datasource": "Infinity",
    "endpoint": "/recommendations/dual-success?limit=100",
    "xField": "tmdb_quality_score",
    "yField": "reddit_buzz_score",
    "sizeField": "dual_success_score",
    "span": 8
  },
  {
    "title": "Algorithm Weight Breakdown",
    "type": "piechart",
    "datasource": "Custom",
    "data": [
      {"name": "Reddit Buzz (60%)", "value": 60},
      {"name": "TMDB Quality (40%)", "value": 40}
    ],
    "span": 4
  }
]
```

#### Row 4: Recommendations by Genre
```json
[
  {
    "title": "Recommendations Served by Genre (24h)",
    "type": "barchart",
    "datasource": "Prometheus",
    "query": "sum by (genre) (increase(recommendation_requests_total[24h]))",
    "span": 12
  }
]
```

#### Row 5: Top Recommendations
```json
[
  {
    "title": "Top 20 Dual-Success Recommendations",
    "type": "table",
    "datasource": "Infinity",
    "endpoint": "/recommendations/dual-success?limit=20",
    "columns": [
      {"field": "rank", "header": "#"},
      {"field": "movie_title", "header": "Movie"},
      {"field": "genres", "header": "Genres"},
      {"field": "dual_success_score", "header": "Dual Score"},
      {"field": "reddit_buzz_score", "header": "Reddit (60%)"},
      {"field": "tmdb_quality_score", "header": "TMDB (40%)"},
      {"field": "vote_average", "header": "Rating"},
      {"field": "reddit_mentions", "header": "Reddit Mentions"}
    ],
    "span": 12
  }
]
```

#### Row 6: Performance Monitoring
```json
[
  {
    "title": "Speed Layer Freshness",
    "type": "gauge",
    "datasource": "Infinity",
    "endpoint": "/recommendations/dual-success?limit=100",
    "query": "count(speed_layer_contribution > 0) / count(*) * 100",
    "unit": "%",
    "thresholds": [0, 20, 50],
    "span": 4
  },
  {
    "title": "Recommendation Diversity (Genre Entropy)",
    "type": "stat",
    "datasource": "Infinity",
    "endpoint": "/recommendations/dual-success?limit=100",
    "calculation": "entropy(genres)",
    "decimals": 2,
    "span": 4
  },
  {
    "title": "Dual-Success Score by Genre",
    "type": "bargauge",
    "datasource": "Infinity",
    "endpoint": "/recommendations/dual-success/genre/$genre?limit=10",
    "field": "avg_dual_success_score",
    "span": 4
  }
]
```

### Dashboard Variables
```json
{
  "variables": [
    {
      "name": "genre",
      "type": "query",
      "datasource": "Infinity",
      "query": "/utilities/genres",
      "includeAll": true,
      "label": "Genre"
    },
    {
      "name": "min_rating",
      "type": "custom",
      "options": ["0", "6.0", "7.0", "8.0"],
      "current": "6.0",
      "label": "Min TMDB Rating"
    },
    {
      "name": "limit",
      "type": "custom",
      "options": ["10", "20", "50", "100"],
      "current": "20",
      "label": "Results Limit"
    }
  ]
}
```

---

## 📋 Dashboard #4: System Health & Infrastructure

**File:** `4-system-health.json`  
**Purpose:** Monitor API, MongoDB, Redis, and data pipeline health

### API Endpoints Used

| Endpoint | Purpose | Panel(s) |
|----------|---------|----------|
| `GET /health` | System health status | Overall Health Status |
| Prometheus: `up{job="fastapi"}` | API availability | API Uptime |
| Prometheus: `http_requests_total` | Request metrics | Request Rate, Success Rate |
| Prometheus: `http_request_duration_seconds` | Latency metrics | Response Time |
| Prometheus: `mongodb_connections` | MongoDB health | DB Connections |
| Prometheus: `redis_keyspace_hits_total` | Cache metrics | Cache Hit Rate |

### Dashboard Panels

#### Row 1: System Status
```json
[
  {
    "title": "API Uptime",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "avg_over_time(up{job='fastapi'}[24h]) * 100",
    "unit": "percent",
    "decimals": 2,
    "thresholds": [0, 99, 99.9]
  },
  {
    "title": "Request Rate",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "rate(http_requests_total[5m])",
    "unit": "reqps"
  },
  {
    "title": "P95 Latency",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
    "unit": "s",
    "thresholds": [0, 0.1, 0.2]
  },
  {
    "title": "Error Rate",
    "type": "stat",
    "datasource": "Prometheus",
    "query": "rate(http_requests_total{status=~'5..'}[5m]) / rate(http_requests_total[5m]) * 100",
    "unit": "percent",
    "thresholds": [0, 1, 5]
  }
]
```

#### Row 2: Request Rate by Goal
```json
[
  {
    "title": "API Request Rate by Business Goal",
    "type": "timeseries",
    "datasource": "Prometheus",
    "queries": [
      {
        "expr": "rate(http_requests_total{endpoint=~'/crisis-detection.*'}[5m])",
        "legendFormat": "Goal #1: Crisis Detection"
      },
      {
        "expr": "rate(http_requests_total{endpoint=~'/viral-detection.*'}[5m])",
        "legendFormat": "Goal #2: Viral Detection"
      },
      {
        "expr": "rate(http_requests_total{endpoint=~'/recommendations.*'}[5m])",
        "legendFormat": "Goal #3: Recommendations"
      },
      {
        "expr": "rate(http_requests_total{endpoint=~'/utilities.*'}[5m])",
        "legendFormat": "Utilities"
      }
    ],
    "span": 12
  }
]
```

#### Row 3: Database & Cache Health
```json
[
  {
    "title": "MongoDB Latency",
    "type": "graph",
    "datasource": "Prometheus",
    "query": "mongodb_query_latency_seconds",
    "span": 6
  },
  {
    "title": "Redis Cache Hit Rate",
    "type": "graph",
    "datasource": "Prometheus",
    "query": "rate(redis_keyspace_hits_total[5m]) / (rate(redis_keyspace_hits_total[5m]) + rate(redis_keyspace_misses_total[5m]))",
    "span": 6
  }
]
```

#### Row 4: Data Freshness
```json
[
  {
    "title": "Batch Layer Last Update",
    "type": "stat",
    "datasource": "Infinity",
    "endpoint": "/health",
    "field": "batch_layer_last_update",
    "unit": "dateTimeFromNow"
  },
  {
    "title": "Speed Layer Last Update",
    "type": "stat",
    "datasource": "Infinity",
    "endpoint": "/health",
    "field": "speed_layer_last_update",
    "unit": "dateTimeFromNow"
  },
  {
    "title": "Total Movies (Batch)",
    "type": "stat",
    "datasource": "Infinity",
    "endpoint": "/health",
    "field": "batch_layer_movie_count"
  },
  {
    "title": "Active Discussions (Speed)",
    "type": "stat",
    "datasource": "Infinity",
    "endpoint": "/health",
    "field": "speed_layer_movie_count"
  }
]
```

---

### Additional Schema Notes

#### ✅ Compatible Fields (No Action Needed)

1. **Sentiment Range:** All layers use -1.0 to 1.0 (Float in Cassandra, Double in MongoDB)
2. **Timestamp Formats:** API can parse both ISO 8601 strings (batch) and Cassandra timestamps (speed)
3. **Viral Score:** Speed layer calculates using batch layer thresholds (complementary, not conflicting)

#### ⚠️ Baseline Selection Priority

When querying `sentiment_baselines`, use **priority-based fallback**:

```python
def get_sentiment_baseline(movie):
    """
    Get sentiment baseline with priority: franchise > genre > year.
    
    Args:
        movie: Movie document from movie_intelligence
        
    Returns:
        dict: Baseline document with type metadata
    """
    # Priority 1: Franchise baseline (most specific)
    if movie.get("franchise"):
        baseline = sentiment_baselines.find_one({
            "franchise": movie.get("franchise"),
            "genre": None,
            "year": None
        })
        if baseline:
            return {**baseline, "baseline_type": "franchise"}
    
    # Priority 2: Genre baseline
    primary_genre = get_primary_genre(movie)  # ✅ Handle array type
    if primary_genre:
        baseline = sentiment_baselines.find_one({
            "genre": primary_genre,
            "franchise": None,
            "year": None
        })
        if baseline:
            return {**baseline, "baseline_type": "genre"}
    
    # Priority 3: Yearly baseline (least specific)
    if movie.get("release_year"):
        baseline = sentiment_baselines.find_one({
            "year": movie.get("release_year"),
            "genre": None,
            "franchise": None
        })
        if baseline:
            return {**baseline, "baseline_type": "yearly"}
    
    # No baseline found
    return None
```

**Where to Apply:**
- ✅ `/crisis-detection/movies/{id}/sentiment` - Baseline selection
- ✅ `/crisis-detection/alerts` - Batch crisis detection

---

### Implementation Checklist

**Before API Development:**
- [ ] Create `layers/serving_layer/query_engine/utils.py` with helper functions
- [ ] Add `normalize_movie_title()` function
- [ ] Add `get_sentiment_baseline()` function with priority fallback
- [ ] Add `merge_batch_speed_data()` function with 48h cutoff
- [ ] Write unit tests for edge cases (title normalization, baseline fallback)

**During API Development:**
- [ ] Use `normalize_movie_title()` whenever querying speed layer by title
- [ ] Use `merge_batch_speed_data()` for endpoints that need real-time data
- [ ] Document data source in API responses (`"data_source": "speed_layer" | "batch_layer"`)
- [ ] Use direct genre string comparison (no array handling needed)
- [ ] Add logging for title normalization mismatches

**Testing:**
- [ ] Test title normalization with edge cases:
  - "The Flash (2023)" ✅ matches "the flash"
  - "Spider-Man: No Way Home" ✅ matches "spiderman no way home"
  - "The Matrix!!!" ✅ matches "matrix"
  - "The Batman" ✅ matches "batman, the"
- [ ] Test batch-speed merge with various data ages:
  - Fresh data (< 1h old): Use speed layer
  - Stale data (> 48h old): Use batch layer
  - No speed data: Fallback to batch layer
- [ ] Test baseline priority fallback:
  - MCU movie → franchise baseline
  - Non-franchise Action movie → genre baseline
  - Old classic → yearly baseline
  - Edge case → global baseline

---

## �🚀 Implementation Plan

### Phase 1: API Route Refactoring (Day 1-2)

#### Step 1: Create New Route Files
```bash
layers/serving_layer/api/routes/
├── __init__.py (update imports)
├── health.py (keep as-is)
├── crisis_detection.py (NEW - Goal #1)
├── viral_detection.py (NEW - Goal #2)
├── recommendations.py (refactor existing)
└── utilities.py (NEW - supporting endpoints)

# Archive old files
layers/serving_layer/api/routes/archive/
├── movies.py (old)
├── trending.py (old)
└── search.py (old)
```

#### Step 2: Implement New Endpoints

**File: `crisis_detection.py`**
```python
"""
Crisis Detection Routes - Goal #1: PR Crisis Detection & Sentiment Monitoring
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
import logging

router = APIRouter(
    prefix="/crisis-detection",
    tags=["crisis-detection"]
)

@router.get("/movies/{movie_id}/sentiment")
async def get_movie_sentiment(movie_id: int):
    """Get sentiment analysis for specific movie (migrated from /movies/{id}/sentiment)"""
    pass

@router.get("/movies/by-title/{title}/sentiment")
async def get_movie_sentiment_by_title(title: str):
    """Get sentiment by movie title"""
    pass

@router.get("/alerts")
async def get_crisis_alerts(
    severity: Optional[str] = Query(None, description="Filter by severity"),
    genre: Optional[str] = Query(None, description="Filter by genre"),
    limit: int = Query(20, ge=1, le=100)
):
    """List active crisis alerts"""
    pass

@router.get("/alerts/{alert_id}")
async def get_alert_details(alert_id: str):
    """Get specific crisis alert details"""
    pass

@router.get("/baselines/genre/{genre}")
async def get_genre_baseline(genre: str):
    """Get sentiment baseline for genre"""
    pass

@router.get("/baselines/franchise/{franchise}")
async def get_franchise_baseline(franchise: str):
    """Get sentiment baseline for franchise"""
    pass

@router.get("/monitoring")
async def get_monitoring_data():
    """Get real-time monitoring dashboard data"""
    pass
```

**File: `viral_detection.py`**
```python
"""
Viral Detection Routes - Goal #2: Viral Content Identification
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
import logging

router = APIRouter(
    prefix="/viral-detection",
    tags=["viral-detection"]
)

@router.get("/trending")
async def get_trending_movies(
    genre: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    viral_threshold: float = Query(1.0, ge=0.0),
    window: int = Query(48, ge=1, le=168)
):
    """Get top viral movies (migrated from /trending/movies)"""
    pass

@router.get("/trending/genre/{genre}")
async def get_trending_by_genre(
    genre: str,
    limit: int = Query(20, ge=1, le=100)
):
    """Get viral movies for specific genre"""
    pass

@router.get("/movies/{movie_id}/viral-score")
async def get_viral_score(movie_id: int):
    """Get viral coefficient for specific movie"""
    pass

@router.get("/thresholds")
async def get_viral_thresholds(
    genre: Optional[str] = Query(None),
    budget_tier: Optional[str] = Query(None),
    season: Optional[str] = Query(None)
):
    """Get viral thresholds by context"""
    pass

@router.get("/velocity/{movie_id}")
async def get_engagement_velocity(movie_id: int):
    """Get engagement velocity metrics for movie"""
    pass

@router.get("/opportunities")
async def get_marketing_opportunities(
    min_viral_coefficient: float = Query(1.5, ge=1.0),
    limit: int = Query(10, ge=1, le=50)
):
    """Get marketing amplification opportunities"""
    pass
```

**File: `recommendations.py` (refactored)**
```python
"""
Recommendation Routes - Goal #3: Content Recommendation Optimization
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
import logging

router = APIRouter(
    prefix="/recommendations",
    tags=["recommendations"]
)

@router.get("/dual-success")
async def get_dual_success_recommendations(
    genre: Optional[str] = Query(None),
    min_rating: float = Query(6.0, ge=0, le=10),
    limit: int = Query(20, ge=1, le=100)
):
    """Get dual-success recommendations (60% Reddit + 40% TMDB)"""
    pass

@router.get("/dual-success/genre/{genre}")
async def get_dual_success_by_genre(
    genre: str,
    min_rating: float = Query(6.0, ge=0, le=10),
    limit: int = Query(20, ge=1, le=100)
):
    """Get dual-success recommendations for specific genre"""
    pass

@router.get("/similar/{movie_id}")
async def get_similar_movies(
    movie_id: int,
    limit: int = Query(10, ge=1, le=50)
):
    """Get content-based similar movies"""
    pass

@router.get("/reddit-buzz")
async def get_reddit_buzz_recommendations(
    genre: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100)
):
    """Get top Reddit buzz movies (Reddit component only)"""
    pass

@router.get("/tmdb-quality")
async def get_tmdb_quality_recommendations(
    genre: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100)
):
    """Get top TMDB quality movies (TMDB component only)"""
    pass

@router.get("/personalized")
async def get_personalized_recommendations():
    """Get personalized recommendations (future feature)"""
    raise HTTPException(status_code=501, detail="Not implemented yet")
```

**File: `utilities.py`**
```python
"""
Utility Routes - Supporting endpoints
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
import logging

router = APIRouter(
    prefix="/utilities",
    tags=["utilities"]
)

@router.get("/movies/{movie_id}")
async def get_movie_details(movie_id: int):
    """Get movie details (batch + speed merge)"""
    pass

@router.get("/movies/by-title/{title}")
async def get_movie_by_title(title: str):
    """Get movie details by title"""
    pass

@router.get("/search")
async def search_movies(
    q: Optional[str] = Query(None),
    genre: Optional[str] = Query(None),
    year_from: Optional[int] = Query(None),
    year_to: Optional[int] = Query(None),
    limit: int = Query(20, ge=1, le=100)
):
    """Search movies with filters"""
    pass

@router.get("/genres")
async def get_genres():
    """Get list of available genres"""
    pass
```

#### Step 3: Update main.py
```python
# Update imports in api/main.py
from api.routes import (
    health_router,
    crisis_detection_router,
    viral_detection_router,
    recommendations_router,
    utilities_router
)

# Include routers with new prefixes
app.include_router(health_router, prefix="/api/v1")
app.include_router(crisis_detection_router, prefix="/api/v1")
app.include_router(viral_detection_router, prefix="/api/v1")
app.include_router(recommendations_router, prefix="/api/v1")
app.include_router(utilities_router, prefix="/api/v1")
```

### Phase 2: Dashboard Creation (Day 3-4)

#### Step 1: Create Dashboard JSON Files
```bash
# Use the panel specifications from above to create:
layers/serving_layer/visualization/grafana/dashboards/
├── 1-crisis-detection.json
├── 2-viral-content.json
├── 3-recommendation-performance.json
└── 4-system-health.json

# Archive old dashboards
layers/serving_layer/visualization/grafana/dashboards_archive/
├── business-kpi.json
├── data-freshness-dashboard.json
├── genre-analytics-dashboard.json
├── movie-analytics-overview.json
├── pr-crisis-detection.json
├── recommendation-performance.json
├── system-health-dashboard.json
├── trending-movies.json
└── viral-content.json
```

#### Step 2: Configure Dashboard Provisioning
```yaml
# layers/serving_layer/visualization/grafana/provisioning/dashboards/dashboards.yml
apiVersion: 1

providers:
  - name: 'Business Goals'
    orgId: 1
    folder: 'Business Goals'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards
      foldersFromFilesStructure: false
```

### Phase 3: Testing & Validation (Day 5)

#### Checklist:
- [ ] All API endpoints return expected responses
- [ ] Old endpoints redirect or return deprecation warnings
- [ ] All dashboard panels load data successfully
- [ ] Prometheus queries execute < 1s
- [ ] Infinity datasource queries work
- [ ] Dashboard variables update panels correctly
- [ ] Links between dashboards work
- [ ] Mobile view is readable

### Phase 4: Documentation (Day 6)

#### Update Documentation:
- [ ] Update `README.md` with new API structure
- [ ] Update `TESTING_GUIDE.md` with new endpoints
- [ ] Create dashboard user guides (1 page per dashboard)
- [ ] Update API documentation (Swagger UI)
- [ ] Create migration guide for users

---

## 📊 Success Metrics

### After Reorganization:
- ✅ API routes clearly aligned with business goals
- ✅ Dashboards map 1:1 with business goals
- ✅ Reduced cognitive load (find Goal #1 features under `/crisis-detection`)
- ✅ Easier onboarding for new team members
- ✅ Clearer separation of concerns
- ✅ Better naming conventions
- ✅ 4-5 focused dashboards (down from 9)
- ✅ All panels optimized for < 3s load time
- ✅ Schema inconsistencies resolved with utility functions
- ✅ Genre array/string mismatch handled
- ✅ Title normalization implemented for batch-speed merging

---

## 📚 Related Documentation

- **`SCHEMA_VALIDATION.md`** - Complete schema validation across batch & speed layers
  - Full schemas for all 6 tables (3 MongoDB + 3 Cassandra)
  - Critical schema issues identified and documented
  - Compatibility checks and validation rules
  - Code examples for handling mismatches
  
- **`SENTIMENT_BASELINES_EXPLAINED.md`** - Deep dive on sentiment_baselines collection
  - 3 baseline types (genre, franchise, yearly)
  - Calculation logic and query patterns
  - Sample documents and usage examples
  
- **`BASELINE_SELECTION_EXAMPLES.md`** - Priority-based baseline selection examples
  - 5 real-world examples showing fallback strategy
  - MCU franchise, indie film, old classic scenarios
  - Comparison table for quick reference

---

## 🔄 Backwards Compatibility Strategy

### Option 1: Hard Cutover (Recommended)
- Remove old routes immediately
- Update all clients to use new routes
- Clear deprecation timeline (e.g., 2 weeks notice)

### Option 2: Gradual Migration
- Keep old routes with deprecation warnings
- Add `X-Deprecated` header to responses
- Log usage of old routes
- Remove after 30 days

### Option 3: Dual Support
- Keep both old and new routes
- Old routes proxy to new routes internally
- Maintain indefinitely (not recommended)

**Recommendation:** Use Option 1 since this is an internal API

---

## 🎯 Next Steps

1. **Review this plan** - Get approval from stakeholders
2. **Validate schemas** - Review SCHEMA_VALIDATION.md for critical fixes
3. **Create utility functions** - Implement `get_primary_genre()`, `normalize_movie_title()`, `merge_batch_speed_data()`
4. **Prioritize phases** - Decide which phase to start with (recommend schema fixes first)
5. **Assign resources** - Allocate developers for implementation
6. **Set timeline** - Define concrete deadlines
7. **Create tickets** - Break down tasks into JIRA/GitHub issues
8. **Begin implementation** - Start with utility functions, then Phase 1 (API refactoring)

---

**Status:** ✅ Plan ready with schema validation completed

**Estimated Effort:** 7 days (1 developer)
- Day 0.5: Utility functions (schema fixes)
- Day 1-2: API refactoring
- Day 3-4: Dashboard creation
- Day 5: Testing & validation
- Day 6: Documentation
- Day 0.5: Buffer for edge cases

**Risk Level:** LOW (schema issues identified and fixes documented)

**Dependencies:** 
- ✅ Schema validation completed (SCHEMA_VALIDATION.md)
- ⏳ MongoDB collections created (sentiment_baselines, viral_thresholds, movie_intelligence)
- ⏳ Cassandra speed_views table populated
- ⏳ Batch layer export pipeline running
