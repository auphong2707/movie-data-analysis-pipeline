# MongoDB Schema Documentation

**Database Name:** `moviedb`

**Generated:** December 18, 2025

---

## Collections Overview

The `moviedb` database contains 4 collections:
1. `movie_intelligence` - Batch layer aggregated movie analytics
2. `speed_views` - Real-time streaming data from Reddit
3. `viral_thresholds` - Pre-calculated viral thresholds for different segments
4. `sentiment_baselines` - Pre-calculated sentiment baselines for comparison

---

## 1. movie_intelligence Collection

**Purpose:** Stores comprehensive movie intelligence data aggregated from batch processing layer (TMDB data + sentiment analysis).

**Document Count:** 3,260 movies

### Schema

| Field Name | Type | Range/Values | Meaning |
|------------|------|--------------|---------|
| `_id` | ObjectId | MongoDB ObjectId | Unique document identifier |
| `movie_id` | Integer | Positive integers | TMDB movie ID |
| `title` | String | Variable length | Movie title |
| `genre` | String | See genre list below | Primary genre classification |
| `release_date` | String (ISO Date) | "YYYY-MM-DD" | Movie release date |
| `release_month` | String | "January" - "December" | Month of release |
| `release_year` | Integer | 1902 - 2027 | Year of release |
| `avg_sentiment` | Float | -1.0 to 1.0 | Average sentiment score from reviews (0 = neutral) |
| `vote_average` | Float | 0 - 10 | TMDB user rating average |
| `vote_count` | Integer | ≥ 0 | Number of votes on TMDB |
| `popularity` | Float | 0.0053 - 446.1148 | TMDB popularity score |
| `budget` | Integer | ≥ 0 | Movie budget in USD (0 = unknown) |
| `budget_tier` | String | "indie", "mid", "blockbuster", "unknown" | Budget classification |
| `franchise` | String or null | Variable | Franchise/series name if applicable |
| `director` | String | Variable length | Primary director name |
| `runtime` | Integer | ≥ 0 (minutes) | Movie duration in minutes |
| `review_count` | Integer | ≥ 0 | Number of reviews analyzed |
| `type` | String | "movie_intelligence" | Document type identifier |
| `updated_at` | ISODate | Timestamp | Last document update time |
| `batch_run_timestamp` | String (ISO) | Timestamp | Batch job execution timestamp |
| `aggregation_granularity` | String | "all_time", etc. | Temporal aggregation level |
| `data_period_start` | String (ISO Date) | "YYYY-MM-DD" | Start of data period |
| `data_period_end` | String (ISO Date) | "YYYY-MM-DD" | End of data period |

#### Genre Values
Action, Adventure, Animation, Comedy, Crime, Documentary, Drama, Family, Fantasy, History, Horror, Music, Mystery, Romance, Science Fiction, TV Movie, Thriller, War, Western, "" (empty)

#### Budget Tiers
- **indie:** Low-budget independent films
- **mid:** Mid-range budget films
- **blockbuster:** High-budget major productions
- **unknown:** Budget information not available

---

## 2. speed_views Collection

**Purpose:** Stores real-time aggregated metrics from Reddit posts and comments about movies (speed layer/streaming data).

**Document Count:** 461 aggregated views

### Schema

| Field Name | Type | Range/Values | Meaning |
|------------|------|--------------|---------|
| `_id` | ObjectId | MongoDB ObjectId | Unique document identifier |
| `window_start` | ISODate | Timestamp | Start time of aggregation window |
| `data_type` | String | "reddit_post", "reddit_comment" | Type of Reddit data |
| `hour` | ISODate | Hourly timestamps | Hour bucket for aggregation |
| `movie_title` | String | Variable length | Movie being discussed |
| `data_source` | String | "reddit" | Source platform |
| `metrics` | Object | See metrics breakdown | Aggregated metrics object |
| `metrics.post_count` | Integer | 1 - 2 | Number of posts/comments in window |
| `metrics.total_upvotes` | Integer | -13 to 2812 | Sum of upvotes (can be negative) |
| `metrics.avg_upvote_ratio` | Float | 0.0 - 1.0 | Average upvote ratio |
| `metrics.total_comments` | Integer | ≥ 0 | Total comment count |
| `metrics.total_awards` | Integer | ≥ 0 | Total Reddit awards received |
| `metrics.avg_sentiment` | Float | -1.0 to 1.0 | Average sentiment score |
| `metrics.max_upvotes` | Integer | ≥ 0 | Maximum upvotes in a single post |
| `metrics.upvote_velocity` | Float | ≥ 0 | Rate of upvote change |
| `metrics.comment_velocity` | Float | ≥ 0 | Rate of comment change |
| `metrics.award_velocity` | Float | ≥ 0 | Rate of award change |
| `metrics.viral_score` | Float | ≥ 0 | Calculated virality score |
| `processed_at` | ISODate | Timestamp | When streaming job processed the data |
| `synced_at` | ISODate | Timestamp | When data was synced to MongoDB |
| `ttl_expires_at` | ISODate | Timestamp | TTL expiration time (auto-delete) |

#### Data Sources
- **reddit:** Reddit platform data

#### Data Types
- **reddit_post:** Aggregated metrics from Reddit submissions
- **reddit_comment:** Aggregated metrics from Reddit comments

---

## 3. viral_thresholds Collection

**Purpose:** Pre-calculated viral thresholds segmented by genre, budget tier, and season for quick comparison.

**Document Count:** Variable (one per segment combination)

### Schema

| Field Name | Type | Range/Values | Meaning |
|------------|------|--------------|---------|
| `_id` | ObjectId | MongoDB ObjectId | Unique document identifier |
| `genre` | String or null | Genre names or null | Genre segment (null = all genres) |
| `viral_threshold` | Float | ≥ 0 | Calculated virality threshold |
| `avg_popularity` | Float | ≥ 0 | Average popularity for segment |
| `movie_count` | Integer | ≥ 0 | Number of movies in segment |
| `budget_tier` | String or null | Budget tier or null | Budget segment (null = all tiers) |
| `budget_tier_threshold` | Float or null | ≥ 0 or null | Threshold specific to budget tier |
| `budget_tier_coefficient` | Float or null | Coefficient or null | Adjustment coefficient for tier |
| `season` | String or null | Season name or null | Seasonal segment (null = all seasons) |
| `seasonal_threshold` | Float or null | ≥ 0 or null | Threshold specific to season |
| `type` | String | "viral_threshold" | Document type identifier |
| `updated_at` | ISODate | Timestamp | Last document update time |
| `batch_run_timestamp` | String (ISO) | Timestamp | Batch job execution timestamp |
| `aggregation_granularity` | String | "all_time", etc. | Temporal aggregation level |
| `data_period_start` | String (ISO Date) | "YYYY-MM-DD" | Start of data period |
| `data_period_end` | String (ISO Date) | "YYYY-MM-DD" | End of data period |

#### Segmentation Strategy
Thresholds can be segmented by:
- **Genre:** Specific genre or null (all genres)
- **Budget Tier:** indie/mid/blockbuster or null (all tiers)
- **Season:** Seasonal period or null (all seasons)

---

## 4. sentiment_baselines Collection

**Purpose:** Pre-calculated sentiment baselines for different segments (genre, franchise, year) for comparative analysis.

**Document Count:** Variable (one per segment combination)

### Schema

| Field Name | Type | Range/Values | Meaning |
|------------|------|--------------|---------|
| `_id` | ObjectId | MongoDB ObjectId | Unique document identifier |
| `genre` | String or null | Genre names or null | Genre segment (null = all genres) |
| `avg_sentiment` | Float | -1.0 to 1.0 | Average sentiment for segment |
| `sentiment_stddev` | Float | ≥ 0 | Standard deviation of sentiment |
| `movie_count` | Integer | ≥ 0 | Number of movies in segment |
| `review_count` | Integer | ≥ 0 | Total reviews analyzed in segment |
| `franchise` | String or null | Franchise name or null | Franchise segment (null = all franchises) |
| `franchise_avg_sentiment` | Float or null | -1.0 to 1.0 or null | Average sentiment for franchise |
| `year` | Integer or null | Year or null | Year segment (null = all years) |
| `yearly_sentiment` | Float or null | -1.0 to 1.0 or null | Average sentiment for year |
| `type` | String | "sentiment_baseline" | Document type identifier |
| `updated_at` | ISODate | Timestamp | Last document update time |
| `batch_run_timestamp` | String (ISO) | Timestamp | Batch job execution timestamp |
| `aggregation_granularity` | String | "all_time", etc. | Temporal aggregation level |
| `data_period_start` | String (ISO Date) | "YYYY-MM-DD" | Start of data period |
| `data_period_end` | String (ISO Date) | "YYYY-MM-DD" | End of data period |

#### Segmentation Strategy
Baselines can be segmented by:
- **Genre:** Specific genre or null (all genres)
- **Franchise:** Specific franchise or null (all franchises)
- **Year:** Specific year or null (all years)

---

## Common Patterns

### Batch Layer Metadata
All batch layer collections (`movie_intelligence`, `viral_thresholds`, `sentiment_baselines`) share common metadata fields:
- `type`: Document type identifier
- `updated_at`: Last update timestamp
- `batch_run_timestamp`: Batch job execution time
- `aggregation_granularity`: Level of temporal aggregation
- `data_period_start`: Start of data coverage period
- `data_period_end`: End of data coverage period

### Speed Layer Metadata
Speed layer collection (`speed_views`) includes:
- `processed_at`: Streaming processing timestamp
- `synced_at`: MongoDB sync timestamp
- `ttl_expires_at`: TTL index expiration time (for automatic cleanup)

### Null Values
- `null` values typically indicate "all" or "aggregate across all values" for segmentation fields
- In dimensional fields (genre, franchise, etc.), `null` = no segmentation on that dimension

---

## Indexing Notes

Based on query patterns, the following indexes are recommended:

### movie_intelligence
- `movie_id`: Unique index for movie lookups
- `genre`: For genre-based queries
- `release_year`: For temporal queries
- `popularity`: For sorting by popularity
- Compound: `{genre: 1, release_year: 1}` for common filtered queries

### speed_views
- `movie_title`: For movie-specific lookups
- `hour`: For time-based queries
- `data_type`: For filtering by post vs comment
- TTL index on `ttl_expires_at` for automatic expiration
- Compound: `{movie_title: 1, hour: -1}` for recent activity

### viral_thresholds
- Compound: `{genre: 1, budget_tier: 1, season: 1}` for threshold lookups

### sentiment_baselines
- Compound: `{genre: 1, franchise: 1, year: 1}` for baseline lookups

---

## Data Flow

1. **Batch Layer → movie_intelligence, viral_thresholds, sentiment_baselines**
   - Source: TMDB API + Batch processing (Airflow + Spark)
   - Update Frequency: Daily batch runs
   - Data Freshness: D-1 (previous day)

2. **Speed Layer → speed_views**
   - Source: Reddit API → Kafka → Spark Streaming → Cassandra → MongoDB sync
   - Update Frequency: Real-time (5-minute windows)
   - Data Retention: 24 hours (TTL)

3. **Serving Layer Queries**
   - Merges batch + speed data for comprehensive views
   - Falls back to batch data if speed data unavailable
   - Uses pre-calculated thresholds/baselines for performance

---

## Version Information

- **MongoDB Version:** Check with `db.version()`
- **Database Size:** ~1.5 MB (as of December 18, 2025)
- **Total Documents:** ~4,000 across all collections
- **Connection:** `mongodb://admin:password@serving-mongodb:27017/moviedb`

---

## Related Documentation

- API Endpoints: `/layers/serving_layer/API_ENDPOINT_SCHEMA_ISSUES.md`
- Query Engine: `/layers/serving_layer/query_engine/`
- MongoDB Client: `/layers/serving_layer/mongodb/client.py`
- MongoDB Queries: `/layers/serving_layer/mongodb/queries.py`
