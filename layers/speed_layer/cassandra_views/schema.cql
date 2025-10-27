-- Speed Layer Cassandra Schema
-- TTL = 48 hours (172800 seconds) for all speed layer tables

-- Create keyspace for speed layer
CREATE KEYSPACE IF NOT EXISTS speed_layer
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};

USE speed_layer;

-- Table 1: Review Sentiments by Window
-- Stores aggregated sentiment analysis results for movie reviews
CREATE TABLE IF NOT EXISTS review_sentiments (
    window_start timestamp,
    window_end timestamp,
    movie_id bigint,
    review_count bigint,
    avg_sentiment_compound double,
    avg_sentiment_positive double,
    avg_sentiment_negative double,
    avg_sentiment_neutral double,
    avg_rating double,
    positive_count bigint,
    negative_count bigint,
    neutral_count bigint,
    created_at timestamp,
    PRIMARY KEY ((window_start, window_end), movie_id)
) WITH default_time_to_live = 172800
  AND comment = 'Aggregated sentiment analysis for movie reviews by time window'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 6
  };

-- Table 2: Movie Statistics by Window  
-- Stores aggregated movie metrics including popularity and ratings
CREATE TABLE IF NOT EXISTS movie_stats (
    window_start timestamp,
    window_end timestamp,
    movie_id bigint,
    title text,
    avg_popularity double,
    avg_vote_average double,
    avg_vote_count bigint,
    update_count bigint,
    min_popularity double,
    max_popularity double,
    popularity_stddev double,
    popularity_velocity double,
    popularity_acceleration double,
    created_at timestamp,
    PRIMARY KEY ((window_start, window_end), movie_id)
) WITH default_time_to_live = 172800
  AND comment = 'Aggregated movie statistics and velocity metrics by time window'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 6
  };

-- Table 3: Movie Ratings by Window
-- Stores aggregated rating statistics and distributions
CREATE TABLE IF NOT EXISTS movie_ratings_by_window (
    window_start timestamp,
    window_end timestamp,
    movie_id bigint,
    rating_count bigint,
    avg_rating double,
    min_rating double,
    max_rating double,
    rating_stddev double,
    high_ratings_count bigint,    -- ratings >= 8.0
    medium_ratings_count bigint,  -- ratings 6.0-7.9
    low_ratings_count bigint,     -- ratings < 6.0
    rating_velocity double,
    created_at timestamp,
    PRIMARY KEY ((window_start, window_end), movie_id)
) WITH default_time_to_live = 172800
  AND comment = 'Aggregated rating statistics and distributions by time window'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 6
  };

-- Table 4: Trending Movies
-- Stores trending movie detection results
CREATE TABLE IF NOT EXISTS trending_movies (
    window_start timestamp,
    window_end timestamp,
    movie_id bigint,
    title text,
    trend_rank int,
    hotness_score double,
    avg_trend_score double,
    trend_velocity double,
    trend_acceleration double,
    total_review_volume bigint,
    event_count bigint,
    velocity_score double,
    acceleration_score double,
    volume_score double,
    created_at timestamp,
    PRIMARY KEY ((window_start, window_end), trend_rank, movie_id)
) WITH default_time_to_live = 172800
  AND comment = 'Trending and hot movie detection results by time window'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 6
  };

-- Table 5: Breakout Movies
-- Stores movies with sudden popularity spikes
CREATE TABLE IF NOT EXISTS breakout_movies (
    window_start timestamp,
    window_end timestamp,
    movie_id bigint,
    title text,
    breakout_rank int,
    breakout_score double,
    surge_multiplier double,
    trend_acceleration double,
    trend_velocity double,
    avg_trend_score double,
    total_review_volume bigint,
    created_at timestamp,
    PRIMARY KEY ((window_start, window_end), breakout_rank, movie_id)
) WITH default_time_to_live = 172800
  AND comment = 'Movies with sudden popularity breakouts by time window'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 6
  };

-- Table 6: Declining Movies
-- Stores movies losing momentum
CREATE TABLE IF NOT EXISTS declining_movies (
    window_start timestamp,
    window_end timestamp,
    movie_id bigint,
    title text,
    decline_rank int,
    decline_score double,
    momentum_loss double,
    trend_velocity double,
    trend_acceleration double,
    avg_trend_score double,
    total_review_volume bigint,
    created_at timestamp,
    PRIMARY KEY ((window_start, window_end), decline_rank, movie_id)
) WITH default_time_to_live = 172800
  AND comment = 'Movies with declining popularity by time window'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 6
  };

-- Secondary indexes for common query patterns

-- Index for querying by movie_id across different windows
CREATE INDEX IF NOT EXISTS movie_id_idx_review_sentiments ON review_sentiments (movie_id);
CREATE INDEX IF NOT EXISTS movie_id_idx_movie_stats ON movie_stats (movie_id);
CREATE INDEX IF NOT EXISTS movie_id_idx_movie_ratings ON movie_ratings_by_window (movie_id);
CREATE INDEX IF NOT EXISTS movie_id_idx_trending ON trending_movies (movie_id);

-- Index for querying by time ranges
CREATE INDEX IF NOT EXISTS window_start_idx_review_sentiments ON review_sentiments (window_start);
CREATE INDEX IF NOT EXISTS window_start_idx_movie_stats ON movie_stats (window_start);
CREATE INDEX IF NOT EXISTS window_start_idx_movie_ratings ON movie_ratings_by_window (window_start);
CREATE INDEX IF NOT EXISTS window_start_idx_trending ON trending_movies (window_start);

-- Materialized views for common query patterns

-- ============================================================
-- MATERIALIZED VIEWS (Currently Disabled - Experimental Feature)
-- These provide optimized query paths but require enabling in cassandra.yaml
-- To enable: set materialized_views_enabled=true in cassandra.yaml
-- ============================================================

-- COMMENTED OUT - Uncomment when materialized views are enabled
/*
-- View 1: Latest movie statistics (most recent window)
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_movie_stats AS
    SELECT window_start, window_end, movie_id, title, avg_popularity, 
           avg_vote_average, popularity_velocity, created_at
    FROM movie_stats
    WHERE window_start IS NOT NULL 
      AND window_end IS NOT NULL 
      AND movie_id IS NOT NULL
      AND created_at IS NOT NULL
    PRIMARY KEY (created_at, window_start, window_end, movie_id)
    WITH CLUSTERING ORDER BY (created_at DESC)
    AND default_time_to_live = 172800;

-- View 2: Top trending movies (by hotness score)
CREATE MATERIALIZED VIEW IF NOT EXISTS top_trending_movies AS
    SELECT window_start, window_end, movie_id, title, trend_rank, 
           hotness_score, trend_velocity, created_at
    FROM trending_movies
    WHERE window_start IS NOT NULL 
      AND window_end IS NOT NULL 
      AND trend_rank IS NOT NULL
      AND movie_id IS NOT NULL
      AND hotness_score IS NOT NULL
    PRIMARY KEY (hotness_score, window_start, window_end, trend_rank, movie_id)
    WITH CLUSTERING ORDER BY (hotness_score DESC)
    AND default_time_to_live = 172800;

-- View 3: Recent sentiment analysis (last 6 hours)
CREATE MATERIALIZED VIEW IF NOT EXISTS recent_sentiments AS
    SELECT window_start, window_end, movie_id, review_count,
           avg_sentiment_compound, avg_rating, created_at
    FROM review_sentiments  
    WHERE window_start IS NOT NULL
      AND window_end IS NOT NULL
      AND movie_id IS NOT NULL
      AND created_at IS NOT NULL
    PRIMARY KEY (created_at, window_start, window_end, movie_id)
    WITH CLUSTERING ORDER BY (created_at DESC)
    AND default_time_to_live = 21600;  -- 6 hours
*/

-- ============================================================
-- USER-DEFINED FUNCTIONS (Currently Disabled - Experimental Feature)
-- To enable: set user_defined_functions_enabled=true in cassandra.yaml
-- ============================================================

-- COMMENTED OUT - Uncomment when UDFs are enabled
/*
-- Function to calculate trend score
CREATE OR REPLACE FUNCTION trend_score_calc(
    velocity double,
    acceleration double, 
    volume bigint
) RETURNS NULL ON NULL INPUT
  RETURNS double
  LANGUAGE java AS '
    return (velocity * 0.4) + (acceleration * 0.3) + ((volume / 100.0) * 0.3);
  ';

-- Function to categorize sentiment
CREATE OR REPLACE FUNCTION sentiment_category(
    compound double
) RETURNS NULL ON NULL INPUT
  RETURNS text
  LANGUAGE java AS '
    if (compound >= 0.05) return "positive";
    else if (compound <= -0.05) return "negative";
    else return "neutral";
  ';
*/

-- ============================================================
-- ROLLUP TABLES - These ARE enabled and ready to use
-- ============================================================

-- Create aggregation tables for rollup data (hourly summaries)

-- Hourly rollup of movie statistics
CREATE TABLE IF NOT EXISTS movie_stats_hourly (
    date_hour timestamp,  -- Truncated to hour
    movie_id bigint,
    title text,
    window_count bigint,
    avg_popularity double,
    avg_vote_average double,
    max_popularity_velocity double,
    total_update_count bigint,
    created_at timestamp,
    PRIMARY KEY (date_hour, movie_id)
) WITH default_time_to_live = 604800  -- 7 days
  AND comment = 'Hourly rollup of movie statistics for trending analysis';

-- Hourly rollup of sentiment data
CREATE TABLE IF NOT EXISTS sentiment_hourly (
    date_hour timestamp,  -- Truncated to hour
    movie_id bigint,
    window_count bigint,
    total_review_count bigint,
    avg_sentiment_compound double,
    total_positive_count bigint,
    total_negative_count bigint,
    total_neutral_count bigint,
    created_at timestamp,
    PRIMARY KEY (date_hour, movie_id)
) WITH default_time_to_live = 604800  -- 7 days
  AND comment = 'Hourly rollup of sentiment analysis for trend tracking';

-- Performance monitoring table
CREATE TABLE IF NOT EXISTS speed_layer_metrics (
    metric_name text,
    timestamp timestamp,
    value double,
    tags map<text, text>,
    PRIMARY KEY (metric_name, timestamp)
) WITH default_time_to_live = 259200  -- 3 days
  AND comment = 'Performance metrics for speed layer monitoring'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 1
  };

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN KEYSPACE speed_layer TO speed_layer_user;
-- GRANT SELECT ON ALL MATERIALIZED VIEWS IN KEYSPACE speed_layer TO speed_layer_user;