-- ============================================================
-- SPEED LAYER CASSANDRA SCHEMA (TMDB Data Only)
-- ============================================================
-- This schema stores real-time analytics from TMDB API streams
-- All data originates from TMDB - NO synthetic or user-generated data
-- TTL = 48 hours (172800 seconds) for all speed layer tables
-- ============================================================

-- Create keyspace for speed layer
CREATE KEYSPACE IF NOT EXISTS speed_layer
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE speed_layer;

-- ============================================================
-- TABLE 1: review_sentiments
-- ============================================================
-- Purpose: Real-time sentiment stats for reviews
-- Source: movie.reviews Kafka topic (TMDB API /movie/{id}/reviews)
-- Aggregation: 5-minute tumbling windows
CREATE TABLE IF NOT EXISTS review_sentiments (
    movie_id int,
    hour timestamp,
    window_start timestamp,
    window_end timestamp,
    avg_sentiment double,
    review_count int,
    positive_count int,
    negative_count int,
    neutral_count int,
    sentiment_velocity double,
    PRIMARY KEY (movie_id, hour, window_start)
) WITH default_time_to_live = 172800
  AND CLUSTERING ORDER BY (hour DESC, window_start DESC)
  AND comment = 'Real-time review sentiment aggregations by 5-minute windows'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 6
  };

-- Index for time-range queries
CREATE INDEX IF NOT EXISTS idx_review_sentiments_window_start 
ON review_sentiments (window_start);

-- ============================================================
-- TABLE 2: movie_stats
-- ============================================================
-- Purpose: Real-time movie rating statistics
-- Source: movie.ratings + movie.metadata Kafka topics
-- Aggregation: Incremental updates with velocity calculation
CREATE TABLE IF NOT EXISTS movie_stats (
    movie_id int,
    hour timestamp,
    vote_average double,
    vote_count int,
    popularity double,
    rating_velocity double,
    last_updated timestamp,
    PRIMARY KEY (movie_id, hour)
) WITH default_time_to_live = 172800
  AND CLUSTERING ORDER BY (hour DESC)
  AND comment = 'Real-time movie rating statistics with velocity metrics'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 6
  };

-- Index for time-range queries
CREATE INDEX IF NOT EXISTS idx_movie_stats_hour 
ON movie_stats (hour);

-- ============================================================
-- TABLE 3: trending_movies
-- ============================================================
-- Purpose: Hot/trending movie detection
-- Source: movie.ratings Kafka topic
-- Ranking: Top 100 movies by trending_score
CREATE TABLE IF NOT EXISTS trending_movies (
    hour timestamp,
    rank int,
    movie_id int,
    title text,
    trending_score double,
    velocity double,
    acceleration double,
    PRIMARY KEY (hour, rank, movie_id)
) WITH default_time_to_live = 172800
  AND CLUSTERING ORDER BY (rank ASC, movie_id ASC)
  AND comment = 'Trending and hot movie detection results by hour'
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 6
  };

-- Index for movie-specific queries
CREATE INDEX IF NOT EXISTS idx_trending_movies_movie_id 
ON trending_movies (movie_id);

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