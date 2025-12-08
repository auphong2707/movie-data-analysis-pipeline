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
-- GRANT SELECT ON ALL MATERIALIZED VIEWS IN KEYSPACE speed_layer TO speed_layer_user;-- =====================================================================
-- Cassandra Speed Layer Schema
-- =====================================================================
-- Purpose: Store real-time Reddit viral metrics with 48-hour TTL
-- Keyspace: speed_layer
-- TTL: 172800 seconds (48 hours)
-- =====================================================================

-- Create keyspace if not exists
CREATE KEYSPACE IF NOT EXISTS speed_layer
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE speed_layer;

-- =====================================================================
-- Reddit Post Metrics (5-minute windows)
-- =====================================================================
-- Partitioned by movie_title for efficient movie-based queries
-- Clustered by window_start for time-range queries
-- TTL: 48 hours (auto-deletion of old data)
-- =====================================================================

CREATE TABLE IF NOT EXISTS reddit_post_metrics (
    movie_title TEXT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    post_count BIGINT,
    total_upvotes BIGINT,
    avg_upvote_ratio DOUBLE,
    total_comments BIGINT,
    total_awards BIGINT,
    avg_sentiment DOUBLE,
    max_upvotes BIGINT,
    upvote_velocity DOUBLE,
    comment_velocity DOUBLE,
    award_velocity DOUBLE,
    viral_score DOUBLE,
    data_source TEXT,
    processed_at TIMESTAMP,
    PRIMARY KEY (movie_title, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 172800
  AND comment = 'Reddit post viral metrics aggregated in 5-minute windows';

-- Index for time-range queries
CREATE INDEX IF NOT EXISTS idx_post_window_start 
ON reddit_post_metrics (window_start);

-- Index for viral score queries
CREATE INDEX IF NOT EXISTS idx_post_viral_score 
ON reddit_post_metrics (viral_score);

-- =====================================================================
-- Reddit Comment Metrics (5-minute windows)
-- =====================================================================
-- Stores aggregated comment sentiment and engagement
-- TTL: 48 hours
-- =====================================================================

CREATE TABLE IF NOT EXISTS reddit_comment_metrics (
    movie_title TEXT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    comment_count BIGINT,
    total_comment_upvotes BIGINT,
    total_comment_awards BIGINT,
    avg_comment_sentiment DOUBLE,
    max_comment_upvotes BIGINT,
    processed_at TIMESTAMP,
    PRIMARY KEY (movie_title, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 172800
  AND comment = 'Reddit comment metrics aggregated in 5-minute windows';

-- Index for time-range queries
CREATE INDEX IF NOT EXISTS idx_comment_window_start 
ON reddit_comment_metrics (window_start);

-- =====================================================================
-- Merged Speed View (Combined Posts + Comments)
-- =====================================================================
-- Denormalized view combining post and comment metrics
-- Used for fast queries by serving layer
-- TTL: 48 hours
-- =====================================================================

CREATE TABLE IF NOT EXISTS speed_views (
    movie_title TEXT,
    hour TIMESTAMP,  -- Hourly granularity for partition key
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    
    -- Post metrics
    post_count BIGINT,
    total_upvotes BIGINT,
    avg_upvote_ratio DOUBLE,
    upvote_velocity DOUBLE,
    
    -- Comment metrics
    comment_count BIGINT,
    total_comments BIGINT,
    comment_velocity DOUBLE,
    
    -- Awards
    total_awards BIGINT,
    award_velocity DOUBLE,
    
    -- Sentiment
    avg_sentiment DOUBLE,
    avg_comment_sentiment DOUBLE,
    combined_sentiment DOUBLE,
    
    -- Viral score
    viral_score DOUBLE,
    
    -- Metadata
    data_source TEXT,
    last_updated TIMESTAMP,
    
    PRIMARY KEY ((movie_title, hour), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 172800
  AND comment = 'Merged speed view combining post and comment metrics (48h TTL)';

-- Index for viral movies query
CREATE INDEX IF NOT EXISTS idx_speed_viral_score 
ON speed_views (viral_score);

-- Index for sentiment queries
CREATE INDEX IF NOT EXISTS idx_speed_sentiment 
ON speed_views (combined_sentiment);

-- =====================================================================
-- Queries for Common Use Cases
-- =====================================================================

-- Query 1: Get latest viral metrics for a movie
-- SELECT * FROM speed_views 
-- WHERE movie_title = 'Dune' 
-- AND hour >= '2025-01-01 00:00:00'
-- LIMIT 10;

-- Query 2: Get top viral movies in last hour
-- SELECT movie_title, viral_score, upvote_velocity 
-- FROM speed_views 
-- WHERE hour >= dateOf(now()) - 3600000
-- ORDER BY viral_score DESC
-- LIMIT 20;

-- Query 3: Get sentiment trends for a movie
-- SELECT window_start, combined_sentiment, avg_upvote_ratio
-- FROM speed_views
-- WHERE movie_title = 'Oppenheimer'
-- AND hour >= '2025-01-01 00:00:00'
-- ORDER BY window_start DESC;

-- =====================================================================
-- TTL Verification
-- =====================================================================
-- To verify TTL is working:
-- SELECT movie_title, window_start, TTL(viral_score) FROM speed_views LIMIT 10;
-- (Should show remaining seconds until auto-deletion)
-- =====================================================================
