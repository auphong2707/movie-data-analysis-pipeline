-- =====================================================================
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
    hour TIMESTAMP,
    window_start TIMESTAMP,
    post_count INT,
    total_upvotes BIGINT,
    avg_upvote_ratio FLOAT,
    total_comments INT,
    total_awards INT,
    avg_sentiment FLOAT,
    max_upvotes INT,
    upvote_velocity DOUBLE,
    comment_velocity DOUBLE,
    award_velocity DOUBLE,
    viral_score DOUBLE,
    data_source TEXT,
    processed_at TIMESTAMP,
    PRIMARY KEY ((movie_title, hour), window_start)
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
    hour TIMESTAMP,
    window_start TIMESTAMP,
    comment_count INT,
    total_upvotes BIGINT,
    total_awards INT,
    avg_sentiment FLOAT,
    max_upvotes INT,
    upvote_velocity DOUBLE,
    award_velocity DOUBLE,
    data_source TEXT,
    processed_at TIMESTAMP,
    PRIMARY KEY ((movie_title, hour), window_start)
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
