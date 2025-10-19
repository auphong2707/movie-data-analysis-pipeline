"""
Speed View Manager for Cassandra Operations
Handles CRUD operations, upserts, and data management for speed layer tables.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import SimpleStatement, BatchStatement, BatchType
from cassandra.util import uuid_from_time
import yaml

logger = logging.getLogger(__name__)


class CassandraConnectionManager:
    """Manages Cassandra connections and session lifecycle."""
    
    def __init__(self, config_path: str = "config/cassandra_config.yaml"):
        """Initialize Cassandra connection manager."""
        
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.cluster = None
        self.session = None
        self._connect()
        
        logger.info("Cassandra Connection Manager initialized")
    
    def _connect(self):
        """Establish connection to Cassandra cluster."""
        
        cassandra_config = self.config['cassandra']
        
        # Authentication if provided
        auth_provider = None
        if 'username' in cassandra_config and 'password' in cassandra_config:
            auth_provider = PlainTextAuthProvider(
                username=cassandra_config['username'],
                password=cassandra_config['password']
            )
        
        # Load balancing policy
        load_balancing_policy = TokenAwarePolicy(
            DCAwareRoundRobinPolicy(local_dc=cassandra_config.get('local_dc'))
        )
        
        # Create cluster
        self.cluster = Cluster(
            contact_points=cassandra_config['hosts'],
            port=cassandra_config['port'],
            auth_provider=auth_provider,
            load_balancing_policy=load_balancing_policy,
            protocol_version=cassandra_config.get('protocol_version', 4),
            compression=cassandra_config.get('compression', 'lz4'),
            connect_timeout=cassandra_config.get('connect_timeout', 10),
            request_timeout=cassandra_config.get('request_timeout', 10)
        )
        
        try:
            # Connect and set keyspace
            self.session = self.cluster.connect()
            self.session.set_keyspace(cassandra_config['keyspace'])
            
            logger.info(f"Connected to Cassandra cluster: {cassandra_config['hosts']}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise
    
    def get_session(self) -> Session:
        """Get the Cassandra session."""
        if not self.session:
            self._connect()
        return self.session
    
    def close(self):
        """Close Cassandra connection."""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        logger.info("Cassandra connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SpeedViewManager:
    """Manages speed view operations for all Cassandra tables."""
    
    def __init__(self, connection_manager: CassandraConnectionManager):
        """Initialize speed view manager."""
        self.connection_manager = connection_manager
        self.session = connection_manager.get_session()
        
        # Prepare statements for better performance
        self._prepare_statements()
        
        logger.info("Speed View Manager initialized")
    
    def _prepare_statements(self):
        """Prepare commonly used statements."""
        
        # Review sentiments
        self.insert_review_sentiment = self.session.prepare("""
            INSERT INTO review_sentiments (
                window_start, window_end, movie_id, review_count,
                avg_sentiment_compound, avg_sentiment_positive, avg_sentiment_negative, avg_sentiment_neutral,
                avg_rating, positive_count, negative_count, neutral_count, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Movie statistics
        self.insert_movie_stats = self.session.prepare("""
            INSERT INTO movie_stats (
                window_start, window_end, movie_id, title, avg_popularity, avg_vote_average,
                avg_vote_count, update_count, min_popularity, max_popularity,
                popularity_stddev, popularity_velocity, popularity_acceleration, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Movie ratings
        self.insert_movie_ratings = self.session.prepare("""
            INSERT INTO movie_ratings_by_window (
                window_start, window_end, movie_id, rating_count, avg_rating,
                min_rating, max_rating, rating_stddev, high_ratings_count,
                medium_ratings_count, low_ratings_count, rating_velocity, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Trending movies
        self.insert_trending_movies = self.session.prepare("""
            INSERT INTO trending_movies (
                window_start, window_end, movie_id, title, trend_rank, hotness_score,
                avg_trend_score, trend_velocity, trend_acceleration, total_review_volume,
                event_count, velocity_score, acceleration_score, volume_score, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Breakout movies
        self.insert_breakout_movies = self.session.prepare("""
            INSERT INTO breakout_movies (
                window_start, window_end, movie_id, title, breakout_rank, breakout_score,
                surge_multiplier, trend_acceleration, trend_velocity, avg_trend_score,
                total_review_volume, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Declining movies
        self.insert_declining_movies = self.session.prepare("""
            INSERT INTO declining_movies (
                window_start, window_end, movie_id, title, decline_rank, decline_score,
                momentum_loss, trend_velocity, trend_acceleration, avg_trend_score,
                total_review_volume, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
    
    def upsert_review_sentiment(self, data: Dict[str, Any]) -> bool:
        """Upsert review sentiment data."""
        try:
            self.session.execute(self.insert_review_sentiment, [
                data['window_start'],
                data['window_end'], 
                data['movie_id'],
                data['review_count'],
                data['avg_sentiment_compound'],
                data['avg_sentiment_positive'],
                data['avg_sentiment_negative'],
                data['avg_sentiment_neutral'],
                data['avg_rating'],
                data['positive_count'],
                data['negative_count'],
                data['neutral_count'],
                datetime.now(timezone.utc)
            ])
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert review sentiment: {e}")
            return False
    
    def upsert_movie_stats(self, data: Dict[str, Any]) -> bool:
        """Upsert movie statistics data."""
        try:
            self.session.execute(self.insert_movie_stats, [
                data['window_start'],
                data['window_end'],
                data['movie_id'],
                data['title'],
                data['avg_popularity'],
                data['avg_vote_average'],
                data['avg_vote_count'],
                data['update_count'],
                data['min_popularity'],
                data['max_popularity'],
                data['popularity_stddev'],
                data['popularity_velocity'],
                data['popularity_acceleration'],
                datetime.now(timezone.utc)
            ])
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert movie stats: {e}")
            return False
    
    def upsert_movie_ratings(self, data: Dict[str, Any]) -> bool:
        """Upsert movie ratings data."""
        try:
            self.session.execute(self.insert_movie_ratings, [
                data['window_start'],
                data['window_end'],
                data['movie_id'],
                data['rating_count'],
                data['avg_rating'],
                data['min_rating'],
                data['max_rating'],
                data['rating_stddev'],
                data['high_ratings_count'],
                data['medium_ratings_count'],
                data['low_ratings_count'],
                data['rating_velocity'],
                datetime.now(timezone.utc)
            ])
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert movie ratings: {e}")
            return False
    
    def upsert_trending_movies(self, data: Dict[str, Any]) -> bool:
        """Upsert trending movies data."""
        try:
            self.session.execute(self.insert_trending_movies, [
                data['window_start'],
                data['window_end'],
                data['movie_id'],
                data['title'],
                data['trend_rank'],
                data['hotness_score'],
                data['avg_trend_score'],
                data['trend_velocity'],
                data['trend_acceleration'],
                data['total_review_volume'],
                data['event_count'],
                data['velocity_score'],
                data['acceleration_score'],
                data['volume_score'],
                datetime.now(timezone.utc)
            ])
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert trending movies: {e}")
            return False
    
    def upsert_breakout_movies(self, data: Dict[str, Any]) -> bool:
        """Upsert breakout movies data."""
        try:
            self.session.execute(self.insert_breakout_movies, [
                data['window_start'],
                data['window_end'],
                data['movie_id'],
                data['title'],
                data['breakout_rank'],
                data['breakout_score'],
                data['surge_multiplier'],
                data['trend_acceleration'],
                data['trend_velocity'],
                data['avg_trend_score'],
                data['total_review_volume'],
                datetime.now(timezone.utc)
            ])
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert breakout movies: {e}")
            return False
    
    def upsert_declining_movies(self, data: Dict[str, Any]) -> bool:
        """Upsert declining movies data."""
        try:
            self.session.execute(self.insert_declining_movies, [
                data['window_start'],
                data['window_end'],
                data['movie_id'],
                data['title'],
                data['decline_rank'],
                data['decline_score'],
                data['momentum_loss'],
                data['trend_velocity'],
                data['trend_acceleration'],
                data['avg_trend_score'],
                data['total_review_volume'],
                datetime.now(timezone.utc)
            ])
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert declining movies: {e}")
            return False
    
    def batch_upsert_review_sentiments(self, data_list: List[Dict[str, Any]]) -> int:
        """Batch upsert multiple review sentiment records."""
        batch = BatchStatement(BatchType.LOGGED)
        success_count = 0
        
        try:
            for data in data_list:
                batch.add(self.insert_review_sentiment, [
                    data['window_start'],
                    data['window_end'],
                    data['movie_id'],
                    data['review_count'],
                    data['avg_sentiment_compound'],
                    data['avg_sentiment_positive'],
                    data['avg_sentiment_negative'],
                    data['avg_sentiment_neutral'],
                    data['avg_rating'],
                    data['positive_count'],
                    data['negative_count'],
                    data['neutral_count'],
                    datetime.now(timezone.utc)
                ])
                success_count += 1
            
            self.session.execute(batch)
            logger.info(f"Batch upserted {success_count} review sentiment records")
            
        except Exception as e:
            logger.error(f"Batch upsert failed for review sentiments: {e}")
            success_count = 0
        
        return success_count
    
    def get_latest_movie_stats(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get latest movie statistics."""
        query = """
            SELECT * FROM latest_movie_stats
            LIMIT %s
        """
        
        try:
            rows = self.session.execute(query, [limit])
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to get latest movie stats: {e}")
            return []
    
    def get_top_trending_movies(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get top trending movies by hotness score."""
        query = """
            SELECT * FROM top_trending_movies
            LIMIT %s
        """
        
        try:
            rows = self.session.execute(query, [limit])
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to get top trending movies: {e}")
            return []
    
    def get_movie_sentiment_history(self, movie_id: int, hours_back: int = 24) -> List[Dict[str, Any]]:
        """Get sentiment history for a specific movie."""
        start_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        
        query = """
            SELECT * FROM review_sentiments
            WHERE movie_id = %s AND window_start >= %s
            ORDER BY window_start DESC
        """
        
        try:
            rows = self.session.execute(query, [movie_id, start_time])
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to get movie sentiment history: {e}")
            return []
    
    def get_trending_movies_by_window(self, window_start: datetime, 
                                    window_end: datetime) -> List[Dict[str, Any]]:
        """Get trending movies for a specific time window."""
        query = """
            SELECT * FROM trending_movies
            WHERE window_start = %s AND window_end = %s
            ORDER BY trend_rank
        """
        
        try:
            rows = self.session.execute(query, [window_start, window_end])
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to get trending movies by window: {e}")
            return []
    
    def cleanup_old_data(self, days_back: int = 3) -> int:
        """
        Cleanup old data beyond TTL for maintenance.
        Note: This is usually not needed due to TTL, but useful for manual cleanup.
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
        tables = [
            'review_sentiments', 'movie_stats', 'movie_ratings_by_window',
            'trending_movies', 'breakout_movies', 'declining_movies'
        ]
        
        total_deleted = 0
        
        for table in tables:
            try:
                query = f"DELETE FROM {table} WHERE window_start < %s"
                result = self.session.execute(query, [cutoff_time])
                logger.info(f"Cleaned up old data from {table}")
                total_deleted += 1
                
            except Exception as e:
                logger.error(f"Failed to cleanup {table}: {e}")
        
        return total_deleted
    
    def get_table_sizes(self) -> Dict[str, int]:
        """Get approximate row counts for all tables."""
        tables = [
            'review_sentiments', 'movie_stats', 'movie_ratings_by_window',
            'trending_movies', 'breakout_movies', 'declining_movies'
        ]
        
        sizes = {}
        
        for table in tables:
            try:
                query = f"SELECT COUNT(*) FROM {table}"
                result = self.session.execute(query)
                sizes[table] = result.one()[0]
                
            except Exception as e:
                logger.error(f"Failed to get size for {table}: {e}")
                sizes[table] = -1
        
        return sizes
    
    def record_metric(self, metric_name: str, value: float, 
                     tags: Dict[str, str] = None):
        """Record a performance metric."""
        query = """
            INSERT INTO speed_layer_metrics (metric_name, timestamp, value, tags)
            VALUES (?, ?, ?, ?)
        """
        
        try:
            self.session.execute(query, [
                metric_name,
                datetime.now(timezone.utc),
                value,
                tags or {}
            ])
            
        except Exception as e:
            logger.error(f"Failed to record metric {metric_name}: {e}")


class SpeedViewWriter:
    """High-level interface for writing speed view data."""
    
    def __init__(self, config_path: str = "config/cassandra_config.yaml"):
        """Initialize speed view writer."""
        self.connection_manager = CassandraConnectionManager(config_path)
        self.view_manager = SpeedViewManager(self.connection_manager)
        
        logger.info("Speed View Writer initialized")
    
    def write_spark_streaming_batch(self, df_rows: List[Dict[str, Any]], 
                                   table_type: str) -> int:
        """
        Write a batch of rows from Spark streaming to appropriate Cassandra table.
        
        Args:
            df_rows: List of dictionaries representing DataFrame rows
            table_type: Type of table ('review_sentiments', 'movie_stats', etc.)
            
        Returns:
            Number of successfully written records
        """
        success_count = 0
        
        for row in df_rows:
            try:
                if table_type == 'review_sentiments':
                    success = self.view_manager.upsert_review_sentiment(row)
                elif table_type == 'movie_stats':
                    success = self.view_manager.upsert_movie_stats(row)
                elif table_type == 'movie_ratings':
                    success = self.view_manager.upsert_movie_ratings(row)
                elif table_type == 'trending_movies':
                    success = self.view_manager.upsert_trending_movies(row)
                elif table_type == 'breakout_movies':
                    success = self.view_manager.upsert_breakout_movies(row)
                elif table_type == 'declining_movies':
                    success = self.view_manager.upsert_declining_movies(row)
                else:
                    logger.error(f"Unknown table type: {table_type}")
                    continue
                
                if success:
                    success_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to write row to {table_type}: {e}")
        
        # Record metric
        self.view_manager.record_metric(
            f"{table_type}_write_count",
            success_count,
            {"batch_size": len(df_rows)}
        )
        
        return success_count
    
    def close(self):
        """Close the writer."""
        self.connection_manager.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """Test the speed view manager."""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Test connection and basic operations
        with CassandraConnectionManager() as conn_mgr:
            view_manager = SpeedViewManager(conn_mgr)
            
            # Test table sizes
            sizes = view_manager.get_table_sizes()
            logger.info(f"Table sizes: {sizes}")
            
            # Test latest data queries
            latest_stats = view_manager.get_latest_movie_stats(limit=5)
            logger.info(f"Latest movie stats: {len(latest_stats)} records")
            
            trending = view_manager.get_top_trending_movies(limit=5)
            logger.info(f"Top trending movies: {len(trending)} records")
            
            logger.info("Speed view manager test completed successfully")
            
    except Exception as e:
        logger.error(f"Speed view manager test failed: {e}")
        raise


if __name__ == "__main__":
    main()