"""
Cassandra to MongoDB Sync Connector
Syncs speed layer views from Cassandra to MongoDB for serving layer access.
"""

import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import os

logger = logging.getLogger(__name__)


class CassandraToMongoSync:
    """Syncs Cassandra speed layer data to MongoDB for unified serving."""
    
    def __init__(
        self,
        cassandra_hosts: List[str],
        cassandra_keyspace: str = "speed_layer",
        mongo_uri: str = "mongodb://localhost:27017",
        mongo_database: str = "moviedb",
        sync_collection: str = "speed_views"
    ):
        """
        Initialize sync connector.
        
        Args:
            cassandra_hosts: List of Cassandra contact points
            cassandra_keyspace: Cassandra keyspace name
            mongo_uri: MongoDB connection URI
            mongo_database: MongoDB database name
            sync_collection: MongoDB collection for speed views
        """
        self.cassandra_keyspace = cassandra_keyspace
        self.mongo_database = mongo_database
        self.sync_collection = sync_collection
        
        # Initialize Cassandra connection
        self.cassandra_cluster = Cluster(cassandra_hosts)
        self.cassandra_session = self.cassandra_cluster.connect(cassandra_keyspace)
        logger.info(f"Connected to Cassandra keyspace: {cassandra_keyspace}")
        
        # Initialize MongoDB connection
        self.mongo_client = MongoClient(mongo_uri)
        self.mongo_db = self.mongo_client[mongo_database]
        self.mongo_collection = self.mongo_db[sync_collection]
        logger.info(f"Connected to MongoDB database: {mongo_database}")
        
        # Create indexes for efficient queries
        self._create_indexes()
        
        # Metrics
        self.sync_count = 0
        self.error_count = 0
    
    def _create_indexes(self):
        """Create MongoDB indexes for efficient queries."""
        try:
            # Index on movie_id for fast lookups
            self.mongo_collection.create_index("movie_id")
            
            # Index on window_start for time-based queries
            self.mongo_collection.create_index("window_start")
            
            # Compound index for movie + time range queries
            self.mongo_collection.create_index([("movie_id", 1), ("window_start", -1)])
            
            # Index on updated_at for tracking freshness
            self.mongo_collection.create_index("updated_at")
            
            logger.info("MongoDB indexes created successfully")
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
    
    def sync_all_views(self) -> Dict[str, int]:
        """
        Sync all speed layer views from Cassandra to MongoDB.
        
        Returns:
            Dictionary with sync statistics
        """
        logger.info("Starting full sync of speed layer views")
        start_time = time.time()
        
        stats = {
            'review_sentiments': 0,
            'movie_stats': 0,
            'trending_movies': 0,
            'total_synced': 0,
            'errors': 0
        }
        
        # Sync review sentiments
        try:
            count = self._sync_review_sentiments()
            stats['review_sentiments'] = count
            stats['total_synced'] += count
        except Exception as e:
            logger.error(f"Failed to sync review sentiments: {e}")
            stats['errors'] += 1
        
        # Sync movie stats
        try:
            count = self._sync_movie_stats()
            stats['movie_stats'] = count
            stats['total_synced'] += count
        except Exception as e:
            logger.error(f"Failed to sync movie stats: {e}")
            stats['errors'] += 1
        
        # Sync trending movies
        try:
            count = self._sync_trending_movies()
            stats['trending_movies'] = count
            stats['total_synced'] += count
        except Exception as e:
            logger.error(f"Failed to sync trending movies: {e}")
            stats['errors'] += 1
        
        elapsed = time.time() - start_time
        logger.info(f"Sync completed in {elapsed:.2f}s - Stats: {stats}")
        
        return stats
    
    def _sync_review_sentiments(self) -> int:
        """Sync review sentiment data."""
        query = """
        SELECT window_start, window_end, movie_id, review_count, 
               avg_sentiment_compound, avg_sentiment_positive, 
               avg_sentiment_negative, avg_sentiment_neutral, 
               avg_rating, positive_count, negative_count, neutral_count,
               created_at
        FROM review_sentiments
        """
        
        rows = self.cassandra_session.execute(query)
        bulk_ops = []
        
        for row in rows:
            doc = {
                'movie_id': row.movie_id,
                'window_start': row.window_start,
                'window_end': row.window_end,
                'data_type': 'sentiment',
                'sentiment': {
                    'review_count': row.review_count,
                    'avg_compound': row.avg_sentiment_compound,
                    'avg_positive': row.avg_sentiment_positive,
                    'avg_negative': row.avg_sentiment_negative,
                    'avg_neutral': row.avg_sentiment_neutral,
                    'avg_rating': row.avg_rating,
                    'positive_count': row.positive_count,
                    'negative_count': row.negative_count,
                    'neutral_count': row.neutral_count
                },
                'created_at': row.created_at,
                'updated_at': datetime.utcnow()
            }
            
            # Use upsert to avoid duplicates
            bulk_ops.append(
                UpdateOne(
                    {
                        'movie_id': row.movie_id,
                        'window_start': row.window_start,
                        'data_type': 'sentiment'
                    },
                    {'$set': doc},
                    upsert=True
                )
            )
        
        if bulk_ops:
            result = self.mongo_collection.bulk_write(bulk_ops, ordered=False)
            count = result.upserted_count + result.modified_count
            logger.info(f"Synced {count} review sentiment records")
            return count
        
        return 0
    
    def _sync_movie_stats(self) -> int:
        """Sync movie statistics data."""
        query = """
        SELECT window_start, window_end, movie_id, title,
               avg_popularity, avg_vote_average, avg_vote_count,
               update_count, min_popularity, max_popularity,
               popularity_stddev, popularity_velocity, 
               popularity_acceleration, created_at
        FROM movie_stats
        """
        
        rows = self.cassandra_session.execute(query)
        bulk_ops = []
        
        for row in rows:
            doc = {
                'movie_id': row.movie_id,
                'window_start': row.window_start,
                'window_end': row.window_end,
                'data_type': 'stats',
                'title': row.title,
                'stats': {
                    'avg_popularity': row.avg_popularity,
                    'avg_vote_average': row.avg_vote_average,
                    'avg_vote_count': row.avg_vote_count,
                    'update_count': row.update_count,
                    'min_popularity': row.min_popularity,
                    'max_popularity': row.max_popularity,
                    'popularity_stddev': row.popularity_stddev,
                    'popularity_velocity': row.popularity_velocity,
                    'popularity_acceleration': row.popularity_acceleration
                },
                'created_at': row.created_at,
                'updated_at': datetime.utcnow()
            }
            
            bulk_ops.append(
                UpdateOne(
                    {
                        'movie_id': row.movie_id,
                        'window_start': row.window_start,
                        'data_type': 'stats'
                    },
                    {'$set': doc},
                    upsert=True
                )
            )
        
        if bulk_ops:
            result = self.mongo_collection.bulk_write(bulk_ops, ordered=False)
            count = result.upserted_count + result.modified_count
            logger.info(f"Synced {count} movie stats records")
            return count
        
        return 0
    
    def _sync_trending_movies(self) -> int:
        """Sync trending movies data."""
        query = """
        SELECT window_start, window_end, movie_id, title,
               trend_rank, hotness_score, avg_trend_score,
               trend_velocity, trend_acceleration,
               total_review_volume, event_count, velocity_score,
               acceleration_score, volume_score,
               created_at
        FROM trending_movies
        """
        
        rows = self.cassandra_session.execute(query)
        bulk_ops = []
        
        for row in rows:
            doc = {
                'movie_id': row.movie_id,
                'window_start': row.window_start,
                'window_end': row.window_end,
                'data_type': 'trending',
                'title': row.title,
                'trending': {
                    'trend_rank': row.trend_rank,
                    'hotness_score': row.hotness_score,
                    'avg_trend_score': row.avg_trend_score,
                    'trend_velocity': row.trend_velocity,
                    'trend_acceleration': row.trend_acceleration,
                    'total_review_volume': row.total_review_volume,
                    'event_count': row.event_count,
                    'velocity_score': row.velocity_score,
                    'acceleration_score': row.acceleration_score,
                    'volume_score': row.volume_score
                },
                'created_at': row.created_at,
                'updated_at': datetime.utcnow()
            }
            
            bulk_ops.append(
                UpdateOne(
                    {
                        'movie_id': row.movie_id,
                        'window_start': row.window_start,
                        'data_type': 'trending'
                    },
                    {'$set': doc},
                    upsert=True
                )
            )
        
        if bulk_ops:
            result = self.mongo_collection.bulk_write(bulk_ops, ordered=False)
            count = result.upserted_count + result.modified_count
            logger.info(f"Synced {count} trending movies records")
            return count
        
        return 0
    
    def get_merged_view(self, movie_id: int) -> Optional[Dict[str, Any]]:
        """
        Get merged speed layer view for a movie.
        
        Args:
            movie_id: Movie ID to query
            
        Returns:
            Merged document with sentiment, stats, and trending data
        """
        # Get latest window data for each type
        pipeline = [
            {'$match': {'movie_id': movie_id}},
            {'$sort': {'window_start': -1}},
            {'$group': {
                '_id': '$data_type',
                'latest': {'$first': '$$ROOT'}
            }}
        ]
        
        results = list(self.mongo_collection.aggregate(pipeline))
        
        if not results:
            return None
        
        # Merge results
        merged = {
            'movie_id': movie_id,
            'last_updated': datetime.utcnow()
        }
        
        for result in results:
            data_type = result['_id']
            doc = result['latest']
            
            if data_type == 'sentiment':
                merged['sentiment'] = doc.get('sentiment', {})
            elif data_type == 'stats':
                merged['stats'] = doc.get('stats', {})
                merged['title'] = doc.get('title')
            elif data_type == 'trending':
                merged['trending'] = doc.get('trending', {})
        
        return merged
    
    def cleanup_expired_data(self, ttl_hours: int = 48):
        """
        Remove expired data from MongoDB (older than TTL).
        
        Args:
            ttl_hours: Time-to-live in hours (default: 48)
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=ttl_hours)
        
        result = self.mongo_collection.delete_many({
            'window_start': {'$lt': cutoff_time}
        })
        
        logger.info(f"Cleaned up {result.deleted_count} expired documents")
        return result.deleted_count
    
    def close(self):
        """Close all connections."""
        if self.cassandra_cluster:
            self.cassandra_cluster.shutdown()
            logger.info("Cassandra connection closed")
        
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")


def main():
    """Main entry point for sync connector."""
    import argparse
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='Sync Cassandra speed layer to MongoDB')
    parser.add_argument('--cassandra-hosts', 
                       default=os.getenv('CASSANDRA_HOSTS', 'localhost'),
                       help='Cassandra contact points (comma-separated)')
    parser.add_argument('--cassandra-keyspace',
                       default=os.getenv('CASSANDRA_KEYSPACE', 'speed_layer'),
                       help='Cassandra keyspace')
    parser.add_argument('--mongo-uri',
                       default=os.getenv('MONGO_URI', 'mongodb://admin:password@localhost:27017'),
                       help='MongoDB connection URI')
    parser.add_argument('--mongo-database',
                       default=os.getenv('MONGO_DATABASE', 'moviedb'),
                       help='MongoDB database name')
    parser.add_argument('--sync-interval',
                       type=int,
                       default=int(os.getenv('SYNC_INTERVAL', '300')),
                       help='Sync interval in seconds (default: 300)')
    parser.add_argument('--cleanup-interval',
                       type=int,
                       default=int(os.getenv('CLEANUP_INTERVAL', '3600')),
                       help='Cleanup interval in seconds (default: 3600)')
    parser.add_argument('--once',
                       action='store_true',
                       help='Run sync once and exit')
    
    args = parser.parse_args()
    
    # Parse Cassandra hosts
    cassandra_hosts = args.cassandra_hosts.split(',')
    
    # Create sync connector
    sync = CassandraToMongoSync(
        cassandra_hosts=cassandra_hosts,
        cassandra_keyspace=args.cassandra_keyspace,
        mongo_uri=args.mongo_uri,
        mongo_database=args.mongo_database
    )
    
    try:
        if args.once:
            # Run sync once
            logger.info("Running one-time sync...")
            stats = sync.sync_all_views()
            logger.info(f"Sync completed: {stats}")
        else:
            # Run continuous sync
            logger.info(f"Starting continuous sync (interval: {args.sync_interval}s)")
            last_cleanup = time.time()
            
            while True:
                try:
                    # Sync data
                    stats = sync.sync_all_views()
                    
                    # Cleanup expired data periodically
                    if time.time() - last_cleanup > args.cleanup_interval:
                        sync.cleanup_expired_data()
                        last_cleanup = time.time()
                    
                    # Wait for next sync
                    time.sleep(args.sync_interval)
                    
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, stopping...")
                    break
                except Exception as e:
                    logger.error(f"Sync error: {e}", exc_info=True)
                    time.sleep(10)  # Wait before retry
    
    finally:
        sync.close()
        logger.info("Sync connector stopped")


if __name__ == '__main__':
    main()
