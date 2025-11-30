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
        """Create MongoDB indexes per SCHEMA_REQUIREMENTS."""
        try:
            # Primary indexes
            self.mongo_collection.create_index([("movie_id", 1), ("data_type", 1), ("hour", -1)])
            self.mongo_collection.create_index([("data_type", 1), ("hour", -1)])
            
            # TTL index (auto-delete after 48 hours)
            self.mongo_collection.create_index(
                [("ttl_expires_at", 1)],
                expireAfterSeconds=0
            )
            
            # Trending queries (if stats.trending_score exists)
            self.mongo_collection.create_index([("data_type", 1), ("stats.trending_score", -1)])
            
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
        """Sync review sentiment data per SCHEMA_REQUIREMENTS."""
        query = """
        SELECT movie_id, hour, window_start, window_end, review_count, 
               avg_sentiment, positive_count, negative_count, neutral_count,
               sentiment_velocity
        FROM review_sentiments
        """
        
        rows = self.cassandra_session.execute(query)
        bulk_ops = []
        
        for row in rows:
            # Calculate TTL expiration (48 hours from hour)
            ttl_expires_at = row.hour + timedelta(hours=48)
            
            # Structure per SCHEMA_REQUIREMENTS
            doc = {
                'movie_id': row.movie_id,
                'data_type': 'sentiment',
                'hour': row.hour,
                'data': {
                    'avg_sentiment': float(row.avg_sentiment) if row.avg_sentiment else 0.0,
                    'review_count': int(row.review_count) if row.review_count else 0,
                    'positive_count': int(row.positive_count) if row.positive_count else 0,
                    'negative_count': int(row.negative_count) if row.negative_count else 0,
                    'neutral_count': int(row.neutral_count) if row.neutral_count else 0,
                    'sentiment_velocity': float(row.sentiment_velocity) if row.sentiment_velocity else 0.0
                },
                'synced_at': datetime.utcnow(),
                'ttl_expires_at': ttl_expires_at
            }
            
            # Use upsert to avoid duplicates
            bulk_ops.append(
                UpdateOne(
                    {
                        'movie_id': row.movie_id,
                        'data_type': 'sentiment',
                        'hour': row.hour
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
        """Sync movie statistics data per SCHEMA_REQUIREMENTS."""
        query = """
        SELECT movie_id, hour, vote_average, vote_count,
               popularity, rating_velocity, last_updated
        FROM movie_stats
        """
        
        rows = self.cassandra_session.execute(query)
        bulk_ops = []
        
        for row in rows:
            # Calculate TTL expiration (48 hours from hour)
            ttl_expires_at = row.hour + timedelta(hours=48)
            
            # Structure per SCHEMA_REQUIREMENTS
            doc = {
                'movie_id': row.movie_id,
                'data_type': 'stats',
                'hour': row.hour,
                'stats': {
                    'vote_average': float(row.vote_average) if row.vote_average else 0.0,
                    'vote_count': int(row.vote_count) if row.vote_count else 0,
                    'popularity': float(row.popularity) if row.popularity else 0.0,
                    'rating_velocity': float(row.rating_velocity) if row.rating_velocity else 0.0,
                    # Add optional fields if available
                    'popularity_velocity': 0.0,  # Compute if historical data available
                    'vote_velocity': 0,  # Compute if historical data available
                },
                'synced_at': datetime.utcnow(),
                'ttl_expires_at': ttl_expires_at
            }
            
            bulk_ops.append(
                UpdateOne(
                    {
                        'movie_id': row.movie_id,
                        'data_type': 'stats',
                        'hour': row.hour
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
        """
        Sync trending movies data.
        Note: This can be merged into stats data_type with trending_score field.
        """
        query = """
        SELECT hour, rank, movie_id, title,
               trending_score, velocity, acceleration
        FROM trending_movies
        """
        
        rows = self.cassandra_session.execute(query)
        bulk_ops = []
        
        for row in rows:
            # Calculate TTL expiration (48 hours from hour)
            ttl_expires_at = row.hour + timedelta(hours=48)
            
            # Update or create stats document with trending_score
            doc = {
                'movie_id': row.movie_id,
                'data_type': 'stats',
                'hour': row.hour,
                'synced_at': datetime.utcnow(),
                'ttl_expires_at': ttl_expires_at
            }
            
            # Add trending score to stats
            bulk_ops.append(
                UpdateOne(
                    {
                        'movie_id': row.movie_id,
                        'data_type': 'stats',
                        'hour': row.hour
                    },
                    {
                        '$set': {
                            'stats.trending_score': float(row.trending_score) if row.trending_score else 0.0,
                            'synced_at': doc['synced_at'],
                            'ttl_expires_at': doc['ttl_expires_at']
                        }
                    },
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
        Note: With TTL index on ttl_expires_at, MongoDB will auto-delete.
        This is a manual fallback cleanup.
        
        Args:
            ttl_hours: Time-to-live in hours (default: 48)
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=ttl_hours)
        
        result = self.mongo_collection.delete_many({
            'hour': {'$lt': cutoff_time}
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
