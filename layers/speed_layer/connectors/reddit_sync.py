"""
Reddit Cassandra to MongoDB Sync Connector
Syncs Reddit speed layer views from Cassandra to MongoDB for serving layer access.
"""

import logging
import time
import sys
import threading
from typing import Dict, List
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from flask import Flask, jsonify

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask app for health endpoint
health_app = Flask(__name__)
health_status = {
    'status': 'starting',
    'last_successful_sync': None,
    'total_syncs': 0,
    'total_records_synced': 0,
    'last_sync_stats': {},
    'last_error': None
}

@health_app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring."""
    status_code = 200 if health_status['status'] == 'healthy' else 503
    return jsonify(health_status), status_code

@health_app.route('/ready', methods=['GET'])
def readiness_check():
    """Readiness check - is the service ready to accept traffic."""
    ready = health_status['status'] in ['healthy', 'running']
    status_code = 200 if ready else 503
    return jsonify({
        'ready': ready,
        'status': health_status['status']
    }), status_code

def run_health_server():
    """Run Flask health server in background thread."""
    health_app.run(host='0.0.0.0', port=8081, debug=False, use_reloader=False)


class RedditCassandraToMongoSync:
    """Syncs Reddit speed layer data from Cassandra to MongoDB."""
    
    def __init__(
        self,
        cassandra_hosts: List[str],
        cassandra_keyspace: str = "speed_layer",
        mongo_uri: str = "mongodb://admin:password@localhost:27017",
        mongo_database: str = "moviedb",
        sync_collection: str = "speed_views"
    ):
        """Initialize sync connector."""
        self.cassandra_keyspace = cassandra_keyspace
        self.mongo_database = mongo_database
        self.sync_collection = sync_collection
        
        # Initialize Cassandra connection
        try:
            self.cassandra_cluster = Cluster(cassandra_hosts)
            self.cassandra_session = self.cassandra_cluster.connect(cassandra_keyspace)
            logger.info(f"✅ Connected to Cassandra keyspace: {cassandra_keyspace}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Cassandra: {e}")
            raise
        
        # Initialize MongoDB connection
        try:
            self.mongo_client = MongoClient(mongo_uri, authSource='admin')
            self.mongo_db = self.mongo_client[mongo_database]
            self.mongo_collection = self.mongo_db[sync_collection]
            logger.info(f"✅ Connected to MongoDB database: {mongo_database}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
        
        # Create indexes for efficient queries
        self._create_indexes()
    
    def _create_indexes(self):
        """Create MongoDB indexes for speed_views collection."""
        try:
            # Primary index: movie_title + data_type + hour
            self.mongo_collection.create_index([
                ("movie_title", 1),
                ("data_type", 1),
                ("hour", -1)
            ])
            
            # Data type + hour (for time-series queries)
            self.mongo_collection.create_index([
                ("data_type", 1),
                ("hour", -1)
            ])
            
            # TTL index (auto-delete after 48 hours)
            self.mongo_collection.create_index(
                [("ttl_expires_at", 1)],
                expireAfterSeconds=0
            )
            
            # Viral score index for trending queries
            self.mongo_collection.create_index([
                ("data_type", 1),
                ("metrics.viral_score", -1)
            ])
            
            # Sentiment index
            self.mongo_collection.create_index([
                ("data_type", 1),
                ("metrics.avg_sentiment", -1)
            ])
            
            logger.info("✅ MongoDB indexes created successfully")
        except Exception as e:
            logger.warning(f"⚠️ Index creation warning (may already exist): {e}")
    
    def sync_all_views(self) -> Dict[str, int]:
        """
        Sync all Reddit speed layer views from Cassandra to MongoDB.
        
        Returns:
            Dictionary with sync statistics
        """
        logger.info("🔄 Starting full sync of Reddit speed layer views")
        start_time = time.time()
        
        stats = {
            'post_metrics': 0,
            'comment_metrics': 0,
            'total_synced': 0,
            'errors': 0
        }
        
        # Sync Reddit post metrics
        try:
            count = self._sync_post_metrics()
            stats['post_metrics'] = count
            stats['total_synced'] += count
        except Exception as e:
            logger.error(f"❌ Failed to sync post metrics: {e}")
            stats['errors'] += 1
        
        # Sync Reddit comment metrics
        try:
            count = self._sync_comment_metrics()
            stats['comment_metrics'] = count
            stats['total_synced'] += count
        except Exception as e:
            logger.error(f"❌ Failed to sync comment metrics: {e}")
            stats['errors'] += 1
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Sync completed in {elapsed:.2f}s - Stats: {stats}")
        
        return stats
    
    def _sync_post_metrics(self) -> int:
        """Sync Reddit post metrics from Cassandra to MongoDB."""
        query = """
        SELECT movie_title, hour, window_start,
               post_count, total_upvotes, avg_upvote_ratio,
               total_comments, total_awards, avg_sentiment,
               max_upvotes, upvote_velocity, comment_velocity,
               award_velocity, viral_score, data_source, processed_at
        FROM reddit_post_metrics
        """
        
        try:
            rows = self.cassandra_session.execute(query)
            bulk_ops = []
            
            for row in rows:
                # Calculate TTL expiration (48 hours from hour)
                ttl_expires_at = row.hour + timedelta(hours=48)
                
                # Structure document for MongoDB
                doc = {
                    'movie_title': row.movie_title,
                    'data_type': 'reddit_post',
                    'hour': row.hour,
                    'window_start': row.window_start,
                    'metrics': {
                        'post_count': int(row.post_count) if row.post_count else 0,
                        'total_upvotes': int(row.total_upvotes) if row.total_upvotes else 0,
                        'avg_upvote_ratio': float(row.avg_upvote_ratio) if row.avg_upvote_ratio else 0.0,
                        'total_comments': int(row.total_comments) if row.total_comments else 0,
                        'total_awards': int(row.total_awards) if row.total_awards else 0,
                        'avg_sentiment': float(row.avg_sentiment) if row.avg_sentiment else 0.0,
                        'max_upvotes': int(row.max_upvotes) if row.max_upvotes else 0,
                        'upvote_velocity': float(row.upvote_velocity) if row.upvote_velocity else 0.0,
                        'comment_velocity': float(row.comment_velocity) if row.comment_velocity else 0.0,
                        'award_velocity': float(row.award_velocity) if row.award_velocity else 0.0,
                        'viral_score': float(row.viral_score) if row.viral_score else 0.0
                    },
                    'data_source': row.data_source if row.data_source else 'reddit',
                    'processed_at': row.processed_at,
                    'synced_at': datetime.utcnow(),
                    'ttl_expires_at': ttl_expires_at
                }
                
                # Use upsert to avoid duplicates
                bulk_ops.append(
                    UpdateOne(
                        {
                            'movie_title': row.movie_title,
                            'data_type': 'reddit_post',
                            'hour': row.hour,
                            'window_start': row.window_start
                        },
                        {'$set': doc},
                        upsert=True
                    )
                )
            
            if bulk_ops:
                result = self.mongo_collection.bulk_write(bulk_ops, ordered=False)
                count = result.upserted_count + result.modified_count
                logger.info(f"✅ Synced {count} Reddit post metric records")
                return count
            else:
                logger.info("ℹ️ No post metrics to sync")
                return 0
                
        except Exception as e:
            logger.error(f"❌ Error syncing post metrics: {e}")
            raise
    
    def _sync_comment_metrics(self) -> int:
        """Sync Reddit comment metrics from Cassandra to MongoDB."""
        query = """
        SELECT movie_title, hour, window_start,
               comment_count, total_upvotes, total_awards,
               avg_sentiment, max_upvotes, data_source, processed_at
        FROM reddit_comment_metrics
        """
        
        try:
            rows = self.cassandra_session.execute(query)
            bulk_ops = []
            
            for row in rows:
                # Calculate TTL expiration (48 hours from hour)
                ttl_expires_at = row.hour + timedelta(hours=48)
                
                # Structure document for MongoDB
                doc = {
                    'movie_title': row.movie_title,
                    'data_type': 'reddit_comment',
                    'hour': row.hour,
                    'window_start': row.window_start,
                    'metrics': {
                        'comment_count': int(row.comment_count) if row.comment_count else 0,
                        'total_upvotes': int(row.total_upvotes) if row.total_upvotes else 0,
                        'total_awards': int(row.total_awards) if row.total_awards else 0,
                        'avg_sentiment': float(row.avg_sentiment) if row.avg_sentiment else 0.0,
                        'max_upvotes': int(row.max_upvotes) if row.max_upvotes else 0
                    },
                    'data_source': row.data_source if row.data_source else 'reddit',
                    'processed_at': row.processed_at,
                    'synced_at': datetime.utcnow(),
                    'ttl_expires_at': ttl_expires_at
                }
                
                # Use upsert to avoid duplicates
                bulk_ops.append(
                    UpdateOne(
                        {
                            'movie_title': row.movie_title,
                            'data_type': 'reddit_comment',
                            'hour': row.hour,
                            'window_start': row.window_start
                        },
                        {'$set': doc},
                        upsert=True
                    )
                )
            
            if bulk_ops:
                result = self.mongo_collection.bulk_write(bulk_ops, ordered=False)
                count = result.upserted_count + result.modified_count
                logger.info(f"✅ Synced {count} Reddit comment metric records")
                return count
            else:
                logger.info("ℹ️ No comment metrics to sync")
                return 0
                
        except Exception as e:
            logger.error(f"❌ Error syncing comment metrics: {e}")
            raise
    
    def close(self):
        """Close connections."""
        if hasattr(self, 'cassandra_cluster'):
            self.cassandra_cluster.shutdown()
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()
        logger.info("✅ Connections closed")


def main():
    """Main sync loop."""
    # Configuration
    CASSANDRA_HOSTS = ['speed-cassandra']
    MONGO_URI = "mongodb://admin:password@serving-mongodb:27017"
    SYNC_INTERVAL = 300  # 5 minutes
    
    logger.info("🚀 Starting Reddit Cassandra → MongoDB sync service")
    logger.info(f"⏱️ Sync interval: {SYNC_INTERVAL} seconds (5 minutes)")
    
    # Start health check server in background thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    logger.info("Health check server started on port 8081")
    
    sync = None
    health_status['status'] = 'running'
    
    try:
        # Initialize sync connector
        sync = RedditCassandraToMongoSync(
            cassandra_hosts=CASSANDRA_HOSTS,
            mongo_uri=MONGO_URI
        )
        
        # Continuous sync loop
        while True:
            try:
                stats = sync.sync_all_views()
                logger.info(f"📊 Sync stats: {stats}")
                
                # Update health status
                health_status['status'] = 'healthy'
                health_status['last_successful_sync'] = datetime.utcnow().isoformat()
                health_status['total_syncs'] += 1
                health_status['total_records_synced'] += stats.get('total_synced', 0)
                health_status['last_sync_stats'] = stats
                health_status['last_error'] = None
                
                if stats['total_synced'] > 0:
                    logger.info(f"✅ Successfully synced {stats['total_synced']} records")
                else:
                    logger.info("ℹ️ No new records to sync")
                
                # Wait for next sync interval
                logger.info(f"⏸️ Sleeping for {SYNC_INTERVAL} seconds...")
                time.sleep(SYNC_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("⛔ Received interrupt signal, shutting down...")
                health_status['status'] = 'stopped'
                break
            except Exception as e:
                logger.error(f"❌ Sync error: {e}")
                health_status['status'] = 'degraded'
                health_status['last_error'] = str(e)
                logger.info(f"⏸️ Retrying in {SYNC_INTERVAL} seconds...")
                time.sleep(SYNC_INTERVAL)
                
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        health_status['status'] = 'failed'
        health_status['last_error'] = str(e)
        sys.exit(1)
    finally:
        if sync:
            sync.close()
        logger.info("👋 Sync service stopped")


if __name__ == "__main__":
    main()
