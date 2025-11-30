"""
MongoDB Index Management

Creates and manages indexes for optimal query performance
"""

from pymongo import ASCENDING, DESCENDING, TEXT
from pymongo.database import Database
from pymongo.errors import OperationFailure
import logging

logger = logging.getLogger(__name__)


class IndexManager:
    """
    Manages MongoDB indexes for serving layer collections
    """
    
    def __init__(self, db: Database):
        """
        Initialize index manager
        
        Args:
            db: MongoDB database instance
        """
        self.db = db
        self.batch_views = db.batch_views
        self.speed_views = db.speed_views
        self.cache_metadata = db.cache_metadata
    
    def create_all_indexes(self):
        """
        Create all indexes for serving layer collections
        """
        logger.info("Creating indexes for serving layer collections...")
        
        try:
            self.create_batch_views_indexes()
            self.create_speed_views_indexes()
            self.create_cache_metadata_indexes()
            
            logger.info("All indexes created successfully")
        
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            raise
    
    def create_batch_views_indexes(self):
        """
        Create indexes for batch_views collection
        """
        logger.info("Creating indexes for batch_views collection...")
        
        # Index 1: movie_id + view_type (most common query)
        self.batch_views.create_index(
            [("movie_id", ASCENDING), ("view_type", ASCENDING)],
            name="idx_movie_view_type"
        )
        logger.info("✓ Created index: idx_movie_view_type")
        
        # Index 2: view_type + computed_at (for time-based queries)
        self.batch_views.create_index(
            [("view_type", ASCENDING), ("computed_at", DESCENDING)],
            name="idx_view_type_time"
        )
        logger.info("✓ Created index: idx_view_type_time")
        
        # Index 3: genre analytics
        self.batch_views.create_index(
            [
                ("view_type", ASCENDING),
                ("data.genre", ASCENDING),
                ("data.year", DESCENDING),
                ("data.month", DESCENDING)
            ],
            name="idx_genre_analytics"
        )
        logger.info("✓ Created index: idx_genre_analytics")
        
        # Index 4: temporal queries
        self.batch_views.create_index(
            [
                ("view_type", ASCENDING),
                ("data.release_year", DESCENDING),
                ("computed_at", DESCENDING)
            ],
            name="idx_temporal_queries"
        )
        logger.info("✓ Created index: idx_temporal_queries")
        
        # Index 5: computed_at for cutoff queries
        self.batch_views.create_index(
            [("computed_at", DESCENDING)],
            name="idx_computed_at"
        )
        logger.info("✓ Created index: idx_computed_at")
        
        # Index 6: batch_run_id for batch tracking
        self.batch_views.create_index(
            [("batch_run_id", ASCENDING)],
            name="idx_batch_run_id"
        )
        logger.info("✓ Created index: idx_batch_run_id")
        
        # Index 7: Full-text search on movie titles
        try:
            self.batch_views.create_index(
                [("data.title", TEXT)],
                name="idx_title_text"
            )
            logger.info("✓ Created text index: idx_title_text")
        except OperationFailure as e:
            logger.warning(f"Text index creation failed (may already exist): {e}")
        
        logger.info("Batch views indexes created")
    
    def create_speed_views_indexes(self):
        """
        Create indexes for speed_views collection
        """
        logger.info("Creating indexes for speed_views collection...")
        
        # Index 1: movie_id + data_type + hour (primary query pattern)
        self.speed_views.create_index(
            [
                ("movie_id", ASCENDING),
                ("data_type", ASCENDING),
                ("hour", DESCENDING)
            ],
            name="idx_movie_type_hour"
        )
        logger.info("✓ Created index: idx_movie_type_hour")
        
        # Index 2: data_type + hour (for aggregations)
        self.speed_views.create_index(
            [("data_type", ASCENDING), ("hour", DESCENDING)],
            name="idx_type_hour"
        )
        logger.info("✓ Created index: idx_type_hour")
        
        # Index 3: hour for time-based queries
        self.speed_views.create_index(
            [("hour", DESCENDING)],
            name="idx_hour"
        )
        logger.info("✓ Created index: idx_hour")
        
        # Index 4: TTL index for automatic expiration (48 hours)
        self.speed_views.create_index(
            [("ttl_expires_at", ASCENDING)],
            name="idx_ttl",
            expireAfterSeconds=0  # Document expires at ttl_expires_at time
        )
        logger.info("✓ Created TTL index: idx_ttl")
        
        # Index 5: synced_at for monitoring
        self.speed_views.create_index(
            [("synced_at", DESCENDING)],
            name="idx_synced_at"
        )
        logger.info("✓ Created index: idx_synced_at")
        
        # Index 6: trending queries (genre + popularity)
        self.speed_views.create_index(
            [
                ("data_type", ASCENDING),
                ("data.genre", ASCENDING),
                ("data.popularity", DESCENDING)
            ],
            name="idx_trending"
        )
        logger.info("✓ Created index: idx_trending")
        
        logger.info("Speed views indexes created")
    
    def create_cache_metadata_indexes(self):
        """
        Create indexes for cache_metadata collection
        """
        logger.info("Creating indexes for cache_metadata collection...")
        
        # Index 1: cache_key (unique)
        self.cache_metadata.create_index(
            [("cache_key", ASCENDING)],
            name="idx_cache_key",
            unique=True
        )
        logger.info("✓ Created unique index: idx_cache_key")
        
        # Index 2: expires_at for cache cleanup
        self.cache_metadata.create_index(
            [("expires_at", ASCENDING)],
            name="idx_expires_at"
        )
        logger.info("✓ Created index: idx_expires_at")
        
        # Index 3: cached_at for monitoring
        self.cache_metadata.create_index(
            [("cached_at", DESCENDING)],
            name="idx_cached_at"
        )
        logger.info("✓ Created index: idx_cached_at")
        
        logger.info("Cache metadata indexes created")
    
    def list_all_indexes(self) -> dict:
        """
        List all indexes for all collections
        
        Returns:
            Dictionary with collection names and their indexes
        """
        return {
            "batch_views": list(self.batch_views.list_indexes()),
            "speed_views": list(self.speed_views.list_indexes()),
            "cache_metadata": list(self.cache_metadata.list_indexes())
        }
    
    def drop_all_indexes(self, confirm: bool = False):
        """
        Drop all indexes (except _id)
        
        Args:
            confirm: Must be True to actually drop indexes
        """
        if not confirm:
            logger.warning("Index drop not confirmed - no action taken")
            return
        
        logger.warning("Dropping all indexes...")
        
        try:
            self.batch_views.drop_indexes()
            self.speed_views.drop_indexes()
            self.cache_metadata.drop_indexes()
            
            logger.info("All indexes dropped")
        
        except Exception as e:
            logger.error(f"Error dropping indexes: {e}")
            raise
    
    def get_index_stats(self) -> dict:
        """
        Get index usage statistics
        
        Returns:
            Dictionary with index statistics
        """
        stats = {}
        
        for collection_name in ["batch_views", "speed_views", "cache_metadata"]:
            collection = self.db[collection_name]
            
            # Get index stats
            index_stats = list(collection.aggregate([
                {"$indexStats": {}}
            ]))
            
            stats[collection_name] = index_stats
        
        return stats


def setup_indexes(db: Database):
    """
    Setup all indexes for serving layer
    
    Args:
        db: MongoDB database instance
    """
    manager = IndexManager(db)
    manager.create_all_indexes()


if __name__ == "__main__":
    # For manual index creation
    from .client import get_database
    
    logging.basicConfig(level=logging.INFO)
    
    db = get_database()
    setup_indexes(db)
