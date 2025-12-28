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
    
    Updated to work with 3 separate batch collections:
    - sentiment_baselines: Genre/franchise/yearly sentiment patterns
    - viral_thresholds: Genre/budget-tier/seasonal viral cutoffs
    - movie_intelligence: Individual movie competitive data
    """
    
    def __init__(self, db: Database):
        """
        Initialize index manager
        
        Args:
            db: MongoDB database instance
        """
        self.db = db
        # 3 separate batch collections
        self.sentiment_baselines = db.sentiment_baselines
        self.viral_thresholds = db.viral_thresholds
        self.movie_intelligence = db.movie_intelligence
        # Other collections
        self.speed_views = db.speed_views
        self.cache_metadata = db.cache_metadata
    
    def create_all_indexes(self):
        """
        Create all indexes for serving layer collections
        """
        logger.info("Creating indexes for serving layer collections...")
        
        try:
            self.create_sentiment_baselines_indexes()
            self.create_viral_thresholds_indexes()
            self.create_movie_intelligence_indexes()
            self.create_speed_views_indexes()
            self.create_cache_metadata_indexes()
            
            logger.info("All indexes created successfully")
        
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            raise
    
    def create_sentiment_baselines_indexes(self):
        """
        Create indexes for sentiment_baselines collection
        """
        logger.info("Creating indexes for sentiment_baselines collection...")
        
        # Index 1: genre (for genre-based queries)
        self.sentiment_baselines.create_index(
            [("genre", ASCENDING)],
            name="idx_genre"
        )
        logger.info("✓ Created index: idx_genre")
        
        # Index 2: franchise (for franchise-based queries)
        self.sentiment_baselines.create_index(
            [("franchise", ASCENDING)],
            name="idx_franchise"
        )
        logger.info("✓ Created index: idx_franchise")
        
        # Index 3: year (for temporal queries)
        self.sentiment_baselines.create_index(
            [("year", DESCENDING)],
            name="idx_year"
        )
        logger.info("✓ Created index: idx_year")
        
        # Index 4: genre + year (composite for common queries)
        self.sentiment_baselines.create_index(
            [("genre", ASCENDING), ("year", DESCENDING)],
            name="idx_genre_year"
        )
        logger.info("✓ Created index: idx_genre_year")
        
        # Index 5: franchise + year
        self.sentiment_baselines.create_index(
            [("franchise", ASCENDING), ("year", DESCENDING)],
            name="idx_franchise_year"
        )
        logger.info("✓ Created index: idx_franchise_year")
        
        # Index 6: batch_run_timestamp (for latest data)
        self.sentiment_baselines.create_index(
            [("batch_run_timestamp", DESCENDING)],
            name="idx_batch_run_timestamp"
        )
        logger.info("✓ Created index: idx_batch_run_timestamp")
        
        logger.info("Sentiment baselines indexes created")
    
    def create_viral_thresholds_indexes(self):
        """
        Create indexes for viral_thresholds collection
        """
        logger.info("Creating indexes for viral_thresholds collection...")
        
        # Index 1: genre (for genre-based queries)
        self.viral_thresholds.create_index(
            [("genre", ASCENDING)],
            name="idx_genre"
        )
        logger.info("✓ Created index: idx_genre")
        
        # Index 2: budget_tier (for budget-based queries)
        self.viral_thresholds.create_index(
            [("budget_tier", ASCENDING)],
            name="idx_budget_tier"
        )
        logger.info("✓ Created index: idx_budget_tier")
        
        # Index 3: season (for seasonal queries)
        self.viral_thresholds.create_index(
            [("season", ASCENDING)],
            name="idx_season"
        )
        logger.info("✓ Created index: idx_season")
        
        # Index 4: genre + budget_tier + season (composite)
        self.viral_thresholds.create_index(
            [("genre", ASCENDING), ("budget_tier", ASCENDING), ("season", ASCENDING)],
            name="idx_genre_budget_season"
        )
        logger.info("✓ Created index: idx_genre_budget_season")
        
        # Index 5: batch_run_timestamp (for latest data)
        self.viral_thresholds.create_index(
            [("batch_run_timestamp", DESCENDING)],
            name="idx_batch_run_timestamp"
        )
        logger.info("✓ Created index: idx_batch_run_timestamp")
        
        logger.info("Viral thresholds indexes created")
    
    def create_movie_intelligence_indexes(self):
        """
        Create indexes for movie_intelligence collection
        """
        logger.info("Creating indexes for movie_intelligence collection...")
        
        # Index 1: movie_id (unique, most common query)
        self.movie_intelligence.create_index(
            [("movie_id", ASCENDING)],
            name="idx_movie_id",
            unique=True
        )
        logger.info("✓ Created unique index: idx_movie_id")
        
        # Index 2: genre (for genre filtering)
        self.movie_intelligence.create_index(
            [("genre", ASCENDING)],
            name="idx_genre"
        )
        logger.info("✓ Created index: idx_genre")
        
        # Index 3: genres array (for multi-genre queries)
        self.movie_intelligence.create_index(
            [("genres", ASCENDING)],
            name="idx_genres"
        )
        logger.info("✓ Created index: idx_genres")
        
        # Index 4: release_year (for temporal queries)
        self.movie_intelligence.create_index(
            [("release_year", DESCENDING)],
            name="idx_release_year"
        )
        logger.info("✓ Created index: idx_release_year")
        
        # Index 5: franchise (for franchise queries)
        self.movie_intelligence.create_index(
            [("franchise", ASCENDING)],
            name="idx_franchise"
        )
        logger.info("✓ Created index: idx_franchise")
        
        # Index 6: genre + release_year (composite)
        self.movie_intelligence.create_index(
            [("genre", ASCENDING), ("release_year", DESCENDING)],
            name="idx_genre_year"
        )
        logger.info("✓ Created index: idx_genre_year")
        
        # Index 7: budget_tier (for budget-based queries)
        self.movie_intelligence.create_index(
            [("budget_tier", ASCENDING)],
            name="idx_budget_tier"
        )
        logger.info("✓ Created index: idx_budget_tier")
        
        # Index 8: vote_average (for rating queries)
        self.movie_intelligence.create_index(
            [("vote_average", DESCENDING)],
            name="idx_vote_average"
        )
        logger.info("✓ Created index: idx_vote_average")
        
        # Index 9: popularity (for trending queries)
        self.movie_intelligence.create_index(
            [("popularity", DESCENDING)],
            name="idx_popularity"
        )
        logger.info("✓ Created index: idx_popularity")
        
        # Index 10: batch_run_timestamp (for latest data)
        self.movie_intelligence.create_index(
            [("batch_run_timestamp", DESCENDING)],
            name="idx_batch_run_timestamp"
        )
        logger.info("✓ Created index: idx_batch_run_timestamp")
        
        # Index 11: Full-text search on title
        try:
            self.movie_intelligence.create_index(
                [("title", TEXT)],
                name="idx_title_text"
            )
            logger.info("✓ Created text index: idx_title_text")
        except OperationFailure as e:
            logger.warning(f"Text index creation failed (may already exist): {e}")
        
        logger.info("Movie intelligence indexes created")
    
    def create_batch_views_indexes(self):
        """
        DEPRECATED: Legacy method for backward compatibility
        Now creates indexes on the 3 separate collections
        """
        logger.warning("create_batch_views_indexes is deprecated - use create_all_indexes instead")
        self.create_sentiment_baselines_indexes()
        self.create_viral_thresholds_indexes()
        self.create_movie_intelligence_indexes()
    
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
            "sentiment_baselines": list(self.sentiment_baselines.list_indexes()),
            "viral_thresholds": list(self.viral_thresholds.list_indexes()),
            "movie_intelligence": list(self.movie_intelligence.list_indexes()),
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
            self.sentiment_baselines.drop_indexes()
            self.viral_thresholds.drop_indexes()
            self.movie_intelligence.drop_indexes()
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
        
        for collection_name in ["sentiment_baselines", "viral_thresholds", "movie_intelligence", "speed_views", "cache_metadata"]:
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
