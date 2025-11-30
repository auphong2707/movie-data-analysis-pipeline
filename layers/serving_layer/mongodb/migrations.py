"""
Database Migrations

Handles schema migrations and data transformations for serving layer
"""

from pymongo.database import Database
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class Migration:
    """Base class for database migrations"""
    
    def __init__(self, db: Database):
        self.db = db
    
    def up(self):
        """Apply migration"""
        raise NotImplementedError("Migration must implement up() method")
    
    def down(self):
        """Rollback migration"""
        raise NotImplementedError("Migration must implement down() method")
    
    @property
    def version(self) -> str:
        """Migration version identifier"""
        raise NotImplementedError("Migration must define version property")


class Migration_001_InitialSchema(Migration):
    """
    Migration 001: Create initial serving layer schema
    
    - Creates batch_views collection
    - Creates speed_views collection
    - Creates cache_metadata collection
    - Sets up TTL indexes
    """
    
    @property
    def version(self) -> str:
        return "001_initial_schema"
    
    def up(self):
        logger.info(f"Applying migration: {self.version}")
        
        # Create collections if they don't exist
        collections = self.db.list_collection_names()
        
        if "batch_views" not in collections:
            self.db.create_collection("batch_views")
            logger.info("✓ Created batch_views collection")
        
        if "speed_views" not in collections:
            self.db.create_collection("speed_views")
            logger.info("✓ Created speed_views collection")
        
        if "cache_metadata" not in collections:
            self.db.create_collection("cache_metadata")
            logger.info("✓ Created cache_metadata collection")
        
        logger.info(f"Migration {self.version} completed")
    
    def down(self):
        logger.info(f"Rolling back migration: {self.version}")
        
        # Drop collections
        self.db.batch_views.drop()
        self.db.speed_views.drop()
        self.db.cache_metadata.drop()
        
        logger.info(f"Rollback {self.version} completed")


class Migration_002_AddViewTypeField(Migration):
    """
    Migration 002: Add view_type field to existing documents
    
    Ensures all documents have a view_type field for consistent querying
    """
    
    @property
    def version(self) -> str:
        return "002_add_view_type"
    
    def up(self):
        logger.info(f"Applying migration: {self.version}")
        
        # Update batch_views documents without view_type
        result = self.db.batch_views.update_many(
            {"view_type": {"$exists": False}},
            {"$set": {"view_type": "unknown"}}
        )
        logger.info(f"✓ Updated {result.modified_count} batch_views documents")
        
        # Update speed_views documents
        result = self.db.speed_views.update_many(
            {"data_type": {"$exists": False}},
            {"$set": {"data_type": "unknown"}}
        )
        logger.info(f"✓ Updated {result.modified_count} speed_views documents")
        
        logger.info(f"Migration {self.version} completed")
    
    def down(self):
        logger.info(f"Rolling back migration: {self.version}")
        
        # Remove view_type fields
        self.db.batch_views.update_many(
            {"view_type": "unknown"},
            {"$unset": {"view_type": ""}}
        )
        
        self.db.speed_views.update_many(
            {"data_type": "unknown"},
            {"$unset": {"data_type": ""}}
        )
        
        logger.info(f"Rollback {self.version} completed")


class Migration_003_AddTTLField(Migration):
    """
    Migration 003: Add TTL expiration field to speed_views
    
    Sets ttl_expires_at field for existing documents (48 hours from synced_at)
    """
    
    @property
    def version(self) -> str:
        return "003_add_ttl_field"
    
    def up(self):
        logger.info(f"Applying migration: {self.version}")
        
        # Add TTL field to documents that don't have it
        pipeline = [
            {"$match": {"ttl_expires_at": {"$exists": False}}},
            {
                "$addFields": {
                    "ttl_expires_at": {
                        "$add": [
                            "$hour",
                            {"$multiply": [48, 60, 60, 1000]}  # 48 hours in milliseconds
                        ]
                    }
                }
            },
            {"$merge": {"into": "speed_views"}}
        ]
        
        self.db.speed_views.aggregate(pipeline)
        
        logger.info(f"✓ Added TTL field to speed_views documents")
        logger.info(f"Migration {self.version} completed")
    
    def down(self):
        logger.info(f"Rolling back migration: {self.version}")
        
        # Remove TTL field
        self.db.speed_views.update_many(
            {},
            {"$unset": {"ttl_expires_at": ""}}
        )
        
        logger.info(f"Rollback {self.version} completed")


class MigrationManager:
    """
    Manages database migrations
    """
    
    def __init__(self, db: Database):
        """
        Initialize migration manager
        
        Args:
            db: MongoDB database instance
        """
        self.db = db
        self.migrations_collection = db.migrations
        
        # List of all migrations in order
        self.migrations = [
            Migration_001_InitialSchema,
            Migration_002_AddViewTypeField,
            Migration_003_AddTTLField
        ]
    
    def get_applied_migrations(self) -> set:
        """
        Get set of applied migration versions
        
        Returns:
            Set of migration version strings
        """
        applied = self.migrations_collection.find(
            {"status": "applied"},
            {"version": 1}
        )
        return {m["version"] for m in applied}
    
    def migrate_up(self, target_version: str = None):
        """
        Apply all pending migrations up to target version
        
        Args:
            target_version: Stop at this version (optional, applies all if None)
        """
        applied = self.get_applied_migrations()
        
        logger.info(f"Applied migrations: {applied}")
        logger.info("Applying pending migrations...")
        
        for migration_class in self.migrations:
            migration = migration_class(self.db)
            version = migration.version
            
            # Skip if already applied
            if version in applied:
                logger.info(f"⊘ Skipping {version} (already applied)")
                continue
            
            # Apply migration
            try:
                logger.info(f"⟳ Applying migration {version}...")
                migration.up()
                
                # Record migration
                self.migrations_collection.insert_one({
                    "version": version,
                    "status": "applied",
                    "applied_at": datetime.utcnow()
                })
                
                logger.info(f"✓ Migration {version} applied successfully")
                
                # Stop if we reached target version
                if target_version and version == target_version:
                    break
            
            except Exception as e:
                logger.error(f"✗ Migration {version} failed: {e}")
                
                # Record failure
                self.migrations_collection.insert_one({
                    "version": version,
                    "status": "failed",
                    "applied_at": datetime.utcnow(),
                    "error": str(e)
                })
                
                raise
        
        logger.info("All migrations applied successfully")
    
    def migrate_down(self, target_version: str = None):
        """
        Rollback migrations down to target version
        
        Args:
            target_version: Rollback to this version (optional, rolls back all if None)
        """
        applied = self.get_applied_migrations()
        
        logger.info("Rolling back migrations...")
        
        # Reverse order for rollback
        for migration_class in reversed(self.migrations):
            migration = migration_class(self.db)
            version = migration.version
            
            # Skip if not applied
            if version not in applied:
                continue
            
            # Stop if we reached target version
            if target_version and version == target_version:
                break
            
            # Rollback migration
            try:
                logger.info(f"⟲ Rolling back migration {version}...")
                migration.down()
                
                # Update migration record
                self.migrations_collection.update_one(
                    {"version": version},
                    {
                        "$set": {
                            "status": "rolled_back",
                            "rolled_back_at": datetime.utcnow()
                        }
                    }
                )
                
                logger.info(f"✓ Migration {version} rolled back successfully")
            
            except Exception as e:
                logger.error(f"✗ Rollback {version} failed: {e}")
                raise
        
        logger.info("Rollback completed")
    
    def get_migration_status(self) -> list:
        """
        Get status of all migrations
        
        Returns:
            List of migration status documents
        """
        return list(self.migrations_collection.find().sort("applied_at", 1))


def run_migrations(db: Database):
    """
    Run all pending migrations
    
    Args:
        db: MongoDB database instance
    """
    manager = MigrationManager(db)
    manager.migrate_up()


if __name__ == "__main__":
    # For manual migration execution
    from .client import get_database
    
    logging.basicConfig(level=logging.INFO)
    
    db = get_database()
    run_migrations(db)
