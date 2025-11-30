"""
MongoDB Client Management

Handles MongoDB connection pooling and client lifecycle
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from typing import Optional
import logging
import os

logger = logging.getLogger(__name__)

# Global MongoDB client instance
_mongodb_client: Optional[MongoClient] = None


def get_mongodb_client() -> MongoClient:
    """
    Get MongoDB client with connection pooling
    
    Returns:
        MongoClient: Connected MongoDB client
    
    Raises:
        ConnectionFailure: If unable to connect to MongoDB
    """
    global _mongodb_client
    
    if _mongodb_client is None:
        # Get MongoDB URI from environment or use default
        mongodb_uri = os.getenv(
            'MONGODB_URI', 
            'mongodb://admin:password@localhost:27017'
        )
        database_name = os.getenv('MONGODB_DATABASE', 'moviedb')
        
        try:
            # Create client with connection pooling
            _mongodb_client = MongoClient(
                mongodb_uri,
                maxPoolSize=100,
                minPoolSize=10,
                maxIdleTimeMS=60000,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=20000,
            )
            
            # Test connection
            _mongodb_client.admin.command('ping')
            logger.info(f"Successfully connected to MongoDB at {mongodb_uri}")
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    return _mongodb_client


def get_database(db_name: Optional[str] = None):
    """
    Get MongoDB database instance
    
    Args:
        db_name: Database name (optional, defaults to env var)
    
    Returns:
        Database instance
    """
    client = get_mongodb_client()
    db_name = db_name or os.getenv('MONGODB_DATABASE', 'moviedb')
    return client[db_name]


def close_mongodb_client():
    """
    Close MongoDB client connection
    """
    global _mongodb_client
    
    if _mongodb_client is not None:
        _mongodb_client.close()
        _mongodb_client = None
        logger.info("MongoDB client connection closed")


def check_mongodb_health() -> dict:
    """
    Check MongoDB connection health
    
    Returns:
        dict: Health status and metrics
    """
    try:
        client = get_mongodb_client()
        
        # Ping server
        start_time = os.times().elapsed
        client.admin.command('ping')
        end_time = os.times().elapsed
        latency_ms = (end_time - start_time) * 1000
        
        # Get server info
        server_info = client.server_info()
        
        return {
            'status': 'up',
            'latency_ms': round(latency_ms, 2),
            'version': server_info.get('version', 'unknown'),
            'connected': True
        }
        
    except Exception as e:
        logger.error(f"MongoDB health check failed: {e}")
        return {
            'status': 'down',
            'latency_ms': None,
            'error': str(e),
            'connected': False
        }


# Context manager for database operations
class MongoDBConnection:
    """
    Context manager for MongoDB database operations
    
    Usage:
        with MongoDBConnection() as db:
            result = db.batch_views.find_one({'movie_id': 12345})
    """
    
    def __init__(self, db_name: Optional[str] = None):
        self.db_name = db_name
        self.db = None
    
    def __enter__(self):
        self.db = get_database(self.db_name)
        return self.db
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Connection pooling handles cleanup automatically
        pass
