"""
Cache Manager - Redis-based caching for API responses

Implements caching strategies to reduce database load and improve response times
"""

import redis
import json
import pickle
from typing import Any, Optional, Callable
from functools import wraps
from datetime import timedelta
import logging
import os
import hashlib

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Manages Redis-based caching for API responses
    """
    
    def __init__(
        self,
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: Optional[str] = None
    ):
        """
        Initialize cache manager
        
        Args:
            redis_host: Redis server host
            redis_port: Redis server port
            redis_db: Redis database number
            redis_password: Redis password (optional)
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        
        try:
            self.client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=False,  # We'll handle encoding
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            self.client.ping()
            logger.info(f"Successfully connected to Redis at {redis_host}:{redis_port}")
            
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.client = None
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache
        
        Args:
            key: Cache key
        
        Returns:
            Cached value or None if not found/expired
        """
        if not self.client:
            return None
        
        try:
            value = self.client.get(key)
            if value:
                logger.debug(f"Cache HIT: {key}")
                return pickle.loads(value)
            else:
                logger.debug(f"Cache MISS: {key}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting from cache: {e}")
            return None
    
    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: int = 300
    ) -> bool:
        """
        Set value in cache with TTL
        
        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: Time to live in seconds
        
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            return False
        
        try:
            serialized = pickle.dumps(value)
            self.client.setex(key, ttl_seconds, serialized)
            logger.debug(f"Cache SET: {key} (TTL: {ttl_seconds}s)")
            return True
            
        except Exception as e:
            logger.error(f"Error setting cache: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """
        Delete key from cache
        
        Args:
            key: Cache key
        
        Returns:
            True if deleted, False otherwise
        """
        if not self.client:
            return False
        
        try:
            result = self.client.delete(key)
            logger.debug(f"Cache DELETE: {key}")
            return result > 0
            
        except Exception as e:
            logger.error(f"Error deleting from cache: {e}")
            return False
    
    def clear_pattern(self, pattern: str) -> int:
        """
        Clear all keys matching pattern
        
        Args:
            pattern: Key pattern (e.g., "movie:*")
        
        Returns:
            Number of keys deleted
        """
        if not self.client:
            return 0
        
        try:
            keys = self.client.keys(pattern)
            if keys:
                deleted = self.client.delete(*keys)
                logger.info(f"Cache CLEAR: {len(keys)} keys matching '{pattern}'")
                return deleted
            return 0
            
        except Exception as e:
            logger.error(f"Error clearing cache pattern: {e}")
            return 0
    
    def get_stats(self) -> dict:
        """
        Get cache statistics
        
        Returns:
            Cache statistics dictionary
        """
        if not self.client:
            return {'status': 'disconnected'}
        
        try:
            info = self.client.info('stats')
            keyspace = self.client.info('keyspace')
            
            return {
                'status': 'connected',
                'total_keys': sum(
                    db_info.get('keys', 0) 
                    for db_info in keyspace.values()
                ),
                'hits': info.get('keyspace_hits', 0),
                'misses': info.get('keyspace_misses', 0),
                'hit_rate': self._calculate_hit_rate(
                    info.get('keyspace_hits', 0),
                    info.get('keyspace_misses', 0)
                ),
                'total_connections': info.get('total_connections_received', 0),
                'connected_clients': info.get('connected_clients', 0)
            }
            
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {'status': 'error', 'error': str(e)}
    
    @staticmethod
    def _calculate_hit_rate(hits: int, misses: int) -> float:
        """
        Calculate cache hit rate
        
        Args:
            hits: Number of cache hits
            misses: Number of cache misses
        
        Returns:
            Hit rate as percentage (0-100)
        """
        total = hits + misses
        if total == 0:
            return 0.0
        return round((hits / total) * 100, 2)
    
    @staticmethod
    def generate_cache_key(prefix: str, *args, **kwargs) -> str:
        """
        Generate cache key from arguments
        
        Args:
            prefix: Key prefix
            *args: Positional arguments
            **kwargs: Keyword arguments
        
        Returns:
            Cache key string
        """
        # Create a string representation of arguments
        key_parts = [prefix]
        
        # Add positional arguments
        for arg in args:
            key_parts.append(str(arg))
        
        # Add sorted keyword arguments
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        
        # Join and hash if too long
        key = ":".join(key_parts)
        if len(key) > 200:
            # Hash long keys
            key_hash = hashlib.md5(key.encode()).hexdigest()
            key = f"{prefix}:hash:{key_hash}"
        
        return key


# Decorator for caching function results
def cache_response(
    cache_manager: CacheManager,
    key_prefix: str,
    ttl_seconds: int = 300
):
    """
    Decorator to cache function responses
    
    Args:
        cache_manager: CacheManager instance
        key_prefix: Prefix for cache keys
        ttl_seconds: Time to live in seconds
    
    Returns:
        Decorated function
    
    Example:
        @cache_response(cache_mgr, 'movie', ttl_seconds=600)
        async def get_movie(movie_id: int):
            # ... fetch movie ...
            return movie_data
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = CacheManager.generate_cache_key(
                key_prefix,
                *args,
                **kwargs
            )
            
            # Try to get from cache
            cached_result = cache_manager.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Call function
            result = await func(*args, **kwargs)
            
            # Store in cache
            cache_manager.set(cache_key, result, ttl_seconds)
            
            return result
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = CacheManager.generate_cache_key(
                key_prefix,
                *args,
                **kwargs
            )
            
            # Try to get from cache
            cached_result = cache_manager.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Call function
            result = func(*args, **kwargs)
            
            # Store in cache
            cache_manager.set(cache_key, result, ttl_seconds)
            
            return result
        
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# Global cache manager instance
_cache_manager: Optional[CacheManager] = None


def get_cache_manager() -> CacheManager:
    """
    Get global cache manager instance
    
    Returns:
        CacheManager instance
    """
    global _cache_manager
    
    if _cache_manager is None:
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        redis_db = int(os.getenv('REDIS_DB', 0))
        redis_password = os.getenv('REDIS_PASSWORD', None)
        
        _cache_manager = CacheManager(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            redis_password=redis_password
        )
    
    return _cache_manager


def close_cache_manager():
    """
    Close global cache manager connection
    """
    global _cache_manager
    
    if _cache_manager and _cache_manager.client:
        _cache_manager.client.close()
        _cache_manager = None
        logger.info("Cache manager connection closed")
