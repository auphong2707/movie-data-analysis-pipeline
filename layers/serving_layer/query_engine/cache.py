"""
Redis Cache Module for API Response Caching

Provides decorators and utilities for caching API responses in Redis.
Uses async Redis client for optimal performance with FastAPI.
"""

import redis.asyncio as aioredis
import json
import os
from typing import Optional, Any, Callable
from functools import wraps
import hashlib
import logging

logger = logging.getLogger(__name__)

# Global Redis client instance
_redis_client: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    """
    Get or create Redis client instance
    
    Returns:
        Async Redis client
    """
    global _redis_client
    if _redis_client is None:
        host = os.getenv('REDIS_HOST', 'serving-redis')
        port = int(os.getenv('REDIS_PORT', '6379'))
        password = os.getenv('REDIS_PASSWORD')
        
        redis_url = f"redis://{':' + password + '@' if password else ''}{host}:{port}/0"
        
        _redis_client = aioredis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            max_connections=50
        )
        
        # Test connection
        try:
            await _redis_client.ping()
            logger.info(f"Redis cache connected: {host}:{port}")
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            _redis_client = None
            raise
    
    return _redis_client


async def close_redis():
    """Close Redis connection"""
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        _redis_client = None
        logger.info("Redis connection closed")


def generate_cache_key(*args, **kwargs) -> str:
    """
    Generate cache key from function arguments
    
    Args:
        *args: Positional arguments
        **kwargs: Keyword arguments
    
    Returns:
        Hash-based cache key component
    """
    def is_cacheable(value):
        """Check if a value should be included in cache key"""
        if value is None or callable(value):
            return False
        # Exclude FastAPI dependency injection objects (like MovieQueries)
        # Check if it's a basic type that can be JSON serialized
        if isinstance(value, (str, int, float, bool, list, dict, tuple)):
            return True
        # Exclude class instances (dependencies)
        if hasattr(value, '__dict__'):
            return False
        return True
    
    key_parts = []
    
    # Add non-None positional args
    for arg in args:
        if is_cacheable(arg):
            key_parts.append(str(arg))
    
    # Add non-None keyword args (sorted for consistency)
    if kwargs:
        sorted_kwargs = sorted(
            (k, v) for k, v in kwargs.items() 
            if is_cacheable(v)
        )
        if sorted_kwargs:
            # Create hash of kwargs for shorter keys
            kwargs_str = json.dumps(sorted_kwargs, sort_keys=True)
            kwargs_hash = hashlib.md5(kwargs_str.encode()).hexdigest()[:8]
            key_parts.append(kwargs_hash)
    
    return ":".join(key_parts) if key_parts else "default"


def cached(ttl: int = 300, prefix: str = "api"):
    """
    Cache decorator for async route handlers
    
    Caches the return value of the decorated function in Redis.
    Cache key is generated from function name and arguments.
    
    Args:
        ttl: Time to live in seconds (default: 300 = 5 minutes)
        prefix: Cache key prefix for namespacing
    
    Usage:
        @cached(ttl=600, prefix="genres")
        async def get_genres():
            return {"genres": [...]}
    
    Returns:
        Decorated function with caching logic
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            key_suffix = generate_cache_key(*args, **kwargs)
            cache_key = f"{prefix}:{func.__name__}:{key_suffix}"
            
            # Try to get from cache
            try:
                redis = await get_redis()
                cached_value = await redis.get(cache_key)
                
                if cached_value:
                    logger.debug(f"Cache HIT: {cache_key}")
                    try:
                        return json.loads(cached_value)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Cache value decode error: {e}")
                        # Continue to function execution
            except Exception as e:
                logger.warning(f"Cache read error for {cache_key}: {e}")
                # Continue to function execution if cache read fails
            
            # Cache miss - execute function
            logger.debug(f"Cache MISS: {cache_key}")
            result = await func(*args, **kwargs)
            
            # Store in cache (fire and forget - don't block on cache write)
            try:
                redis = await get_redis()
                # Convert Pydantic models to dict before serialization
                if hasattr(result, 'model_dump'):
                    # Pydantic v2
                    cache_data = result.model_dump(mode='json')
                elif hasattr(result, 'dict'):
                    # Pydantic v1
                    cache_data = result.dict()
                else:
                    cache_data = result
                
                # Handle datetime objects and other non-JSON types
                serialized = json.dumps(cache_data, default=str)
                await redis.setex(cache_key, ttl, serialized)
                logger.debug(f"Cache SET: {cache_key} (TTL: {ttl}s)")
            except Exception as e:
                logger.warning(f"Cache write error for {cache_key}: {e}")
                # Don't fail request if cache write fails
            
            return result
        
        return wrapper
    return decorator


async def invalidate_cache(pattern: str) -> int:
    """
    Invalidate cache keys matching pattern
    
    Args:
        pattern: Redis key pattern (e.g., "recommendations:*")
    
    Returns:
        Number of keys deleted
    """
    try:
        redis = await get_redis()
        keys = []
        
        async for key in redis.scan_iter(match=pattern):
            keys.append(key)
        
        if keys:
            deleted = await redis.delete(*keys)
            logger.info(f"Invalidated {deleted} cache keys matching: {pattern}")
            return deleted
        
        return 0
    except Exception as e:
        logger.error(f"Cache invalidation error for {pattern}: {e}")
        return 0


async def get_cache_stats() -> dict:
    """
    Get Redis cache statistics
    
    Returns:
        Dictionary with cache stats
    """
    try:
        redis = await get_redis()
        info = await redis.info()
        stats = await redis.info('stats')
        
        hits = stats.get('keyspace_hits', 0)
        misses = stats.get('keyspace_misses', 0)
        total = hits + misses
        hit_rate = round((hits / total * 100) if total > 0 else 0, 2)
        
        return {
            "connected": True,
            "used_memory_mb": round(info.get('used_memory', 0) / 1024 / 1024, 2),
            "connected_clients": info.get('connected_clients', 0),
            "total_keys": sum(
                db.get('keys', 0) 
                for db in info.values() 
                if isinstance(db, dict)
            ),
            "keyspace_hits": hits,
            "keyspace_misses": misses,
            "hit_rate_percent": hit_rate
        }
    except Exception as e:
        logger.error(f"Failed to get cache stats: {e}")
        return {"connected": False, "error": str(e)}
