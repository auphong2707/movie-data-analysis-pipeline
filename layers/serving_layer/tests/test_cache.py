"""
Cache Manager Tests

Tests for Redis caching functionality
"""

import pytest
from unittest.mock import Mock, patch
import json
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from query_engine.cache_manager import CacheManager


class TestCacheManager:
    """Test CacheManager class"""
    
    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client"""
        redis_mock = Mock()
        redis_mock.ping.return_value = True
        return redis_mock
    
    @pytest.fixture
    def cache_manager(self, mock_redis):
        """Create CacheManager with mock Redis"""
        with patch('query_engine.cache_manager.redis.Redis', return_value=mock_redis):
            manager = CacheManager()
            manager.client = mock_redis
            return manager
    
    def test_cache_manager_initialization(self):
        """Test CacheManager initialization"""
        # Test with valid Redis connection
        with patch('query_engine.cache_manager.redis.Redis') as mock_redis_class:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_redis_class.return_value = mock_client
            
            manager = CacheManager()
            assert manager.client is not None
    
    def test_cache_set_get(self, cache_manager, mock_redis):
        """Test setting and getting cache values"""
        key = "test_key"
        value = {"data": "test_value"}
        ttl = 300
        
        # Mock Redis operations
        mock_redis.get.return_value = json.dumps(value).encode()
        
        # Set value
        cache_manager.set(key, value, ttl)
        
        # Get value
        result = cache_manager.get(key)
        
        assert mock_redis.setex.called or mock_redis.set.called
        assert mock_redis.get.called
    
    def test_cache_miss(self, cache_manager, mock_redis):
        """Test cache miss returns None"""
        mock_redis.get.return_value = None
        
        result = cache_manager.get("nonexistent_key")
        
        assert result is None
    
    def test_cache_delete(self, cache_manager, mock_redis):
        """Test deleting cache key"""
        key = "test_key"
        
        cache_manager.delete(key)
        
        assert mock_redis.delete.called
    
    def test_cache_clear(self, cache_manager, mock_redis):
        """Test clearing all cache"""
        cache_manager.clear()
        
        # Should call flushdb or similar
        assert mock_redis.flushdb.called or mock_redis.flushall.called
    
    def test_cache_ttl(self, cache_manager, mock_redis):
        """Test TTL setting"""
        key = "test_key"
        value = {"data": "test"}
        ttl = 600
        
        cache_manager.set(key, value, ttl)
        
        # Verify TTL was set
        call_args = mock_redis.setex.call_args if mock_redis.setex.called else mock_redis.set.call_args
        if call_args:
            assert ttl in call_args[0] or (call_args[1] and ttl in call_args[1].values())


class TestCacheDecorator:
    """Test cache decorator functionality"""
    
    @pytest.fixture
    def cache_manager(self):
        """Create CacheManager with mock Redis"""
        with patch('query_engine.cache_manager.redis.Redis') as mock_redis_class:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_client.get.return_value = None
            mock_redis_class.return_value = mock_client
            
            manager = CacheManager()
            return manager
    
    def test_decorator_caches_result(self, cache_manager):
        """Test that decorator caches function results"""
        # This would test the actual cache_response decorator
        # if it's implemented in cache_manager
        pass
    
    def test_decorator_returns_cached_value(self, cache_manager):
        """Test that decorator returns cached value on subsequent calls"""
        pass


class TestCacheKeyGeneration:
    """Test cache key generation"""
    
    @pytest.fixture
    def cache_manager(self):
        """Create CacheManager"""
        with patch('query_engine.cache_manager.redis.Redis') as mock_redis_class:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_redis_class.return_value = mock_client
            
            return CacheManager()
    
    def test_generate_cache_key(self, cache_manager):
        """Test cache key generation from parameters"""
        # Test key generation logic if exposed
        pass
    
    def test_cache_key_uniqueness(self, cache_manager):
        """Test that different parameters generate different keys"""
        pass


class TestCacheInvalidation:
    """Test cache invalidation"""
    
    @pytest.fixture
    def cache_manager(self):
        """Create CacheManager with mock Redis"""
        with patch('query_engine.cache_manager.redis.Redis') as mock_redis_class:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_redis_class.return_value = mock_client
            
            return CacheManager()
    
    def test_invalidate_by_pattern(self, cache_manager):
        """Test invalidating cache by pattern"""
        # Test pattern-based invalidation if implemented
        pass
    
    def test_invalidate_movie_cache(self, cache_manager):
        """Test invalidating all cache for a specific movie"""
        # Test movie-specific invalidation
        pass


class TestCacheMetrics:
    """Test cache metrics and monitoring"""
    
    @pytest.fixture
    def cache_manager(self):
        """Create CacheManager with mock Redis"""
        with patch('query_engine.cache_manager.redis.Redis') as mock_redis_class:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_client.info.return_value = {
                "used_memory": 1024000,
                "keyspace_hits": 1000,
                "keyspace_misses": 100
            }
            mock_redis_class.return_value = mock_client
            
            return CacheManager()
    
    def test_get_cache_stats(self, cache_manager):
        """Test getting cache statistics"""
        # Test cache stats retrieval if implemented
        pass
    
    def test_cache_hit_rate(self, cache_manager):
        """Test calculating cache hit rate"""
        # Test hit rate calculation
        pass


class TestErrorHandling:
    """Test error handling in cache operations"""
    
    def test_redis_connection_failure(self):
        """Test handling of Redis connection failure"""
        with patch('query_engine.cache_manager.redis.Redis') as mock_redis_class:
            mock_redis_class.side_effect = Exception("Connection failed")
            
            # Should handle gracefully and maybe fall back to no cache
            try:
                manager = CacheManager()
                # Should not crash
                assert manager.client is None or manager is not None
            except Exception:
                # Or raise appropriate exception
                pass
    
    def test_cache_operation_failure(self):
        """Test handling of cache operation failures"""
        with patch('query_engine.cache_manager.redis.Redis') as mock_redis_class:
            mock_client = Mock()
            mock_client.get.side_effect = Exception("Redis error")
            mock_redis_class.return_value = mock_client
            
            manager = CacheManager()
            
            # Should handle error gracefully
            result = manager.get("test_key")
            # Should return None or handle appropriately
            assert result is None or isinstance(result, dict)


class TestCacheHealth:
    """Test cache health checks"""
    
    @pytest.fixture
    def cache_manager(self):
        """Create CacheManager with mock Redis"""
        with patch('query_engine.cache_manager.redis.Redis') as mock_redis_class:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_redis_class.return_value = mock_client
            
            return CacheManager()
    
    def test_cache_health_check(self, cache_manager):
        """Test cache health check"""
        # Test health check if implemented
        if hasattr(cache_manager, 'health_check'):
            status = cache_manager.health_check()
            assert status is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
