"""
Health Check Endpoints - System health and status monitoring
"""

from fastapi import APIRouter, Depends
from datetime import datetime
import logging

from mongodb.client import check_mongodb_health, get_database
from query_engine.cache_manager import get_cache_manager
from query_engine.query_router import QueryRouter

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/health",
    tags=["health"]
)


@router.get("")
async def health_check():
    """
    Comprehensive health check
    
    Checks:
    - MongoDB connection and latency
    - Redis connection and stats
    - Batch layer data freshness
    - Speed layer data freshness
    
    Returns:
        Health status for all components
    """
    try:
        # Check MongoDB
        mongodb_health = check_mongodb_health()
        
        # Check Redis
        cache = get_cache_manager()
        redis_stats = cache.get_stats() if cache.client else {'status': 'down'}
        
        # Check data freshness
        db = get_database()
        router = QueryRouter(db)
        routing_stats = router.get_routing_stats()
        
        # Get latest updates
        batch_latest = db.batch_views.find_one(
            {},
            sort=[('computed_at', -1)]
        )
        
        speed_latest = db.speed_views.find_one(
            {},
            sort=[('hour', -1)]
        )
        
        # Calculate overall status
        overall_status = 'healthy'
        if mongodb_health['status'] != 'up' or redis_stats['status'] != 'connected':
            overall_status = 'degraded'
        
        return {
            'status': overall_status,
            'timestamp': datetime.utcnow().isoformat(),
            'services': {
                'mongodb': {
                    'status': mongodb_health['status'],
                    'latency_ms': mongodb_health.get('latency_ms'),
                    'connected': mongodb_health['connected']
                },
                'redis': {
                    'status': redis_stats['status'],
                    'total_keys': redis_stats.get('total_keys', 0),
                    'hit_rate': redis_stats.get('hit_rate', 0)
                },
                'batch_layer': {
                    'status': 'up' if batch_latest else 'no_data',
                    'last_update': batch_latest.get('computed_at') if batch_latest else None,
                    'document_count': routing_stats['batch_layer']['total_documents']
                },
                'speed_layer': {
                    'status': 'up' if speed_latest else 'no_data',
                    'last_update': speed_latest.get('hour') if speed_latest else None,
                    'document_count': routing_stats['speed_layer']['total_documents'],
                    'recent_documents': routing_stats['speed_layer']['recent_documents']
                }
            },
            'data_freshness': {
                'cutoff_hours': routing_stats['cutoff_hours'],
                'cutoff_time': routing_stats['cutoff_time']
            },
            'version': '1.0.0'
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            'status': 'error',
            'timestamp': datetime.utcnow().isoformat(),
            'error': str(e),
            'version': '1.0.0'
        }


@router.get("/mongodb")
async def mongodb_health():
    """
    Detailed MongoDB health check
    
    Returns:
        MongoDB connection status and metrics
    """
    try:
        health = check_mongodb_health()
        
        # Get collection stats
        db = get_database()
        batch_count = db.batch_views.count_documents({})
        speed_count = db.speed_views.count_documents({})
        
        return {
            'status': health['status'],
            'latency_ms': health.get('latency_ms'),
            'version': health.get('version'),
            'collections': {
                'batch_views': {
                    'count': batch_count
                },
                'speed_views': {
                    'count': speed_count
                }
            }
        }
        
    except Exception as e:
        logger.error(f"MongoDB health check failed: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


@router.get("/cache")
async def cache_health():
    """
    Detailed Redis cache health check
    
    Returns:
        Cache status, hit rate, and statistics
    """
    try:
        cache = get_cache_manager()
        stats = cache.get_stats()
        
        return {
            'status': stats['status'],
            'statistics': stats
        }
        
    except Exception as e:
        logger.error(f"Cache health check failed: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


@router.get("/routing")
async def routing_health():
    """
    Check data routing and layer status
    
    Returns:
        Routing statistics and layer coverage
    """
    try:
        db = get_database()
        router = QueryRouter(db)
        stats = router.get_routing_stats()
        
        return {
            'status': 'healthy',
            'routing_stats': stats
        }
        
    except Exception as e:
        logger.error(f"Routing health check failed: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }
