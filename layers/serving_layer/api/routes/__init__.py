"""
API routes module
"""

from .movies import router as movies_router
from .trending import router as trending_router
from .analytics import router as analytics_router
from .search import router as search_router
from .health import router as health_router
from .recommendations import router as recommendations_router
from .predictions import router as predictions_router

__all__ = [
    'movies_router',
    'trending_router',
    'analytics_router',
    'search_router',
    'health_router',
    'recommendations_router',
    'predictions_router'
]
