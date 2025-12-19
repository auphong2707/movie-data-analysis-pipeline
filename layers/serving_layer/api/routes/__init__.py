"""
API Routes Package - Goal-Aligned Endpoint Organization
"""
from api.routes.health import router as health_router
from api.routes.crisis_detection import router as crisis_detection_router
from api.routes.viral_detection import router as viral_detection_router
from api.routes.recommendations import router as recommendations_router
from api.routes.utilities import router as utilities_router

__all__ = [
    "health_router",
    "crisis_detection_router",
    "viral_detection_router",
    "recommendations_router",
    "utilities_router",
]
