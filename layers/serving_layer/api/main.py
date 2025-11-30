"""
Serving Layer - FastAPI Main Application

Unified query interface that merges batch and speed layer views.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Import routes
from .routes import (
    movies_router,
    trending_router,
    analytics_router,
    search_router,
    health_router
)
from api.routes.recommendations import router as recommendations_router
from api.routes.predictions import router as predictions_router

# Import clients for lifecycle management
from mongodb.client import get_mongodb_client, close_mongodb_client
from query_engine.cache_manager import get_cache_manager, close_cache_manager

# Import middleware
from api.middleware.cors import setup_cors
from api.middleware.rate_limit import get_rate_limiter


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager
    
    Handles startup and shutdown events:
    - Startup: Initialize MongoDB and Redis connections
    - Shutdown: Close all connections gracefully
    """
    # Startup
    logger.info("Starting Movie Analytics API...")
    
    try:
        # Initialize MongoDB connection
        mongo_client = get_mongodb_client()
        logger.info("MongoDB connection established")
        
        # Initialize Redis cache
        cache = get_cache_manager()
        if cache.client:
            logger.info("Redis cache connection established")
        else:
            logger.warning("Redis cache unavailable - running without cache")
        
        logger.info("API startup complete")
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down Movie Analytics API...")
    
    try:
        # Close MongoDB connection
        close_mongodb_client()
        logger.info("MongoDB connection closed")
        
        # Close Redis cache
        close_cache_manager()
        logger.info("Redis cache connection closed")
        
        logger.info("API shutdown complete")
        
    except Exception as e:
        logger.error(f"Shutdown error: {e}")


# Create FastAPI application
app = FastAPI(
    title="Movie Analytics API",
    description="""
    ## Lambda Architecture Serving Layer
    
    Unified query interface for movie analytics that merges:
    - **Batch Layer**: Historical data (>48 hours old) for accuracy
    - **Speed Layer**: Recent data (≤48 hours) for freshness
    
    ### Features
    - **Movie Details**: Get complete movie information from batch and speed layers
    - **Sentiment Analysis**: Real-time and historical sentiment tracking
    - **Trending Movies**: Discover what's hot right now
    - **Analytics**: Genre analytics and temporal trends
    - **Search**: Flexible movie search with multiple filters
    
    ### Data Sources
    - **Batch Layer**: Historical data (>48 hours old) for accuracy
    - **Speed Layer**: Recent data (≤48 hours) for freshness
    - **Merged Views**: Best of both worlds at query time
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Setup CORS middleware
setup_cors(app)

# Initialize rate limiter (will be used in routes if enabled)
rate_limiter = get_rate_limiter()

# Exception handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if app.debug else "An unexpected error occurred"
        }
    )

# Include routers
app.include_router(health_router, prefix="/api/v1")
app.include_router(movies_router, prefix="/api/v1")
app.include_router(trending_router, prefix="/api/v1")
app.include_router(analytics_router, prefix="/api/v1")
app.include_router(search_router, prefix="/api/v1")
app.include_router(recommendations_router, prefix="/api/v1")
app.include_router(predictions_router, prefix="/api/v1")

# Root endpoint
@app.get("/")
async def root():
    """
    API root endpoint
    
    Returns:
        Welcome message and API information
    """
    return {
        "message": "Movie Analytics API - Lambda Architecture Serving Layer",
        "version": "1.0.0",
        "documentation": "/docs",
        "health": "/api/v1/health",
        "endpoints": {
            "movies": "/api/v1/movies",
            "trending": "/api/v1/trending",
            "analytics": "/api/v1/analytics",
            "search": "/api/v1/search",
            "recommendations": "/api/v1/recommendations",
            "predictions": "/api/v1/predictions"
        }
    }

# For local development
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
