"""
FastAPI Main Application - Movie Data Analysis Pipeline Serving Layer
Goal-Aligned API Structure (Dec 2025)
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

# Import routers
from api.routes import (
    health_router,
    crisis_detection_router,
    viral_detection_router,
    recommendations_router,
    utilities_router
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Movie Data Analysis Pipeline API",
    description="Lambda Architecture Serving Layer - Goal-Aligned Endpoints",
    version="2.0.0",
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers with /api/v1 prefix
app.include_router(health_router, prefix="/api/v1", tags=["health"])
app.include_router(crisis_detection_router, prefix="/api/v1", tags=["crisis-detection"])
app.include_router(viral_detection_router, prefix="/api/v1", tags=["viral-detection"])
app.include_router(recommendations_router, prefix="/api/v1", tags=["recommendations"])
app.include_router(utilities_router, prefix="/api/v1", tags=["utilities"])

@app.get("/")
async def root():
    """Root endpoint - API information"""
    return {
        "name": "Movie Data Analysis Pipeline API",
        "version": "2.0.0",
        "docs": "/api/v1/docs",
        "goals": [
            "Goal #1: PR Crisis Detection & Sentiment Monitoring",
            "Goal #2: Viral Content Identification",
            "Goal #3: Content Recommendation Optimization"
        ]
    }

# TODO: Add middleware for rate limiting
# TODO: Add middleware for metrics collection
# TODO: Add error handlers
# TODO: Add startup/shutdown events for DB connections

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
