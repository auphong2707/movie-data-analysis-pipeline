"""
CORS Configuration

Setup Cross-Origin Resource Sharing for API
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os
import logging

logger = logging.getLogger(__name__)


def setup_cors(app: FastAPI, allow_origins: List[str] = None):
    """
    Configure CORS middleware for FastAPI application
    
    Args:
        app: FastAPI application instance
        allow_origins: List of allowed origins (optional)
    """
    # Default allowed origins
    if allow_origins is None:
        allow_origins_env = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8000")
        allow_origins = [origin.strip() for origin in allow_origins_env.split(",")]
    
    # Check if wildcard is allowed (development only)
    if "*" in allow_origins:
        logger.warning("CORS wildcard (*) enabled - should only be used in development!")
    
    # Configure CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
        allow_headers=["*"],
        expose_headers=[
            "X-RateLimit-Limit",
            "X-RateLimit-Remaining",
            "X-RateLimit-Reset",
            "Retry-After"
        ],
        max_age=600  # Cache preflight requests for 10 minutes
    )
    
    logger.info(f"CORS configured with origins: {allow_origins}")


def get_cors_config() -> dict:
    """
    Get CORS configuration from environment
    
    Returns:
        Dictionary with CORS settings
    """
    return {
        "allow_origins": os.getenv(
            "CORS_ORIGINS",
            "http://localhost:3000,http://localhost:8000"
        ).split(","),
        "allow_credentials": os.getenv("CORS_ALLOW_CREDENTIALS", "true").lower() == "true",
        "allow_methods": os.getenv(
            "CORS_ALLOW_METHODS",
            "GET,POST,PUT,DELETE,OPTIONS,PATCH"
        ).split(","),
        "allow_headers": ["*"],
        "max_age": int(os.getenv("CORS_MAX_AGE", "600"))
    }
