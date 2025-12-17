"""
Trending Endpoints - Real-time trending movies
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
import logging

from mongodb.client import get_database
from query_engine.view_merger import ViewMerger
from query_engine.cache_manager import get_cache_manager
from api.metrics import record_viral_detection

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/trending",
    tags=["trending"]
)


def get_view_merger():
    """Get ViewMerger instance"""
    db = get_database()
    return ViewMerger(db)


def get_cache():
    """Get cache manager instance"""
    return get_cache_manager()


@router.get("/movies")
async def get_trending_movies(
    genre: Optional[str] = Query(None, description="Filter by genre"),
    limit: int = Query(20, ge=1, le=100, description="Number of results"),
    min_threshold: float = Query(0.0, description="Minimum viral coefficient (optional filter, default: 0.0 to show all)"),
    window: int = Query(48, ge=1, le=168, description="Time window in hours (default: 48, max: 1 week)"),
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Get currently trending movies (Business Goal #2: Viral Content Identification)
    
    Returns top N movies ranked by Reddit viral coefficient:
    - Calculates upvote/comment velocity from Reddit speed layer
    - Compares against genre-specific viral thresholds from batch layer
    - Returns top movies ranked by viral coefficient (no hard threshold)
    
    Uses Reddit viral coefficient instead of TMDB popularity:
    - Calculates upvote/comment velocity from Reddit speed layer
    - Compares against genre-specific viral thresholds from batch layer
    - Returns viral coefficient (velocity / threshold)
    
    Returns:
    - rank: Position in viral ranking
    - viral_coefficient: How many times above viral threshold
    - reddit_metrics: Upvotes, comments, sentiment from Reddit
    - viral_status: "viral" (>=1.0) or "trending" (<1.0)
    
    Args:
        genre: Optional genre filter
        limit: Maximum number of results (1-100)
        min_threshold: Optional minimum viral coefficient filter (default: 0.0)
        window: Time window in hours (default: 48, max: 168 for 1 week)
    
    Returns:
        Top N viral movies ranked by Reddit engagement coefficient
    """
    try:
        # Handle "All Genres" as a special case - treat as None (no filter)
        # Grafana sends genre="All Genres" for the all-genres option
        if genre == "All Genres" or (genre is not None and not genre.strip()):
            genre = None
        
        # Try cache first
        cache_key = f"trending:viral:{genre}:{limit}:{min_threshold}:{window}"
        cached = cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for viral trending movies")
            return cached
        
        # Update the merger's cutoff hours temporarily for this request
        original_cutoff = merger.cutoff_hours
        merger.cutoff_hours = window
        
        # Use merge_viral_data - pass min_threshold to allow optional filtering
        response = merger.merge_viral_data(
            genre=genre,
            limit=limit,
            viral_coefficient_threshold=min_threshold
        )
        
        # Restore original cutoff
        merger.cutoff_hours = original_cutoff
        
        if not response.get('viral_movies'):
            return {
                'trending_movies': [],  # Grafana compatibility
                'viral_movies': [],
                'total_trending': 0,
                'threshold_used': min_threshold,
                'window_hours': window,
                'timestamp': None,
                'message': 'No viral content found in time window'
            }
        
        # Record viral detection metrics for each movie by genre
        viral_movies = response.get('viral_movies', [])
        if viral_movies:
            # Record individual genre metrics for each viral movie
            for movie in viral_movies:
                movie_genre = movie.get('genre', 'Unknown')
                record_viral_detection(genre=movie_genre)
            
            # Also record aggregate metric if query wasn't genre-filtered
            if not genre:
                record_viral_detection(genre="all")
        
        # Cache result (short TTL - viral trends change fast)
        cache.set(cache_key, response, ttl_seconds=300)  # 5 minutes
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting viral trending movies: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

