"""
Trending Endpoints - Real-time trending movies
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
import logging

from mongodb.client import get_database
from query_engine.view_merger import ViewMerger
from query_engine.cache_manager import get_cache_manager

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
    window: int = Query(6, ge=1, le=48, description="Time window in hours"),
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Get currently trending movies
    
    Returns per README schema with:
    - rank: Position in trending list
    - trending_score: Composite score
    - velocity: Popularity increase rate
    - acceleration: Rating velocity
    
    Args:
        genre: Optional genre filter
        limit: Maximum number of results (1-100)
        window: Time window in hours (1-48)
    
    Returns:
        Trending movies list per README format
    """
    try:
        # Try cache first (skip cache if genre filter is used)
        cache_key = f"trending:{genre}:{limit}:{window}"
        cached = None if genre else cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for trending movies")
            return cached
        
        # Get trending movies (already formatted by ViewMerger)
        response = merger.merge_trending_views(
            genre=genre,
            limit=limit,
            window_hours=window
        )
        
        if not response.get('trending_movies'):
            return {
                'trending_movies': [],
                'generated_at': None,
                'window': f"{window}h",
                'data_source': 'speed'
            }
        
        # Cache result (short TTL - trending changes fast)
        cache.set(cache_key, response, ttl_seconds=300)  # 5 minutes
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting trending movies: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/genres/{genre}")
async def get_trending_by_genre(
    genre: str,
    limit: int = Query(10, ge=1, le=50),
    window: int = Query(6, ge=1, le=48),
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Get trending movies for a specific genre
    
    Args:
        genre: Genre name (e.g., Action, Drama, Comedy)
        limit: Maximum number of results
        window: Time window in hours
    
    Returns:
        List of trending movies in the genre
    """
    try:
        # Try cache first
        cache_key = f"trending:genre:{genre}:{limit}:{window}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        # Get trending for genre
        trending_data = merger.merge_trending_views(
            genre=genre,
            limit=limit,
            window_hours=window
        )
        
        response = {
            'genre': genre,
            'window': f"{window}h",
            'trending_movies': [
                {
                    'rank': idx + 1,
                    'movie_id': movie['movie_id'],
                    'title': movie.get('title', 'Unknown'),
                    'trending_score': round(movie['trending_score'], 2),
                    'popularity': round(movie['popularity'], 2),
                    'vote_average': round(movie['vote_average'], 2)
                }
                for idx, movie in enumerate(trending_data.get('trending_movies', []))
            ],
            'count': len(trending_data.get('trending_movies', []))
        }
        
        # Cache result
        cache.set(cache_key, response, ttl_seconds=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting trending for genre {genre}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
