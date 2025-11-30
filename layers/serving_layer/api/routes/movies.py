"""
Movie Endpoints - Movie details and sentiment analysis
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from datetime import datetime, timedelta
import logging

from mongodb.client import get_database
from query_engine.view_merger import ViewMerger
from query_engine.cache_manager import get_cache_manager, cache_response

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/movies",
    tags=["movies"]
)


# Dependency injection
def get_view_merger():
    """Get ViewMerger instance"""
    db = get_database()
    return ViewMerger(db)


def get_cache():
    """Get cache manager instance"""
    return get_cache_manager()


@router.get("/{movie_id}")
async def get_movie(
    movie_id: int,
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Get detailed movie information
    
    Returns:
    - Complete movie metadata from batch layer
    - Current stats from speed layer (if available)
    - Data source indicator
    
    Args:
        movie_id: TMDB movie ID
    
    Returns:
        Complete movie information per README schema
    """
    try:
        # Try cache first
        cache_key = f"movie:{movie_id}"
        cached = cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for movie {movie_id}")
            return cached
        
        # Get merged view
        result = merger.merge_movie_views(movie_id)
        
        if not result.get('found'):
            raise HTTPException(
                status_code=404,
                detail=f"Movie with ID {movie_id} not found"
            )
        
        # Format response per README schema
        response = {
            'movie_id': result['movie_id'],
            'title': result.get('title'),
            'release_date': result.get('release_date'),
            'genres': result.get('genres', []),
            'vote_average': result.get('vote_average'),
            'vote_count': result.get('vote_count'),
            'popularity': result.get('popularity'),
            'runtime': result.get('runtime'),
            'budget': result.get('budget'),
            'revenue': result.get('revenue'),
            'overview': result.get('overview'),
            'original_language': result.get('original_language'),
            'data_source': result.get('data_source'),
            'last_updated': result.get('last_updated')
        }
        
        # Cache result (5 minutes for speed data, 1 hour for batch data)
        ttl = 300 if result.get('data_source') == 'speed' else 3600
        cache.set(cache_key, response, ttl_seconds=ttl)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting movie {movie_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{movie_id}/sentiment")
async def get_movie_sentiment(
    movie_id: int,
    window: Optional[str] = Query(None, description="Time window: 7d, 30d, all"),
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Get sentiment analysis for a movie
    
    Returns per README schema:
    - Overall sentiment score and label
    - Positive/negative/neutral counts
    - Daily sentiment breakdown
    - Sentiment velocity (change rate)
    
    Args:
        movie_id: TMDB movie ID
        window: Time window (7d, 30d, all)
    
    Returns:
        Comprehensive sentiment analysis
    """
    try:
        # Try cache first
        cache_key = f"sentiment:{movie_id}:{window}"
        cached = cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for sentiment {movie_id}")
            return cached
        
        # Get merged sentiment
        result = merger.merge_sentiment_views(movie_id, window)
        
        if not result.get('sentiment'):
            raise HTTPException(
                status_code=404,
                detail=f"Sentiment data for movie {movie_id} not found"
            )
        
        # Format response per README
        response = {
            'movie_id': result['movie_id'],
            'title': result.get('title'),
            'sentiment': result['sentiment'],
            'breakdown': result.get('breakdown', []),
            'data_sources': result.get('data_sources', {})
        }
        
        # Cache result (3 minutes - sentiment is dynamic)
        cache.set(cache_key, response, ttl_seconds=180)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting sentiment for movie {movie_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{movie_id}/stats")
async def get_movie_stats(
    movie_id: int,
    hours: int = Query(48, description="Hours of historical stats"),
    merger: ViewMerger = Depends(get_view_merger)
):
    """
    Get real-time statistics for a movie
    
    Provides recent statistics from speed layer:
    - Vote average and count
    - Popularity score
    - Rating velocity
    
    Args:
        movie_id: TMDB movie ID
        hours: Number of hours to look back
    
    Returns:
        Recent movie statistics
    """
    try:
        from mongodb.queries import MovieQueries
        from mongodb.client import get_database
        
        db = get_database()
        queries = MovieQueries(db)
        
        # Get speed layer stats
        stats = queries.get_speed_movie_stats(movie_id, hours_back=hours)
        
        if not stats:
            raise HTTPException(
                status_code=404,
                detail=f"No recent stats found for movie {movie_id}"
            )
        
        return {
            'movie_id': movie_id,
            'hours_back': hours,
            'stats': [
                {
                    'hour': stat['hour'],
                    'vote_average': stat['stats']['vote_average'],
                    'vote_count': stat['stats']['vote_count'],
                    'popularity': stat['stats']['popularity'],
                    'rating_velocity': stat['stats'].get('rating_velocity', 0)
                }
                for stat in stats
            ],
            'latest': stats[0]['stats'] if stats else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stats for movie {movie_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


def _parse_time_window(window: str) -> dict:
    """
    Parse time window string to datetime range
    
    Args:
        window: Window string (7d, 30d, all)
    
    Returns:
        Dictionary with start and end datetimes
    """
    from datetime import timedelta
    
    now = datetime.utcnow()
    
    if window == '7d':
        return {
            'start': now - timedelta(days=7),
            'end': now
        }
    elif window == '30d':
        return {
            'start': now - timedelta(days=30),
            'end': now
        }
    elif window == 'all':
        return None
    else:
        # Default to 30 days
        return {
            'start': now - timedelta(days=30),
            'end': now
        }
