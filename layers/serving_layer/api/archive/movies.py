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
from api.metrics import record_crisis_alert, update_sentiment_metrics

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
        
        # Check for PR crisis (sentiment drop)
        sentiment_data = result['sentiment']
        overall_score = sentiment_data.get('overall_score', 0)
        sentiment_label = sentiment_data.get('label', 'neutral')
        
        # Record crisis alert if sentiment is negative or very low
        if sentiment_label == 'negative' or overall_score < 0.3:
            record_crisis_alert(severity="warning")
        elif sentiment_label == 'very_negative' or overall_score < 0.1:
            record_crisis_alert(severity="critical")
        
        # Format response per README
        response = {
            'movie_id': result['movie_id'],
            'title': result.get('title'),
            'genres': result.get('genres', []),
            'sentiment': result['sentiment'],
            'breakdown': result.get('breakdown', []),
            'data_sources': result.get('data_sources', {})
        }
        
        # Update Prometheus sentiment metrics for dashboard
        update_sentiment_metrics(response)
        
        # Cache result (3 minutes - sentiment is dynamic)
        cache.set(cache_key, response, ttl_seconds=180)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting sentiment for movie {movie_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/by-title/{title}")
async def get_movie_by_title(
    title: str,
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Get detailed movie information by title
    
    This endpoint accepts movie titles and internally looks up the movie_id.
    Useful when you know the movie title but not the TMDB ID.
    
    Args:
        title: Movie title (case-insensitive, e.g., "The Flash")
    
    Returns:
        Complete movie information per README schema
    """
    try:
        from mongodb.queries import MovieQueries
        from mongodb.client import get_database
        
        db = get_database()
        queries = MovieQueries(db)
        
        # Look up movie_id by title
        movie_id = queries.get_movie_id_by_title(title)
        
        if not movie_id:
            raise HTTPException(
                status_code=404,
                detail=f"Movie with title '{title}' not found. Try using the search endpoint to find the exact title."
            )
        
        # Use the existing get_movie logic
        cache_key = f"movie:{movie_id}"
        cached = cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for movie {movie_id} (title: {title})")
            return cached
        
        result = merger.merge_movie_views(movie_id)
        
        if not result.get('found'):
            raise HTTPException(
                status_code=404,
                detail=f"Movie with title '{title}' (ID: {movie_id}) not found"
            )
        
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
        
        ttl = 300 if result.get('data_source') == 'speed' else 3600
        cache.set(cache_key, response, ttl_seconds=ttl)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting movie by title '{title}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/by-title/{title}/sentiment")
async def get_movie_sentiment_by_title(
    title: str,
    window: Optional[str] = Query(None, description="Time window: 7d, 30d, all"),
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Get sentiment analysis for a movie by title
    
    This endpoint accepts movie titles and internally looks up the movie_id.
    Useful when you know the movie title but not the TMDB ID.
    
    Args:
        title: Movie title (case-insensitive, e.g., "The Flash")
        window: Time window (7d, 30d, all)
    
    Returns:
        Comprehensive sentiment analysis
    """
    try:
        from mongodb.queries import MovieQueries
        from mongodb.client import get_database
        
        db = get_database()
        queries = MovieQueries(db)
        
        # Look up movie_id by title
        movie_id = queries.get_movie_id_by_title(title)
        
        if not movie_id:
            raise HTTPException(
                status_code=404,
                detail=f"Movie with title '{title}' not found. Try using the search endpoint to find the exact title."
            )
        
        # Use the existing sentiment logic
        cache_key = f"sentiment:{movie_id}:{window}"
        cached = cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for sentiment {movie_id} (title: {title})")
            return cached
        
        result = merger.merge_sentiment_views(movie_id, window)
        
        if not result.get('sentiment'):
            raise HTTPException(
                status_code=404,
                detail=f"Sentiment data for movie '{title}' (ID: {movie_id}) not found"
            )
        
        # Check for PR crisis (sentiment drop)
        sentiment_data = result['sentiment']
        overall_score = sentiment_data.get('overall_score', 0)
        sentiment_label = sentiment_data.get('label', 'neutral')
        
        # Record crisis alert if sentiment is negative or very low
        if sentiment_label == 'negative' or overall_score < 0.3:
            record_crisis_alert(severity="warning")
        elif sentiment_label == 'very_negative' or overall_score < 0.1:
            record_crisis_alert(severity="critical")
        
        response = {
            'movie_id': result['movie_id'],
            'title': result.get('title'),
            'genres': result.get('genres', []),
            'sentiment': result['sentiment'],
            'breakdown': result.get('breakdown', []),
            'data_sources': result.get('data_sources', {})
        }
        
        # Update Prometheus sentiment metrics for dashboard
        update_sentiment_metrics(response)
        
        cache.set(cache_key, response, ttl_seconds=180)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting sentiment by title '{title}': {e}", exc_info=True)
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
