"""
Analytics Endpoints - Historical analytics and trends
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from datetime import datetime, timedelta
import logging

from mongodb.client import get_database
from mongodb.queries import MovieQueries
from query_engine.view_merger import ViewMerger
from query_engine.cache_manager import get_cache_manager

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/analytics",
    tags=["analytics"]
)


def get_view_merger():
    """Get ViewMerger instance"""
    db = get_database()
    return ViewMerger(db)


def get_movie_queries():
    """Get MovieQueries instance"""
    db = get_database()
    return MovieQueries(db)


def get_cache():
    """Get cache manager instance"""
    return get_cache_manager()


@router.get("/genre/{genre}")
async def get_genre_analytics(
    genre: str,
    year: Optional[int] = Query(None, description="Filter by year"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Filter by month"),
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Get analytics for a specific genre
    
    Returns per README schema:
    - statistics: total_movies, avg_rating, avg_sentiment, avg_popularity, revenue
    - top_movies: List of top movies in genre
    - trends: rating/sentiment/popularity trend directions
    
    Args:
        genre: Genre name (e.g., Action, Drama, Comedy)
        year: Optional year filter
        month: Optional month filter (1-12)
    
    Returns:
        Genre analytics per README format
    """
    try:
        # Try cache first
        cache_key = f"analytics:genre:{genre}:{year}:{month}"
        cached = cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for genre analytics {genre}")
            return cached
        
        # Get genre analytics (already formatted by ViewMerger)
        response = merger.merge_analytics_views(
            genre=genre,
            year=year,
            month=month
        )
        
        if not response.get('found', True):
            raise HTTPException(
                status_code=404,
                detail=f"No analytics found for genre {genre}"
            )
        
        # Cache result (30 minutes - analytics are updated by batch layer)
        cache.set(cache_key, response, ttl_seconds=1800)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting analytics for genre {genre}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/trends")
async def get_trends(
    movie_id: Optional[int] = Query(None, description="Specific movie ID"),
    genre: Optional[str] = Query(None, description="Specific genre"),
    metric: str = Query("rating", description="Metric to analyze: rating, sentiment, popularity"),
    window: str = Query("30d", description="Time window: 7d, 30d, 90d"),
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Get time-series trend analysis
    
    Returns per README schema:
    - data_points: Array of {date, value, count}
    - summary: {avg, min, max, trend, change_rate}
    
    Supports filtering by:
    - movie_id: Trends for specific movie
    - genre: Trends for specific genre
    - Or overall trends if no filter
    
    Args:
        movie_id: Optional movie ID filter
        genre: Optional genre filter
        metric: Metric to analyze (rating, sentiment, popularity)
        window: Time window (7d, 30d, 90d)
    
    Returns:
        Time-series data per README format
    """
    try:
        # Validate metric
        if metric not in ['rating', 'sentiment', 'popularity']:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid metric: {metric}. Must be rating, sentiment, or popularity"
            )
        
        # Try cache first
        cache_key = f"trends:{movie_id}:{genre}:{metric}:{window}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        # Get temporal trends (already formatted by ViewMerger)
        response = merger.get_temporal_trends(
            metric=metric,
            movie_id=movie_id,
            genre=genre,
            window=window
        )
        
        if not response.get('data_points'):
            return {
                'metric': metric,
                'window': window,
                'movie_id': movie_id,
                'genre': genre,
                'data_points': [],
                'summary': {
                    'avg': 0,
                    'min': 0,
                    'max': 0,
                    'trend': 'no_data',
                    'change_rate': 0
                }
            }
        
        # Cache result (1 hour - trends from batch layer)
        cache.set(cache_key, response, ttl_seconds=3600)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting trends: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/overview")
async def get_analytics_overview(
    queries: MovieQueries = Depends(get_movie_queries),
    cache = Depends(get_cache)
):
    """
    Get overall analytics overview
    
    Provides high-level statistics across all movies:
    - Total movies count
    - Genre distribution
    - Average metrics
    - Recent activity
    
    Returns:
        Analytics overview
    """
    try:
        # Try cache first
        cache_key = "analytics:overview"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        # Get all genre analytics
        all_genres = queries.get_batch_genre_analytics()
        
        if not all_genres:
            return {
                'total_movies': 0,
                'genres': [],
                'overall_stats': {}
            }
        
        # Aggregate statistics
        total_movies = sum(g.get('total_movies', 0) for g in all_genres)
        genres = list(set(g.get('genre') for g in all_genres if g.get('genre')))
        
        avg_rating = sum(
            g.get('avg_rating', 0) * g.get('total_movies', 0) 
            for g in all_genres
        ) / total_movies if total_movies > 0 else 0
        
        response = {
            'total_movies': total_movies,
            'total_genres': len(genres),
            'genres': genres,
            'overall_stats': {
                'avg_rating': round(avg_rating, 2),
                'avg_popularity': round(
                    sum(g.get('avg_popularity', 0) for g in all_genres) / len(all_genres), 2
                ) if all_genres else 0
            },
            'by_genre': [
                {
                    'genre': g.get('genre'),
                    'movie_count': g.get('total_movies', 0),
                    'avg_rating': round(g.get('avg_rating', 0), 2)
                }
                for g in sorted(all_genres, key=lambda x: x.get('total_movies', 0), reverse=True)[:10]
            ]
        }
        
        # Cache result (long TTL - overview changes slowly)
        cache.set(cache_key, response, ttl_seconds=3600)  # 1 hour
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting analytics overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sentiment/comparison")
async def compare_sentiment_by_tier(
    genre: Optional[str] = Query(None, description="Filter by genre"),
    year: Optional[int] = Query(None, description="Filter by year"),
    merger: ViewMerger = Depends(get_view_merger),
    cache = Depends(get_cache)
):
    """
    Compare sentiment across popularity tiers (blockbuster vs niche)
    
    Tiers:
    - Blockbuster: popularity > 80
    - Popular: 50 < popularity <= 80
    - Moderate: 20 < popularity <= 50
    - Niche: popularity <= 20
    
    Args:
        genre: Optional genre filter
        year: Optional year filter
    
    Returns:
        Sentiment comparison across tiers
    """
    try:
        cache_key = f"analytics:sentiment:tiers:{genre}:{year}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        from mongodb.queries import MovieQueries
        db = get_database()
        queries = MovieQueries(db)
        
        # Get movies with sentiment data
        query = {'view_type': 'movie_details'}
        if genre:
            query['data.genres'] = genre
        if year:
            query['data.release_date'] = {'$regex': f'^{year}'}
        
        movies = list(db.batch_views.find(query))
        
        # Categorize by popularity tier
        tiers = {
            'blockbuster': [],
            'popular': [],
            'moderate': [],
            'niche': []
        }
        
        for movie in movies:
            if 'data' not in movie:
                continue
            
            data = movie['data']
            popularity = data.get('popularity', 0)
            movie_id = movie.get('movie_id')
            
            # Get sentiment
            sentiment_doc = db.batch_views.find_one({
                'movie_id': movie_id,
                'view_type': 'sentiment'
            })
            
            if not sentiment_doc or 'data' not in sentiment_doc:
                continue
            
            # Schema: col1=avg_sentiment
            sentiment = sentiment_doc['data'].get('col1', 0) if isinstance(sentiment_doc['data'].get('col1'), (int, float)) else 0
            
            if popularity > 80:
                tiers['blockbuster'].append(sentiment)
            elif popularity > 50:
                tiers['popular'].append(sentiment)
            elif popularity > 20:
                tiers['moderate'].append(sentiment)
            else:
                tiers['niche'].append(sentiment)
        
        # Calculate averages
        response = {
            'genre': genre,
            'year': year,
            'tiers': {
                'blockbuster': {
                    'count': len(tiers['blockbuster']),
                    'avg_sentiment': round(sum(tiers['blockbuster']) / len(tiers['blockbuster']), 3) if tiers['blockbuster'] else 0,
                    'popularity_range': '> 80'
                },
                'popular': {
                    'count': len(tiers['popular']),
                    'avg_sentiment': round(sum(tiers['popular']) / len(tiers['popular']), 3) if tiers['popular'] else 0,
                    'popularity_range': '50-80'
                },
                'moderate': {
                    'count': len(tiers['moderate']),
                    'avg_sentiment': round(sum(tiers['moderate']) / len(tiers['moderate']), 3) if tiers['moderate'] else 0,
                    'popularity_range': '20-50'
                },
                'niche': {
                    'count': len(tiers['niche']),
                    'avg_sentiment': round(sum(tiers['niche']) / len(tiers['niche']), 3) if tiers['niche'] else 0,
                    'popularity_range': '<= 20'
                }
            }
        }
        
        cache.set(cache_key, response, ttl_seconds=1800)  # 30 minutes
        
        return response
        
    except Exception as e:
        logger.error(f"Error comparing sentiment by tier: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


def _parse_window_to_days(window: str) -> int:
    """Parse window string to number of days"""
    if window == '7d':
        return 7
    elif window == '30d':
        return 30
    elif window == '90d':
        return 90
    else:
        return 30  # Default


def _calculate_trend_direction(values: list) -> str:
    """Calculate trend direction from values"""
    if not values or len(values) < 2:
        return 'stable'
    
    # Simple linear trend
    first_half = sum(values[:len(values)//2]) / (len(values)//2)
    second_half = sum(values[len(values)//2:]) / (len(values) - len(values)//2)
    
    diff = second_half - first_half
    
    if diff > 0.1:
        return 'increasing'
    elif diff < -0.1:
        return 'decreasing'
    else:
        return 'stable'
