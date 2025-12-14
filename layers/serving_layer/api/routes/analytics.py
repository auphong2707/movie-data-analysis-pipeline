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
    queries: MovieQueries = Depends(get_movie_queries),
    cache = Depends(get_cache)
):
    """
    Get analytics for a specific genre (aligned with actual batch_views schema)
    
    Returns:
    - sentiment_baseline: Genre sentiment baseline from TMDB historical data
    - viral_threshold: Genre-specific viral engagement thresholds
    - movie_count: Number of movies in genre
    - top_movies: Top-rated movies in genre
    
    Args:
        genre: Genre name (e.g., Action, Drama, Comedy)
        year: Optional year filter (applied to movie search)
        month: Optional month filter (not used for baselines)
    
    Returns:
        Genre analytics with sentiment baselines and viral thresholds
    """
    try:
        # Try cache first
        cache_key = f"analytics:genre:{genre}:{year}:{month}"
        cached = cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit for genre analytics {genre}")
            return cached
        
        db = get_database()
        
        # Query sentiment baseline from batch_views
        sentiment_baseline = db.batch_views.find_one({
            "view_type": "sentiment_baseline",
            "genre": genre
        })
        
        # Query viral threshold from batch_views
        viral_threshold = db.batch_views.find_one({
            "view_type": "viral_threshold",
            "genre": genre
        })
        
        # Get movie intelligence for this genre
        match_criteria = {
            "view_type": "movie_intelligence",
            "data.genres": genre
        }
        
        if year:
            match_criteria["data.release_date"] = {"$regex": f"^{year}"}
        
        movies = list(db.batch_views.find(match_criteria).limit(100))
        
        # Calculate statistics
        if movies:
            ratings = [m.get("data", {}).get("vote_average", 0) for m in movies if "data" in m]
            avg_rating = sum(ratings) / len(ratings) if ratings else 0
            
            # Get top movies
            top_movies = sorted(
                [m for m in movies if "data" in m],
                key=lambda x: x.get("data", {}).get("vote_average", 0),
                reverse=True
            )[:10]
            
            top_movies_list = [
                {
                    "movie_id": m.get("movie_id"),
                    "title": m.get("data", {}).get("title"),
                    "rating": round(m.get("data", {}).get("vote_average", 0), 2),
                    "vote_count": m.get("data", {}).get("vote_count", 0),
                    "release_date": m.get("data", {}).get("release_date")
                }
                for m in top_movies
            ]
        else:
            avg_rating = 0
            top_movies_list = []
        
        response = {
            "genre": genre,
            "year_filter": year,
            "month_filter": month,
            "sentiment_baseline": {
                "avg_sentiment": round(sentiment_baseline.get("avg_sentiment", 0), 3) if sentiment_baseline else None,
                "sentiment_stddev": round(sentiment_baseline.get("sentiment_stddev", 0), 3) if sentiment_baseline else None,
                "sample_size": sentiment_baseline.get("sample_size", 0) if sentiment_baseline else 0,
                "positive_ratio": round(sentiment_baseline.get("positive_ratio", 0), 3) if sentiment_baseline else None
            } if sentiment_baseline else None,
            "viral_threshold": {
                "vote_velocity_p99": viral_threshold.get("vote_velocity_p99", 0) if viral_threshold else None,
                "comment_velocity_p99": viral_threshold.get("comment_velocity_p99", 0) if viral_threshold else None,
                "engagement_velocity_p99": viral_threshold.get("engagement_velocity_p99", 0) if viral_threshold else None,
                "budget_tier": viral_threshold.get("budget_tier") if viral_threshold else None,
                "season": viral_threshold.get("season") if viral_threshold else None
            } if viral_threshold else None,
            "statistics": {
                "movie_count": len(movies),
                "avg_rating": round(avg_rating, 2)
            },
            "top_movies": top_movies_list
        }
        
        # Cache result (30 minutes - analytics are updated by batch layer)
        cache.set(cache_key, response, ttl_seconds=1800)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting analytics for genre {genre}: {e}", exc_info=True)
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
