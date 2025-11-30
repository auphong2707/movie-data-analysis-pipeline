"""
Recommendation Endpoints - Content-based movie recommendations

Combines:
- Content-based filtering (genres, cast, keywords)
- Real-time trending scores
- Sentiment-based re-ranking
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
import logging

from mongodb.client import get_database
from query_engine.view_merger import ViewMerger
from query_engine.recommendation_engine import RecommendationEngine
from query_engine.cache_manager import get_cache_manager

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/recommendations",
    tags=["recommendations"]
)


def get_view_merger():
    """Get ViewMerger instance"""
    db = get_database()
    return ViewMerger(db)


def get_recommendation_engine():
    """Get RecommendationEngine instance"""
    db = get_database()
    return RecommendationEngine(db)


def get_cache():
    """Get cache manager instance"""
    return get_cache_manager()


@router.get("/movies/{movie_id}/similar")
async def get_similar_movies(
    movie_id: int,
    limit: int = Query(10, ge=1, le=50, description="Number of recommendations"),
    include_trending: bool = Query(True, description="Boost trending movies"),
    include_sentiment: bool = Query(True, description="Boost high sentiment movies"),
    engine: RecommendationEngine = Depends(get_recommendation_engine),
    cache = Depends(get_cache)
):
    """
    Get similar movies based on content (genres, cast, keywords)
    
    Re-ranked by:
    - Trending score (if include_trending=True)
    - Sentiment score (if include_sentiment=True)
    
    Args:
        movie_id: Source movie ID
        limit: Number of recommendations
        include_trending: Boost trending movies
        include_sentiment: Boost high sentiment movies
    
    Returns:
        List of similar movies with similarity scores
    """
    try:
        cache_key = f"rec:similar:{movie_id}:{limit}:{include_trending}:{include_sentiment}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        recommendations = engine.get_similar_movies(
            movie_id=movie_id,
            limit=limit,
            boost_trending=include_trending,
            boost_sentiment=include_sentiment
        )
        
        if not recommendations:
            raise HTTPException(
                status_code=404,
                detail=f"Movie {movie_id} not found or no similar movies available"
            )
        
        response = {
            'source_movie_id': movie_id,
            'recommendations': recommendations,
            'total': len(recommendations),
            'ranking_factors': {
                'content_similarity': True,
                'trending_boost': include_trending,
                'sentiment_boost': include_sentiment
            }
        }
        
        cache.set(cache_key, response, ttl_seconds=600)  # 10 minutes
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting recommendations for movie {movie_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/genres/{genre}")
async def get_genre_recommendations(
    genre: str,
    limit: int = Query(20, ge=1, le=100),
    min_rating: float = Query(6.0, ge=0, le=10),
    sort_by: str = Query("hybrid", description="hybrid, trending, sentiment, rating"),
    engine: RecommendationEngine = Depends(get_recommendation_engine),
    cache = Depends(get_cache)
):
    """
    Get top movies in a genre with hybrid ranking
    
    Ranking strategies:
    - hybrid: Combines rating, trending, and sentiment
    - trending: Real-time popularity
    - sentiment: Audience perception
    - rating: Historical rating
    
    Args:
        genre: Genre name
        limit: Number of results
        min_rating: Minimum rating threshold
        sort_by: Ranking strategy
    
    Returns:
        Top movies in genre with scores
    """
    try:
        cache_key = f"rec:genre:{genre}:{limit}:{min_rating}:{sort_by}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        recommendations = engine.get_genre_recommendations(
            genre=genre,
            limit=limit,
            min_rating=min_rating,
            sort_by=sort_by
        )
        
        response = {
            'genre': genre,
            'sort_by': sort_by,
            'min_rating': min_rating,
            'recommendations': recommendations,
            'total': len(recommendations)
        }
        
        cache.set(cache_key, response, ttl_seconds=900)  # 15 minutes
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting genre recommendations: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/personalized")
async def get_personalized_recommendations(
    liked_movie_ids: List[int] = Query(..., description="Movies user liked"),
    limit: int = Query(20, ge=1, le=100),
    engine: RecommendationEngine = Depends(get_recommendation_engine)
):
    """
    Get personalized recommendations based on liked movies
    
    Combines:
    - Content similarity to liked movies
    - Trending boost
    - Sentiment filtering
    
    Args:
        liked_movie_ids: List of movie IDs user liked
        limit: Number of recommendations
    
    Returns:
        Personalized movie recommendations
    """
    try:
        if not liked_movie_ids:
            raise HTTPException(
                status_code=400,
                detail="At least one liked movie ID required"
            )
        
        recommendations = engine.get_personalized_recommendations(
            liked_movie_ids=liked_movie_ids,
            limit=limit
        )
        
        return {
            'based_on': liked_movie_ids,
            'recommendations': recommendations,
            'total': len(recommendations)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting personalized recommendations: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
