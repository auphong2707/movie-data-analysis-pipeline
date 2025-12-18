"""
Recommendation Routes - Goal #3: Content Recommendation Optimization
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging
import math

from api.schemas.recommendations import (
    DualSuccessResponse,
    DualSuccessRecommendation,
    SimilarMoviesResponse,
    RedditBuzzResponse,
    TMDBQualityResponse
)
from mongodb.client import get_mongodb_client, get_database
from mongodb.queries import MovieQueries

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/recommendations",
    tags=["recommendations"]
)


def get_movie_queries() -> MovieQueries:
    """Dependency to get MovieQueries instance"""
    db = get_database()
    return MovieQueries(db)


def calculate_recency_weight(age_hours: float) -> float:
    """
    Calculate recency weight based on age in hours
    
    Args:
        age_hours: Age of the discussion in hours
    
    Returns:
        Recency weight (0.2 to 1.0)
    """
    if age_hours <= 24:
        return 1.0
    elif age_hours <= 48:
        return 0.8
    elif age_hours <= 168:  # 7 days
        return 0.6
    elif age_hours <= 720:  # 30 days
        return 0.4
    else:
        return 0.2


def normalize_scores(values: List[float]) -> List[float]:
    """
    Normalize a list of values to 0-100 scale
    
    Args:
        values: List of raw score values
    
    Returns:
        List of normalized scores (0-100)
    """
    if not values or len(values) == 0:
        return []
    
    min_val = min(values)
    max_val = max(values)
    
    # Handle edge case where all values are the same
    if max_val == min_val:
        return [50.0] * len(values)
    
    return [(v - min_val) / (max_val - min_val) * 100 for v in values]


@router.get("/dual-success", response_model=DualSuccessResponse)
async def get_dual_success_recommendations(
    genre: Optional[str] = Query(None, description="Filter by genre"),
    min_rating: float = Query(6.0, ge=0, le=10, description="Minimum TMDB rating"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    queries: MovieQueries = Depends(get_movie_queries)
):
    """
    Get dual-success recommendations (60% Reddit buzz + 40% TMDB quality)
    
    Formula calibrated on 3,657 movies from production database (Dec 18, 2025).
    See API_DASHBOARD_REORGANIZATION_PLAN.md for complete formula details.
    """
    try:
        logger.info(f"Fetching dual-success recommendations: genre={genre}, min_rating={min_rating}, limit={limit}")
        
        # Step 1: Get batch layer movies (movie_intelligence collection)
        batch_movies = queries.get_batch_movies_for_recommendations(
            min_rating=min_rating,
            genre=genre,
            min_popularity=1.0,
            min_vote_count=50
        )
        
        if not batch_movies:
            return DualSuccessResponse(
                recommendations=[],
                total_count=0,
                filters_applied={
                    "genre": genre,
                    "min_rating": min_rating,
                    "limit": limit
                }
            )
        
        logger.info(f"Retrieved {len(batch_movies)} movies from batch layer")
        
        # Step 2: Get speed layer engagement data
        movie_titles = [movie['title'] for movie in batch_movies]
        speed_engagement = queries.get_speed_layer_engagement(
            movie_titles=movie_titles,
            days_back=30
        )
        
        logger.info(f"Retrieved speed layer data for {len(speed_engagement)} movies")
        
        # Step 3: Calculate raw scores for all movies
        movie_scores = []
        now = datetime.utcnow()
        
        for movie in batch_movies:
            movie_title = movie['title']
            
            # Calculate Reddit Score (raw)
            reddit_raw = 0.0
            has_speed_data = False
            discussion_count = 0
            
            if movie_title in speed_engagement:
                has_speed_data = True
                engagement = speed_engagement[movie_title]
                
                # Calculate total engagement
                upvotes = engagement.get('total_upvotes', 0)
                comments = engagement.get('total_comments', 0)
                awards = engagement.get('total_awards', 0)
                discussion_count = engagement.get('discussion_count', 0)
                
                total_engagement = upvotes + (comments * 2) + (awards * 10)
                
                # Apply minimum engagement threshold
                if total_engagement >= 10:
                    # Calculate recency weight
                    last_window = engagement.get('last_window_start')
                    if last_window:
                        age_hours = (now - last_window).total_seconds() / 3600
                        recency_weight = calculate_recency_weight(age_hours)
                    else:
                        recency_weight = 0.2
                    
                    # Calculate raw Reddit score
                    reddit_raw = math.log10(total_engagement + 1) * recency_weight
            
            # Calculate TMDB Score (raw) - Calibrated hybrid formula
            # Components: 50% popularity + 30% quality + 20% credibility
            popularity = movie.get('popularity', 0)
            vote_average = movie.get('vote_average', 0)
            vote_count = movie.get('vote_count', 0)
            
            tmdb_raw = (
                0.5 * popularity +
                0.3 * (vote_average * 10) +
                0.2 * math.log10(vote_count + 1)
            )
            
            # Store movie with raw scores
            movie_scores.append({
                'movie': movie,
                'reddit_raw': reddit_raw,
                'tmdb_raw': tmdb_raw,
                'has_speed_data': has_speed_data,
                'discussion_count': discussion_count
            })
        
        # Step 4: Normalize scores to 0-100 scale
        reddit_raws = [m['reddit_raw'] for m in movie_scores]
        tmdb_raws = [m['tmdb_raw'] for m in movie_scores]
        
        reddit_normalized = normalize_scores(reddit_raws)
        tmdb_normalized = normalize_scores(tmdb_raws)
        
        # Step 5: Calculate dual-success scores and build recommendations
        recommendations = []
        
        for i, movie_score in enumerate(movie_scores):
            movie = movie_score['movie']
            reddit_score = reddit_normalized[i]
            tmdb_score = tmdb_normalized[i]
            
            # Calculate dual-success score: 60% Reddit + 40% TMDB
            dual_success_score = (0.6 * reddit_score) + (0.4 * tmdb_score)
            
            # Get primary genre (handle both flat and array)
            genre_value = movie.get('genre')
            if not genre_value and 'genres' in movie:
                genres = movie.get('genres', [])
                genre_value = genres[0] if genres else None
            
            recommendations.append({
                'movie_id': movie.get('movie_id'),
                'movie_title': movie.get('title'),
                'genre': genre_value,
                'dual_success_score': round(dual_success_score, 1),
                'reddit_buzz_score': round(reddit_score, 1),
                'tmdb_score': round(tmdb_score, 1),
                'vote_average': movie.get('vote_average'),
                'vote_count': movie.get('vote_count'),
                'popularity': movie.get('popularity'),
                'reddit_mentions': movie_score['discussion_count'],
                'speed_layer_contribution': movie_score['has_speed_data']
            })
        
        # Step 6: Sort by dual-success score and assign ranks
        sorted_recs = sorted(recommendations, key=lambda x: x['dual_success_score'], reverse=True)
        
        # Apply limit and assign ranks
        final_recs = []
        for i, rec in enumerate(sorted_recs[:limit]):
            rec['rank'] = i + 1
            final_recs.append(DualSuccessRecommendation(**rec))
        
        logger.info(f"Returning {len(final_recs)} dual-success recommendations")
        
        return DualSuccessResponse(
            recommendations=final_recs,
            total_count=len(final_recs),
            filters_applied={
                "genre": genre,
                "min_rating": min_rating,
                "limit": limit
            }
        )
    
    except Exception as e:
        logger.error(f"Error in dual-success recommendations: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/dual-success/genre/{genre}", response_model=DualSuccessResponse)
async def get_dual_success_by_genre(
    genre: str,
    min_rating: float = Query(6.0, ge=0, le=10, description="Minimum TMDB rating"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    queries: MovieQueries = Depends(get_movie_queries)
):
    """Get dual-success recommendations for specific genre"""
    return await get_dual_success_recommendations(
        genre=genre,
        min_rating=min_rating,
        limit=limit,
        queries=queries
    )

@router.get("/similar/{movie_id}")
async def get_similar_movies(
    movie_id: int,
    limit: int = Query(10, ge=1, le=50)
):
    """Get content-based similar movies"""
    pass

@router.get("/reddit-buzz")
async def get_reddit_buzz_recommendations(
    genre: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100)
):
    """Get top Reddit buzz movies (Reddit component only)"""
    pass

@router.get("/tmdb-quality")
async def get_tmdb_quality_recommendations(
    genre: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100)
):
    """Get top TMDB quality movies (TMDB component only)"""
    pass

@router.get("/personalized")
async def get_personalized_recommendations():
    """Get personalized recommendations (future feature)"""
    raise HTTPException(status_code=501, detail="Not implemented yet")
