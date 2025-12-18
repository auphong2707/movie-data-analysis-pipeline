"""
Viral Detection Routes - Goal #2: Viral Content Identification
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging
import re

from mongodb.client import get_mongodb_client
from api.schemas.viral_detection import (
    TrendingMoviesResponse,
    TrendingMovieResponse,
    ViralMetrics,
    RedditEngagement,
    MovieIntelligence,
    ThresholdContext,
    ViralScoreDetailResponse,
    ThresholdResponse,
    GenreThresholdSummary,
    ThresholdsListResponse
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/viral-detection",
    tags=["viral-detection"]
)


def normalize_movie_title(title: str) -> str:
    """Normalize movie title for matching between batch and speed layers"""
    # Remove special characters, lowercase, trim whitespace
    normalized = re.sub(r'[^\w\s]', '', title.lower().strip())
    # Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized


@router.get("/trending", response_model=TrendingMoviesResponse)
async def get_trending_movies(
    genre: Optional[str] = Query(None, description="Filter by genre"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    viral_threshold: float = Query(0.0, ge=0.0, description="Minimum viral coefficient"),
    window: int = Query(48, ge=1, le=168, description="Time window in hours")
):
    """
    Get top viral movies ranked by viral coefficient
    
    Implements Goal #2: Viral Detection formula:
    - Viral Coefficient (V) = viral_score / avg_popularity
    - viral_score: Aggregated Reddit engagement velocity from speed layer (summed across all windows)
    - avg_popularity: Average TMDB popularity for genre from viral_thresholds (current TMDB buzz)
    - Compares "current Reddit buzz" to "current TMDB buzz"
    
    Viral Status Classification (based on percentile thresholds):
    - viral: V ≥ 0.3 (Top 5% - extremely high engagement)
    - trending: 0.15 ≤ V < 0.3 (Top 10% - high engagement)
    - growing: 0.05 ≤ V < 0.15 (Top 25% - moderate engagement)
    - stable: V < 0.05 (Below top 25% - normal/low engagement)
    
    Data Sources:
    - speed_views: Reddit engagement metrics (48h TTL, aggregated across all windows)
    - movie_intelligence: Movie metadata from batch layer
    - viral_thresholds: Context-aware thresholds (genre-specific or global)
    """
    try:
        # Get MongoDB connection
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        
        # Note: speed_views has 48h TTL, so all data in collection is already within 48h
        # No need for explicit time cutoff - collection auto-expires old data
        # However, we respect the window parameter for custom time ranges
        
        # Build aggregation pipeline
        pipeline = []
        
        # Optional: Filter by time window if not default 48h
        if window != 48:
            cutoff_time = datetime.utcnow() - timedelta(hours=window)
            pipeline.append({
                "$match": {
                    "window_start": {"$gte": cutoff_time}
                }
            })
        
        # Group by movie, summing metrics across all windows
        # More discussion = more viral (we want total engagement)
        pipeline.extend([
            {
                "$group": {
                    "_id": "$movie_title",
                    "movie_title": {"$first": "$movie_title"},
                    "total_viral_score": {"$sum": "$metrics.viral_score"},
                    "latest_window": {"$max": "$window_start"},
                    "earliest_window": {"$min": "$window_start"},
                    "total_upvotes": {"$sum": "$metrics.total_upvotes"},
                    "total_comments": {"$sum": "$metrics.total_comments"},
                    "total_awards": {"$sum": "$metrics.total_awards"},
                    "avg_sentiment": {"$avg": "$metrics.avg_sentiment"},
                    "max_upvote_velocity": {"$max": "$metrics.upvote_velocity"},
                    "max_comment_velocity": {"$max": "$metrics.comment_velocity"},
                    "max_award_velocity": {"$max": "$metrics.award_velocity"},
                    "window_count": {"$sum": 1}
                }
            },
            # Sort by total viral score descending
            {"$sort": {"total_viral_score": -1}}
        ])
        
        # Execute aggregation
        speed_data = list(db.speed_views.aggregate(pipeline))
        
        # Process each movie
        movies = []
        for row in speed_data:
            # Match with batch layer to get movie intelligence data
            normalized_title = normalize_movie_title(row['movie_title'])
            batch_movie = db.movie_intelligence.find_one({
                "title": {"$regex": f"^{re.escape(normalized_title)}$", "$options": "i"}
            })
            
            if not batch_movie:
                logger.debug(f"No batch data found for speed layer movie: {row['movie_title']}")
                continue
            
            # Apply genre filter if specified
            movie_genre = batch_movie.get("genre", "")
            if genre and movie_genre.lower() != genre.lower():
                continue
            
            # Get threshold for genre only (SINGLE dimension as per schema)
            # Note: genre in movie_intelligence is a single string, not array
            threshold_doc = db.viral_thresholds.find_one({
                "genre": movie_genre,
                "budget_tier": None,
                "season": None
            })
            
            threshold_dimension = "genre"
            if not threshold_doc:
                # Fallback to global threshold (all null dimensions)
                threshold_doc = db.viral_thresholds.find_one({
                    "genre": None,
                    "budget_tier": None,
                    "season": None
                })
                threshold_dimension = "global"
            
            if not threshold_doc:
                logger.warning(f"No threshold found for movie: {row['movie_title']}")
                continue
            
            # Use avg_popularity as threshold (current TMDB buzz)
            # NOT viral_threshold (which is 99th percentile of vote_count)
            threshold = threshold_doc.get("avg_popularity")
            if not threshold or threshold == 0:
                logger.warning(f"Invalid threshold for movie: {row['movie_title']}")
                continue
            
            # Calculate viral coefficient using total viral_score (summed across all windows)
            viral_score = row['total_viral_score']
            V = viral_score / threshold
            
            # Apply viral_threshold filter
            if V < viral_threshold:
                continue
            
            # Determine viral status based on percentile thresholds
            if V >= 0.3:
                status = "viral"
            elif V >= 0.15:
                status = "trending"
            elif V >= 0.05:
                status = "growing"
            else:
                status = "stable"
            
            # Build response object
            movie_response = TrendingMovieResponse(
                # Movie identification
                movie_id=batch_movie.get("movie_id"),
                movie_title=batch_movie.get("title"),
                
                # Viral metrics
                viral_metrics=ViralMetrics(
                    viral_coefficient=V,
                    viral_score=viral_score,
                    viral_status=status,
                    upvote_velocity=row.get('max_upvote_velocity') or 0.0,
                    comment_velocity=row.get('max_comment_velocity') or 0.0,
                    award_velocity=row.get('max_award_velocity') or 0.0
                ),
                
                # Reddit engagement
                reddit_engagement=RedditEngagement(
                    total_upvotes=row.get('total_upvotes', 0),
                    total_comments=row.get('total_comments', 0),
                    total_awards=row.get('total_awards', 0),
                    avg_sentiment=row.get('avg_sentiment') or 0.0
                ),
                
                # Movie intelligence
                movie_intelligence=MovieIntelligence(
                    genre=movie_genre,
                    budget_tier=batch_movie.get("budget_tier"),
                    vote_average=batch_movie.get("vote_average"),
                    vote_count=batch_movie.get("vote_count"),
                    popularity=batch_movie.get("popularity"),
                    release_year=batch_movie.get("release_year"),
                    franchise=batch_movie.get("franchise")
                ),
                
                # Threshold context
                threshold_context=ThresholdContext(
                    threshold_used=threshold,
                    threshold_type="avg_popularity",
                    threshold_dimension=threshold_dimension
                ),
                
                # Timestamps
                last_window_start=row['latest_window']
            )
            
            movies.append(movie_response)
        
        # Sort by viral coefficient descending (most viral first)
        movies.sort(key=lambda x: x.viral_metrics.viral_coefficient, reverse=True)
        
        # Apply limit after sorting
        movies = movies[:limit]
        
        # Return response
        return TrendingMoviesResponse(
            movies=movies,
            count=len(movies),
            filters_applied={
                "genre": genre,
                "limit": limit,
                "viral_threshold": viral_threshold,
                "window_hours": window
            }
        )
        
    except Exception as e:
        logger.error(f"Error in get_trending_movies: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve trending movies: {str(e)}"
        )

@router.get("/trending/genre/{genre}")
async def get_trending_by_genre(
    genre: str,
    limit: int = Query(20, ge=1, le=100)
):
    """Get viral movies for specific genre"""
    pass

@router.get("/movies/{movie_id}/viral-score", response_model=ViralScoreDetailResponse)
async def get_viral_score(movie_id: int):
    """
    Get detailed viral metrics for specific movie
    
    Implements Goal #2: Viral Detection formula for individual movie:
    - Aggregates viral scores from ALL speed_views data (48h TTL)
    - Calculates viral coefficient: V = viral_score / avg_popularity
    - Provides detailed breakdown of engagement metrics
    - Shows time range and window count
    
    Returns 404 if:
    - Movie not found in batch layer
    - No recent discussion data in speed layer
    """
    try:
        # Get MongoDB connection
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        
        # Get batch layer movie data
        batch_movie = db.movie_intelligence.find_one({"movie_id": movie_id})
        if not batch_movie:
            raise HTTPException(
                status_code=404,
                detail=f"Movie with ID {movie_id} not found"
            )
        
        movie_title = batch_movie.get("title", "")
        normalized_title = normalize_movie_title(movie_title)
        
        # Aggregate viral scores from ALL speed_views data (48h TTL)
        # Note: speed_views has 48h TTL, so all data is recent by definition
        pipeline = [
            # Match by normalized title (we need to find matching speed_views)
            # Since speed_views uses movie_title, we'll match after normalization
            {
                "$group": {
                    "_id": "$movie_title",
                    "movie_title": {"$first": "$movie_title"},
                    "total_viral_score": {"$sum": "$metrics.viral_score"},
                    "total_upvotes": {"$sum": "$metrics.total_upvotes"},
                    "total_comments": {"$sum": "$metrics.total_comments"},
                    "total_awards": {"$sum": "$metrics.total_awards"},
                    "avg_sentiment": {"$avg": "$metrics.avg_sentiment"},
                    "max_upvote_velocity": {"$max": "$metrics.upvote_velocity"},
                    "max_comment_velocity": {"$max": "$metrics.comment_velocity"},
                    "max_award_velocity": {"$max": "$metrics.award_velocity"},
                    "first_window": {"$min": "$window_start"},
                    "last_window": {"$max": "$window_start"},
                    "window_count": {"$sum": 1}
                }
            }
        ]
        
        speed_data = list(db.speed_views.aggregate(pipeline))
        
        # Find matching speed data by normalized title
        speed_result = None
        for row in speed_data:
            if normalize_movie_title(row['movie_title']) == normalized_title:
                speed_result = row
                break
        
        if not speed_result:
            raise HTTPException(
                status_code=404,
                detail=f"No recent discussion data found for movie ID {movie_id}"
            )
        
        # Get threshold (same logic as trending endpoint)
        genre = batch_movie.get("genre", "")
        threshold_doc = db.viral_thresholds.find_one({
            "genre": genre,
            "budget_tier": None,
            "season": None
        })
        
        threshold_dimension = "genre"
        if not threshold_doc:
            # Fallback to global threshold
            threshold_doc = db.viral_thresholds.find_one({
                "genre": None,
                "budget_tier": None,
                "season": None
            })
            threshold_dimension = "global"
        
        if not threshold_doc:
            raise HTTPException(
                status_code=500,
                detail="No viral thresholds configured"
            )
        
        # Use avg_popularity as threshold (current TMDB buzz)
        threshold = threshold_doc.get("avg_popularity")
        if not threshold or threshold == 0:
            raise HTTPException(
                status_code=500,
                detail="Invalid threshold configuration"
            )
        
        # Calculate viral coefficient using summed total_viral_score
        viral_score = speed_result['total_viral_score']
        V = viral_score / threshold
        
        # Determine viral status
        if V >= 0.3:
            viral_status = "viral"
        elif V >= 0.15:
            viral_status = "trending"
        elif V >= 0.05:
            viral_status = "growing"
        else:
            viral_status = "stable"
        
        # Calculate time range
        first_window = speed_result['first_window']
        last_window = speed_result['last_window']
        time_range_hours = int((last_window - first_window).total_seconds() / 3600) if first_window and last_window else 48
        
        # Build response
        return ViralScoreDetailResponse(
            # Movie identification
            movie_id=movie_id,
            movie_title=movie_title,
            
            # Viral metrics
            viral_metrics=ViralMetrics(
                viral_coefficient=V,
                viral_score=viral_score,
                viral_status=viral_status,
                upvote_velocity=speed_result.get('max_upvote_velocity') or 0.0,
                comment_velocity=speed_result.get('max_comment_velocity') or 0.0,
                award_velocity=speed_result.get('max_award_velocity') or 0.0
            ),
            
            # Reddit engagement breakdown
            reddit_engagement=RedditEngagement(
                total_upvotes=speed_result.get('total_upvotes', 0),
                total_comments=speed_result.get('total_comments', 0),
                total_awards=speed_result.get('total_awards', 0),
                avg_sentiment=speed_result.get('avg_sentiment') or 0.0
            ),
            
            # Time series data
            window_count=speed_result['window_count'],
            time_range_hours=time_range_hours,
            
            # Movie intelligence
            movie_intelligence=MovieIntelligence(
                genre=genre,
                budget_tier=batch_movie.get("budget_tier"),
                vote_average=batch_movie.get("vote_average"),
                vote_count=batch_movie.get("vote_count"),
                popularity=batch_movie.get("popularity"),
                release_year=batch_movie.get("release_year"),
                franchise=batch_movie.get("franchise")
            ),
            
            # Threshold context
            threshold_context=ThresholdContext(
                threshold_used=threshold,
                threshold_type="avg_popularity",
                threshold_dimension=threshold_dimension
            ),
            
            # Timestamps
            first_window_start=first_window,
            last_window_start=last_window
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_viral_score: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve viral score: {str(e)}"
        )

@router.get("/thresholds")
async def get_viral_thresholds(
    genre: Optional[str] = Query(None, description="Filter by genre"),
    budget_tier: Optional[str] = Query(None, description="Filter by budget tier"),
    season: Optional[str] = Query(None, description="Filter by season")
):
    """
    Get viral thresholds by context (genre, budget_tier, or season)
    
    Important Notes:
    - avg_popularity is used as the threshold denominator in viral coefficient calculation
    - viral_threshold (99th percentile of vote_count) is stored but NOT used in calculation
    - This endpoint returns both values, but only avg_popularity is semantically correct
    - Schema constraint: EXACTLY ONE dimension per document (no multi-dimensional thresholds)
    
    Query Priority:
    1. If genre specified: return genre threshold
    2. Else if budget_tier specified: return budget_tier threshold  
    3. Else if season specified: return season threshold
    4. Else: return all genre thresholds
    """
    try:
        # Get MongoDB connection
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        
        # Priority: genre > budget_tier > season (query ONE dimension only)
        if genre:
            threshold = db.viral_thresholds.find_one({
                "genre": genre,
                "budget_tier": None,
                "season": None
            })
            
            if not threshold:
                raise HTTPException(
                    status_code=404,
                    detail=f"No threshold found for genre: {genre}"
                )
            
            return ThresholdResponse(
                dimension="genre",
                value=genre,
                threshold_used_in_calculation=threshold["avg_popularity"],
                avg_popularity=threshold["avg_popularity"],
                viral_threshold=threshold["viral_threshold"],
                movie_count=threshold.get("movie_count", 0),
                note="avg_popularity is used as denominator in viral coefficient calculation"
            )
        
        elif budget_tier:
            threshold = db.viral_thresholds.find_one({
                "genre": None,
                "budget_tier": budget_tier,
                "season": None
            })
            
            if not threshold:
                raise HTTPException(
                    status_code=404,
                    detail=f"No threshold found for budget_tier: {budget_tier}"
                )
            
            return ThresholdResponse(
                dimension="budget_tier",
                value=budget_tier,
                threshold_used_in_calculation=threshold["avg_popularity"],
                avg_popularity=threshold["avg_popularity"],
                viral_threshold=threshold["viral_threshold"],
                movie_count=threshold.get("movie_count", 0),
                note="avg_popularity is used as denominator in viral coefficient calculation"
            )
        
        elif season:
            threshold = db.viral_thresholds.find_one({
                "genre": None,
                "budget_tier": None,
                "season": season
            })
            
            if not threshold:
                raise HTTPException(
                    status_code=404,
                    detail=f"No threshold found for season: {season}"
                )
            
            return ThresholdResponse(
                dimension="season",
                value=season,
                threshold_used_in_calculation=threshold["avg_popularity"],
                avg_popularity=threshold["avg_popularity"],
                viral_threshold=threshold["viral_threshold"],
                movie_count=threshold.get("movie_count", 0),
                note="avg_popularity is used as denominator in viral coefficient calculation"
            )
        
        # If no filter, return all genre thresholds
        else:
            genre_thresholds = list(db.viral_thresholds.find({
                "genre": {"$ne": None},
                "budget_tier": None,
                "season": None
            }).sort("genre", 1))
            
            thresholds_list = [
                GenreThresholdSummary(
                    genre=t["genre"],
                    threshold_used_in_calculation=t["avg_popularity"],
                    avg_popularity=t["avg_popularity"],
                    viral_threshold=t["viral_threshold"],
                    movie_count=t.get("movie_count", 0)
                )
                for t in genre_thresholds
            ]
            
            return ThresholdsListResponse(
                thresholds=thresholds_list,
                count=len(thresholds_list),
                note="avg_popularity is used as denominator in viral coefficient calculation"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_viral_thresholds: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve thresholds: {str(e)}"
        )

@router.get("/velocity/{movie_id}")
async def get_engagement_velocity(movie_id: int):
    """Get engagement velocity metrics for movie"""
    pass

@router.get("/opportunities")
async def get_marketing_opportunities(
    min_viral_coefficient: float = Query(1.5, ge=1.0),
    limit: int = Query(10, ge=1, le=50)
):
    """Get marketing amplification opportunities"""
    pass
