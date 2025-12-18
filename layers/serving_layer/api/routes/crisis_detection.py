"""
Crisis Detection Routes - Goal #1: PR Crisis Detection & Sentiment Monitoring
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import logging
import re

from mongodb.client import get_mongodb_client
from mongodb.queries import MovieQueries
from api.schemas.crisis_detection import (
    BaselineInfo,
    BaselineAvailability,
    DeviationDetail,
    DeviationAnalysis,
    MovieSentimentResponse,
    CrisisAlert,
    CrisisAlertsResponse
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/crisis-detection",
    tags=["crisis-detection"]
)


def get_severity(deviation_sigma: float) -> str:
    """Calculate severity level from deviation sigma"""
    if deviation_sigma < -4.0:
        return "critical"
    elif deviation_sigma < -3.0:
        return "high"
    elif deviation_sigma < -2.0:
        return "warning"
    else:
        return "normal"


def normalize_movie_title(title: str) -> str:
    """Normalize movie title for matching between batch and speed layers"""
    # Remove special characters, lowercase, trim whitespace
    normalized = re.sub(r'[^\w\s]', '', title.lower().strip())
    # Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized


async def get_sentiment_baseline(
    db,
    movie: Dict[str, Any],
    baseline_type: str
) -> Optional[Dict[str, Any]]:
    """
    Get sentiment baseline for a specific type
    
    Args:
        db: MongoDB database instance
        movie: Movie document
        baseline_type: 'franchise', 'genre', or 'year'
    
    Returns:
        Baseline document or None
    """
    query = {
        "franchise": None,
        "genre": None,
        "year": None
    }
    
    if baseline_type == "franchise" and movie.get("franchise"):
        query["franchise"] = movie["franchise"]
    elif baseline_type == "genre" and movie.get("genre"):
        query["genre"] = movie["genre"]
    elif baseline_type == "year" and movie.get("release_year"):
        query["year"] = movie["release_year"]
    else:
        return None
    
    return db.sentiment_baselines.find_one(query)


@router.get("/movies/{movie_id}/sentiment", response_model=MovieSentimentResponse)
async def get_movie_sentiment(movie_id: int):
    """
    Get sentiment analysis for specific movie with deviation from baseline
    
    Implements Goal #1: PR Crisis Detection formula:
    - Merges sentiment from batch and speed layers
    - Calculates deviation from multiple baselines (franchise, genre, year)
    - Detects crisis conditions (σ < -3.0)
    """
    try:
        # Get MongoDB connection
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        queries = MovieQueries(db)
        
        # Get batch layer movie data
        batch_movie = queries.get_batch_movie_by_id(movie_id)
        if not batch_movie:
            raise HTTPException(
                status_code=404,
                detail=f"Movie with ID {movie_id} not found in batch layer"
            )
        
        # Get speed layer sentiment (last 48 hours)
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        speed_data = list(db.speed_views.find({
            "window_start": {"$gte": cutoff_time}
        }).sort("window_start", -1))
        
        # Find matching speed layer data by title
        normalized_batch_title = normalize_movie_title(batch_movie.get("title", ""))
        speed_sentiment = None
        sentiment_source = "batch_layer"
        last_window_start = None
        
        for speed_doc in speed_data:
            normalized_speed_title = normalize_movie_title(speed_doc.get("movie_title", ""))
            if normalized_speed_title == normalized_batch_title:
                speed_sentiment = speed_doc.get("metrics", {}).get("avg_sentiment")
                last_window_start = speed_doc.get("window_start")
                sentiment_source = "speed_layer"
                break
        
        # Determine current sentiment (S_current)
        if speed_sentiment is not None:
            current_sentiment = speed_sentiment
        else:
            current_sentiment = batch_movie.get("avg_sentiment", 0.0)
        
        # Get all available baselines
        baseline_franchise = await get_sentiment_baseline(db, batch_movie, "franchise")
        baseline_genre = await get_sentiment_baseline(db, batch_movie, "genre")
        baseline_year = await get_sentiment_baseline(db, batch_movie, "year")
        
        # Determine primary baseline (for display)
        primary_baseline = None
        primary_baseline_type = None
        
        if baseline_franchise:
            primary_baseline = baseline_franchise
            primary_baseline_type = "franchise"
        elif baseline_genre:
            primary_baseline = baseline_genre
            primary_baseline_type = "genre"
        elif baseline_year:
            primary_baseline = baseline_year
            primary_baseline_type = "year"
        else:
            raise HTTPException(
                status_code=404,
                detail=f"No sentiment baseline found for movie {movie_id}"
            )
        
        # Build baseline alternatives
        baseline_alternatives = {
            "franchise": BaselineAvailability(
                available=baseline_franchise is not None,
                value=batch_movie.get("franchise") if baseline_franchise else None,
                avg_sentiment=baseline_franchise.get("avg_sentiment") if baseline_franchise else None,
                sentiment_stddev=baseline_franchise.get("sentiment_stddev") if baseline_franchise else None,
                movie_count=baseline_franchise.get("movie_count") if baseline_franchise else None
            ),
            "genre": BaselineAvailability(
                available=baseline_genre is not None,
                value=batch_movie.get("genre") if baseline_genre else None,
                avg_sentiment=baseline_genre.get("avg_sentiment") if baseline_genre else None,
                sentiment_stddev=baseline_genre.get("sentiment_stddev") if baseline_genre else None,
                movie_count=baseline_genre.get("movie_count") if baseline_genre else None
            ),
            "year": BaselineAvailability(
                available=baseline_year is not None,
                value=str(batch_movie.get("release_year")) if baseline_year else None,
                avg_sentiment=baseline_year.get("avg_sentiment") if baseline_year else None,
                sentiment_stddev=baseline_year.get("sentiment_stddev") if baseline_year else None,
                movie_count=baseline_year.get("movie_count") if baseline_year else None
            )
        }
        
        # Calculate deviation for all baselines
        all_deviations = {}
        
        for bl_type, bl_data in [
            ("franchise", baseline_franchise),
            ("genre", baseline_genre),
            ("year", baseline_year)
        ]:
            if bl_data and bl_data.get("sentiment_stddev", 0) > 0:
                # Calculate sigma: σ = (S_current - baseline.avg_sentiment) / baseline.sentiment_stddev
                deviation_sigma = (
                    current_sentiment - bl_data["avg_sentiment"]
                ) / bl_data["sentiment_stddev"]
                
                is_crisis = deviation_sigma < -3.0
                severity = get_severity(deviation_sigma)
                
                all_deviations[bl_type] = DeviationDetail(
                    deviation_sigma=round(deviation_sigma, 2),
                    is_crisis=is_crisis,
                    severity=severity
                )
        
        # Get primary baseline deviation
        primary_deviation = all_deviations.get(primary_baseline_type)
        if not primary_deviation:
            raise HTTPException(
                status_code=500,
                detail="Failed to calculate deviation for primary baseline"
            )
        
        # Generate comparison note
        crisis_count = sum(1 for dev in all_deviations.values() if dev.is_crisis)
        if crisis_count == len(all_deviations):
            comparison_note = f"Crisis detected across all baseline types (all >3σ below baseline)"
        elif crisis_count > 0:
            crisis_types = [k for k, v in all_deviations.items() if v.is_crisis]
            comparison_note = f"Crisis detected in {', '.join(crisis_types)} baseline(s)"
        else:
            comparison_note = "No crisis detected in any baseline type"
        
        # Build response
        response = MovieSentimentResponse(
            movie_id=movie_id,
            movie_title=batch_movie.get("title", "Unknown"),
            current_sentiment=round(current_sentiment, 2),
            sentiment_source=sentiment_source,
            baseline_used=BaselineInfo(
                type=primary_baseline_type,
                value=(
                    batch_movie.get("franchise") if primary_baseline_type == "franchise"
                    else batch_movie.get("genre") if primary_baseline_type == "genre"
                    else str(batch_movie.get("release_year"))
                ),
                avg_sentiment=round(primary_baseline["avg_sentiment"], 2),
                sentiment_stddev=round(primary_baseline["sentiment_stddev"], 3),
                movie_count=primary_baseline["movie_count"]
            ),
            baseline_alternatives=baseline_alternatives,
            deviation_analysis=DeviationAnalysis(
                using_baseline=primary_deviation,
                all_baselines=all_deviations,
                comparison_note=comparison_note
            ),
            last_updated=(
                last_window_start.isoformat() if last_window_start
                else batch_movie.get("updated_at", datetime.utcnow()).isoformat()
            )
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting movie sentiment: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/movies/by-title/{title}/sentiment")
async def get_movie_sentiment_by_title(title: str):
    """Get sentiment by movie title"""
    try:
        # Get MongoDB connection
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        queries = MovieQueries(db)
        
        # Find movie ID by title
        movie_id = queries.get_movie_id_by_title(title)
        if not movie_id:
            raise HTTPException(
                status_code=404,
                detail=f"Movie with title '{title}' not found"
            )
        
        # Delegate to main sentiment endpoint
        return await get_movie_sentiment(movie_id)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting movie sentiment by title: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/alerts", response_model=CrisisAlertsResponse)
async def get_crisis_alerts(
    severity: Optional[str] = Query(None, description="Filter by severity: critical, high, warning"),
    genre: Optional[str] = Query(None, description="Filter by genre"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results")
):
    """
    List all movies currently in crisis state (σ < -3.0)
    
    Scans recent speed layer data and calculates deviation from baselines
    to identify movies experiencing PR crises.
    """
    try:
        # Get MongoDB connection
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        queries = MovieQueries(db)
        
        # Step 1: Get all movies with recent discussions (speed layer)
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        speed_movies = list(db.speed_views.find({
            "window_start": {"$gte": cutoff_time}
        }).sort("window_start", -1))
        
        # Step 2: Match with batch layer and calculate deviation
        alerts = []
        processed_titles = set()  # Avoid duplicates
        
        for speed_movie in speed_movies:
            movie_title = speed_movie.get('movie_title', '')
            
            # Skip if already processed
            normalized_title = normalize_movie_title(movie_title)
            if normalized_title in processed_titles:
                continue
            processed_titles.add(normalized_title)
            
            # Find matching movie in batch layer
            batch_movie = db.movie_intelligence.find_one({
                "title": {"$regex": f"^{re.escape(normalized_title)}$", "$options": "i"}
            })
            
            if not batch_movie:
                continue
            
            # Apply genre filter if specified
            if genre and batch_movie.get("genre", "").lower() != genre.lower():
                continue
            
            # Get current sentiment from speed layer
            S_current = speed_movie.get('metrics', {}).get('avg_sentiment')
            if S_current is None:
                continue
            
            # Get baseline using priority: franchise → genre → year
            baseline = None
            baseline_type = None
            
            # Try franchise first
            if batch_movie.get("franchise"):
                baseline = await get_sentiment_baseline(db, batch_movie, "franchise")
                if baseline:
                    baseline_type = "franchise"
            
            # Try genre if no franchise baseline
            if not baseline and batch_movie.get("genre"):
                baseline = await get_sentiment_baseline(db, batch_movie, "genre")
                if baseline:
                    baseline_type = "genre"
            
            # Try year if no genre baseline
            if not baseline and batch_movie.get("release_year"):
                baseline = await get_sentiment_baseline(db, batch_movie, "year")
                if baseline:
                    baseline_type = "year"
            
            if not baseline:
                continue
            
            S_baseline = baseline.get("avg_sentiment")
            σ_baseline = baseline.get("sentiment_stddev")
            
            if not σ_baseline or σ_baseline == 0:
                continue
            
            # Calculate deviation: σ = (S_current - S_baseline) / σ_baseline
            σ = (S_current - S_baseline) / σ_baseline
            
            # Only include if in crisis (σ < -3.0)
            if σ < -3.0:
                alert_severity = get_severity(σ)
                
                # Apply severity filter if specified
                if severity and alert_severity != severity.lower():
                    continue
                
                # Calculate data age
                window_start = speed_movie.get('window_start')
                if isinstance(window_start, datetime):
                    data_age_hours = (datetime.utcnow() - window_start).total_seconds() / 3600
                else:
                    data_age_hours = 0.0
                
                alerts.append(CrisisAlert(
                    movie_id=batch_movie.get("movie_id"),
                    movie_title=batch_movie.get("title"),
                    current_sentiment=round(S_current, 2),
                    baseline_sentiment=round(S_baseline, 2),
                    baseline_type=baseline_type,
                    deviation_sigma=round(σ, 2),
                    severity=alert_severity,
                    alert_timestamp=window_start.isoformat() if isinstance(window_start, datetime) else str(window_start),
                    data_age_hours=round(data_age_hours, 1)
                ))
        
        # Step 3: Sort by severity (most negative σ first)
        alerts.sort(key=lambda x: x.deviation_sigma)
        
        # Apply limit
        alerts = alerts[:limit]
        
        # Build response
        response = CrisisAlertsResponse(
            total_alerts=len(alerts),
            alerts=alerts,
            filters_applied={
                "severity": severity,
                "genre": genre,
                "limit": str(limit)
            }
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting crisis alerts: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/alerts/{alert_id}")
async def get_alert_details(alert_id: str):
    """Get specific crisis alert details"""
    # TODO: Implement alert details
    pass


@router.get("/baselines/genre/{genre}")
async def get_genre_baseline(genre: str):
    """Get sentiment baseline for genre"""
    # TODO: Implement genre baseline
    pass


@router.get("/baselines/franchise/{franchise}")
async def get_franchise_baseline(franchise: str):
    """Get sentiment baseline for franchise"""
    # TODO: Implement franchise baseline
    pass


@router.get("/baselines/year/{year}")
async def get_year_baseline(year: int):
    """Get sentiment baseline for year"""
    # TODO: Implement year baseline
    pass


@router.get("/monitoring")
async def get_monitoring_data():
    """Get real-time monitoring dashboard data"""
    # TODO: Implement monitoring dashboard
    pass
