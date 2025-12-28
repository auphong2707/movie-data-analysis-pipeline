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
    CrisisAlertsResponse,
    Percentiles,
    DateRange,
    BaselineStatsResponse,
    SeverityCounts,
    SentimentVelocity,
    MonitoringDashboardResponse
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/crisis-detection",
    tags=["crisis-detection"]
)


def get_severity(deviation_sigma: float) -> str:
    """
    Calculate severity level from deviation sigma
    
    Thresholds calibrated after fixing baseline compression (Rounded for clarity):
    - Critical: σ < -8.0 (mean - 3σ) → ~6% of movies
    - High: σ < -5.0 (mean - 2σ) → ~6% of movies  
    - Warning: σ < -2.0 (mean - 1σ) → ~10% of movies
    - Normal: σ >= -2.0
    
    Calibration date: 2025-12-19
    Dataset: 62 movies, mean=1.196, stddev=2.979
    Based on: 1%=-7.85, 5%=-5.17, 10%=-1.75
    """
    if deviation_sigma < -8.0:
        return "critical"
    elif deviation_sigma < -5.0:
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
    - Speed layer: Averages sentiment across ALL windows in last 48h (not just latest)
    - Calculates deviation from multiple baselines (franchise, genre, year)
    - Detects crisis conditions (σ < -3.0)
    
    Current Sentiment Calculation:
    - If movie discussed in last 48h: S_current = AVG(all speed_views windows)
    - Else: S_current = batch layer historical sentiment
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
        # Changed: Average across ALL windows in 48h period, not just latest
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        speed_data = list(db.speed_views.find({
            "window_start": {"$gte": cutoff_time}
        }))
        
        # Find matching speed layer data by title and calculate average sentiment
        # S_speed = AVG(metrics.avg_sentiment) across all speed_views windows in last 48h
        normalized_batch_title = normalize_movie_title(batch_movie.get("title", ""))
        speed_sentiments = []
        sentiment_source = "batch_layer"
        last_window_start = None
        
        for speed_doc in speed_data:
            normalized_speed_title = normalize_movie_title(speed_doc.get("movie_title", ""))
            if normalized_speed_title == normalized_batch_title:
                sentiment_val = speed_doc.get("metrics", {}).get("avg_sentiment")
                if sentiment_val is not None:
                    speed_sentiments.append(sentiment_val)
                    # Track the most recent window timestamp
                    window_start = speed_doc.get("window_start")
                    if last_window_start is None or (window_start and window_start > last_window_start):
                        last_window_start = window_start
        
        # Determine current sentiment (S_current)
        # If movie was discussed in last 48h: S_current = S_speed (average across all windows)
        # Else: S_current = S_batch (historical sentiment)
        if speed_sentiments:
            current_sentiment = sum(speed_sentiments) / len(speed_sentiments)
            sentiment_source = "speed_layer"
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
    List all movies currently in alert state (warning or worse)
    
    Scans recent speed layer data and calculates deviation from baselines
    to identify movies experiencing PR issues.
    
    Alert thresholds (Round Numbers - Strategy 4):
    - Critical: σ < -70 (bottom 1% of negative deviations)
    - High: σ < -30 (bottom 5% of negative deviations)
    - Warning: σ < -25 (bottom 15% of negative deviations)
    
    Implementation:
    - Groups speed_views by movie (last 48h)
    - Calculates S_current = AVG(all windows) for each movie
    - Matches with batch layer to get baseline
    - Returns movies with severity != 'normal'
    """
    try:
        # Get MongoDB connection
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        queries = MovieQueries(db)
        
        # Get recent speed layer data (last 48 hours)
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        speed_data = list(db.speed_views.find({
            "window_start": {"$gte": cutoff_time}
        }))
        
        # Group by movie and calculate average sentiment across all windows
        movie_sentiments = {}  # {normalized_title: {"sentiments": [], "latest_window": datetime, "title": str}}
        
        for speed_doc in speed_data:
            movie_title = speed_doc.get('movie_title', '')
            normalized_title = normalize_movie_title(movie_title)
            
            sentiment_val = speed_doc.get('metrics', {}).get('avg_sentiment')
            window_start = speed_doc.get('window_start')
            
            if sentiment_val is not None:
                if normalized_title not in movie_sentiments:
                    movie_sentiments[normalized_title] = {
                        "sentiments": [],
                        "latest_window": window_start,
                        "title": movie_title
                    }
                
                movie_sentiments[normalized_title]["sentiments"].append(sentiment_val)
                
                # Track the most recent window
                if isinstance(window_start, datetime):
                    current_latest = movie_sentiments[normalized_title]["latest_window"]
                    if current_latest is None or window_start > current_latest:
                        movie_sentiments[normalized_title]["latest_window"] = window_start
        
        # Process each unique movie
        alerts = []
        
        for normalized_title, sentiment_data in movie_sentiments.items():
            # Find matching batch movie
            batch_movie = db.movie_intelligence.find_one({
                "title": {"$regex": f"^{re.escape(normalized_title)}$", "$options": "i"}
            })
            
            if not batch_movie:
                continue
            
            # Apply genre filter if specified
            if genre and batch_movie.get("genre", "").lower() != genre.lower():
                continue
            
            # Calculate average sentiment across all windows in 48h
            sentiments = sentiment_data["sentiments"]
            if not sentiments:
                continue
            
            S_current = sum(sentiments) / len(sentiments)
            latest_window = sentiment_data["latest_window"]
            
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
            
            # Get severity level
            alert_severity = get_severity(σ)
            
            # Only include if in alert state (warning, high, or critical - not normal)
            if alert_severity != "normal":
                # Apply severity filter if specified
                if severity and alert_severity != severity.lower():
                    continue
                
                # Calculate data age
                if isinstance(latest_window, datetime):
                    data_age_hours = (datetime.utcnow() - latest_window).total_seconds() / 3600
                else:
                    data_age_hours = 0.0
                
                alerts.append(CrisisAlert(
                    movie_id=batch_movie.get("movie_id"),
                    movie_title=batch_movie.get("title"),
                    current_sentiment=round(S_current, 3),
                    baseline_sentiment=round(S_baseline, 4),
                    baseline_type=baseline_type,
                    deviation_sigma=round(σ, 2),
                    severity=alert_severity,
                    alert_timestamp=latest_window.isoformat() if isinstance(latest_window, datetime) else str(latest_window),
                    data_age_hours=round(data_age_hours, 1)
                ))
        
        # Sort by severity (most negative σ first)
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


@router.get("/baselines/genre/{genre}", response_model=BaselineStatsResponse)
async def get_genre_baseline(genre: str):
    """Get sentiment baseline statistics for a genre"""
    client = get_mongodb_client()
    db = client.get_database("moviedb")
    
    try:
        # Query sentiment_baselines collection for genre baseline
        # Schema: One dimension per document (genre, franchise, or year - not multiple)
        baseline_doc = db.sentiment_baselines.find_one({
            "genre": genre,
            "franchise": None,
            "year": None
        })
        
        if not baseline_doc:
            raise HTTPException(
                status_code=404,
                detail=f"No baseline data found for genre '{genre}'"
            )
        
        # Calculate percentiles from the baseline document
        # Note: MongoDB aggregation would be better, but using available data
        avg_sentiment = baseline_doc.get("avg_sentiment", 0.0)
        stddev = baseline_doc.get("sentiment_stddev", 0.0)
        
        # Estimate percentiles using normal distribution approximation
        # q1 ≈ mean - 0.675σ, median = mean, q3 ≈ mean + 0.675σ
        percentiles = Percentiles(
            min=baseline_doc.get("min_sentiment", avg_sentiment - 3 * stddev),
            q1=avg_sentiment - 0.675 * stddev,
            median=avg_sentiment,
            q3=avg_sentiment + 0.675 * stddev,
            max=baseline_doc.get("max_sentiment", avg_sentiment + 3 * stddev)
        )
        
        # Crisis threshold: 3 standard deviations below baseline
        crisis_threshold = avg_sentiment - 3 * stddev
        
        # Get data range if available
        data_range = None
        if "data_range" in baseline_doc and baseline_doc["data_range"]:
            data_range = DateRange(
                start_date=baseline_doc["data_range"].get("start_date", "unknown"),
                end_date=baseline_doc["data_range"].get("end_date", "unknown")
            )
        
        return BaselineStatsResponse(
            dimension_type="genre",
            dimension_value=genre,
            baseline_sentiment=avg_sentiment,
            stddev_sentiment=stddev,
            sample_size=baseline_doc.get("movie_count", 0),
            percentiles=percentiles,
            crisis_threshold=crisis_threshold,
            data_range=data_range
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching genre baseline: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/baselines/franchise/{franchise}", response_model=BaselineStatsResponse)
async def get_franchise_baseline(franchise: str):
    """Get sentiment baseline statistics for a franchise"""
    client = get_mongodb_client()
    db = client.get_database("moviedb")
    
    try:
        # Query sentiment_baselines collection for franchise baseline
        baseline_doc = db.sentiment_baselines.find_one({
            "franchise": franchise,
            "genre": None,
            "year": None
        })
        
        if not baseline_doc:
            raise HTTPException(
                status_code=404,
                detail=f"No baseline data found for franchise '{franchise}'"
            )
        
        avg_sentiment = baseline_doc.get("avg_sentiment", 0.0)
        stddev = baseline_doc.get("sentiment_stddev", 0.0)
        
        # Estimate percentiles using normal distribution
        percentiles = Percentiles(
            min=baseline_doc.get("min_sentiment", avg_sentiment - 3 * stddev),
            q1=avg_sentiment - 0.675 * stddev,
            median=avg_sentiment,
            q3=avg_sentiment + 0.675 * stddev,
            max=baseline_doc.get("max_sentiment", avg_sentiment + 3 * stddev)
        )
        
        crisis_threshold = avg_sentiment - 3 * stddev
        
        # Franchises typically don't have explicit date ranges
        data_range = None
        if "data_range" in baseline_doc and baseline_doc["data_range"]:
            data_range = DateRange(
                start_date=baseline_doc["data_range"].get("start_date", "unknown"),
                end_date=baseline_doc["data_range"].get("end_date", "unknown")
            )
        
        return BaselineStatsResponse(
            dimension_type="franchise",
            dimension_value=franchise,
            baseline_sentiment=avg_sentiment,
            stddev_sentiment=stddev,
            sample_size=baseline_doc.get("movie_count", 0),
            percentiles=percentiles,
            crisis_threshold=crisis_threshold,
            data_range=data_range
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching franchise baseline: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/baselines/year/{year}", response_model=BaselineStatsResponse)
async def get_year_baseline(year: int):
    """Get sentiment baseline statistics for a release year"""
    client = get_mongodb_client()
    db = client.get_database("moviedb")
    
    try:
        # Query sentiment_baselines collection for year baseline
        baseline_doc = db.sentiment_baselines.find_one({
            "year": year,
            "genre": None,
            "franchise": None
        })
        
        if not baseline_doc:
            raise HTTPException(
                status_code=404,
                detail=f"No baseline data found for year {year}"
            )
        
        avg_sentiment = baseline_doc.get("avg_sentiment", 0.0)
        stddev = baseline_doc.get("sentiment_stddev", 0.0)
        
        # Estimate percentiles using normal distribution
        percentiles = Percentiles(
            min=baseline_doc.get("min_sentiment", avg_sentiment - 3 * stddev),
            q1=avg_sentiment - 0.675 * stddev,
            median=avg_sentiment,
            q3=avg_sentiment + 0.675 * stddev,
            max=baseline_doc.get("max_sentiment", avg_sentiment + 3 * stddev)
        )
        
        crisis_threshold = avg_sentiment - 3 * stddev
        
        # Year baselines can indicate the year itself as the range
        data_range = DateRange(
            start_date=f"{year}-01-01",
            end_date=f"{year}-12-31"
        )
        
        return BaselineStatsResponse(
            dimension_type="year",
            dimension_value=str(year),
            baseline_sentiment=avg_sentiment,
            stddev_sentiment=stddev,
            sample_size=baseline_doc.get("movie_count", 0),
            percentiles=percentiles,
            crisis_threshold=crisis_threshold,
            data_range=data_range
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching year baseline: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/monitoring", response_model=MonitoringDashboardResponse)
async def get_monitoring_data():
    """
    Get real-time monitoring dashboard data
    
    Returns aggregated statistics for crisis detection monitoring:
    - Counts by severity level
    - Top declining movies by sentiment velocity
    - Overall sentiment trends
    """
    try:
        client = get_mongodb_client()
        db = client.get_database("moviedb")
        queries = MovieQueries(db)
        
        # Get recent speed layer data (last 48 hours)
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        speed_data = list(db.speed_views.find({
            "window_start": {"$gte": cutoff_time}
        }))
        
        # Group by movie and calculate average sentiment across all windows
        movie_sentiments = {}  # {normalized_title: {"sentiments": [], "latest_window": datetime, "title": str, "windows": []}}
        
        for speed_doc in speed_data:
            movie_title = speed_doc.get('movie_title', '')
            normalized_title = normalize_movie_title(movie_title)
            
            sentiment_val = speed_doc.get('metrics', {}).get('avg_sentiment')
            window_start = speed_doc.get('window_start')
            
            if sentiment_val is not None:
                if normalized_title not in movie_sentiments:
                    movie_sentiments[normalized_title] = {
                        "sentiments": [],
                        "latest_window": window_start,
                        "title": movie_title,
                        "windows": []
                    }
                
                movie_sentiments[normalized_title]["sentiments"].append(sentiment_val)
                movie_sentiments[normalized_title]["windows"].append(speed_doc)
                
                # Track the most recent window
                if isinstance(window_start, datetime):
                    current_latest = movie_sentiments[normalized_title]["latest_window"]
                    if current_latest is None or window_start > current_latest:
                        movie_sentiments[normalized_title]["latest_window"] = window_start
        
        # Initialize counters
        severity_counts = {
            "critical": 0,
            "high": 0,
            "warning": 0,
            "normal": 0
        }
        
        total_movies_tracked = 0
        crisis_movies = 0
        sentiment_sum = 0.0
        sentiment_velocities = []
        
        # Process each unique movie
        for normalized_title, sentiment_data in movie_sentiments.items():
            # Find matching batch movie
            batch_movie = db.movie_intelligence.find_one({
                "title": {"$regex": f"^{re.escape(normalized_title)}$", "$options": "i"}
            })
            
            if not batch_movie:
                continue
            
            total_movies_tracked += 1
            
            # Calculate average sentiment across all windows
            sentiments = sentiment_data["sentiments"]
            if not sentiments:
                continue
            
            S_current = sum(sentiments) / len(sentiments)
            sentiment_sum += S_current
            
            # Get baseline (try franchise → genre → year)
            baseline = None
            if batch_movie.get("franchise"):
                baseline = await get_sentiment_baseline(db, batch_movie, "franchise")
            if not baseline and batch_movie.get("genre"):
                baseline = await get_sentiment_baseline(db, batch_movie, "genre")
            if not baseline and batch_movie.get("release_year"):
                baseline = await get_sentiment_baseline(db, batch_movie, "year")
            
            if not baseline:
                continue
            
            # Calculate deviation
            S_baseline = baseline.get("avg_sentiment")
            σ_baseline = baseline.get("sentiment_stddev")
            
            if not σ_baseline or σ_baseline == 0:
                continue
            
            σ = (S_current - S_baseline) / σ_baseline
            severity = get_severity(σ)
            
            # Count by severity using get_severity function
            severity_counts[severity] += 1
            
            if severity in ["critical", "high"]:
                crisis_movies += 1
            
            # Calculate sentiment velocity (rate of change)
            # Get sentiment from 1 hour ago
            movie_title = sentiment_data["title"]
            time_1h_ago = datetime.utcnow() - timedelta(hours=1)
            
            # Find window from ~1 hour ago
            older_window = None
            for window in sentiment_data["windows"]:
                window_start = window.get('window_start')
                if isinstance(window_start, datetime):
                    if time_1h_ago - timedelta(minutes=30) <= window_start <= time_1h_ago + timedelta(minutes=30):
                        older_window = window
                        break
            
            S_1h_ago = None
            velocity = 0.0
            
            if older_window:
                S_1h_ago = older_window.get('metrics', {}).get('avg_sentiment')
                if S_1h_ago is not None:
                    # Calculate velocity: (S_current - S_1h_ago) / 1h
                    velocity = S_current - S_1h_ago
            
            sentiment_velocities.append(SentimentVelocity(
                movie_id=batch_movie.get("movie_id"),
                movie_title=batch_movie.get("title"),
                current_sentiment=round(S_current, 3),
                sentiment_1h_ago=round(S_1h_ago, 3) if S_1h_ago is not None else None,
                velocity=round(velocity, 4),
                is_accelerating=velocity < 0  # Negative velocity = declining sentiment
            ))
        
        # Sort by velocity (most declining first - most negative velocity)
        sentiment_velocities.sort(key=lambda x: x.velocity)
        top_declining = sentiment_velocities[:10]  # Top 10 most declining
        
        # Calculate average sentiment
        avg_sentiment = sentiment_sum / total_movies_tracked if total_movies_tracked > 0 else 0.0
        
        response = MonitoringDashboardResponse(
            severity_counts=SeverityCounts(**severity_counts),
            total_movies_tracked=total_movies_tracked,
            crisis_movies=crisis_movies,
            top_declining_movies=top_declining,
            average_sentiment=round(avg_sentiment, 3),
            last_updated=datetime.utcnow().isoformat()
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting monitoring data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

