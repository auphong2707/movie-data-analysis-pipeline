"""
Business Metrics - Custom Prometheus metrics for business goals

Provides helper functions to track business-specific metrics:
- Crisis alerts (Goal #1)
- Viral detections (Goal #2)
- Recommendation performance (Goal #3)
"""

from api.main import (
    crisis_alerts_total,
    viral_detections_total,
    recommendation_requests_total,
    dual_success_score,
    sentiment_score,
    sentiment_baseline,
    movies_in_crisis
)


def record_crisis_alert(severity: str = "warning"):
    """
    Record a PR crisis alert detection
    
    Args:
        severity: Crisis severity level ("warning" or "critical")
    """
    crisis_alerts_total.labels(severity=severity).inc()


def record_viral_detection(genre: str = "unknown"):
    """
    Record a viral content detection
    
    Args:
        genre: Movie genre that went viral
    """
    viral_detections_total.labels(genre=genre).inc()


def record_recommendation_request(recommendation_type: str = "general"):
    """
    Record a recommendation request
    
    Args:
        recommendation_type: Type of recommendation (similar, genre, personalized, dual_success)
    """
    recommendation_requests_total.labels(recommendation_type=recommendation_type).inc()


def record_dual_success_score(score: float):
    """
    Record a dual-success recommendation score
    
    Args:
        score: Dual-success score (0.0 to 1.0)
    """
    dual_success_score.observe(score)


def update_sentiment_metrics(movie_data: dict):
    """
    Update sentiment gauge metrics for dashboard visualization
    
    Args:
        movie_data: Dictionary containing movie sentiment information with keys:
            - movie_id: TMDB movie ID
            - title: Movie title
            - genres: List of genre strings (from batch layer)
            - sentiment: Dict with 'overall_score', 'label', etc.
            - data_sources: Dict with 'batch' and 'speed' info
    """
    try:
        movie_id = str(movie_data.get('movie_id', 'unknown'))
        title = movie_data.get('title', 'Unknown')
        
        # Get primary genre from the genres array
        genres = movie_data.get('genres', [])
        if genres and isinstance(genres, list) and len(genres) > 0:
            genre = genres[0]
        else:
            genre = 'unknown'
        
        sentiment_data = movie_data.get('sentiment', {})
        current_score = sentiment_data.get('overall_score', 0)
        
        # Update current sentiment from speed layer (Reddit)
        data_sources = movie_data.get('data_sources', {})
        if data_sources.get('speed'):
            sentiment_score.labels(
                movie_id=movie_id,
                movie_title=title,
                genre=str(genre),
                source='reddit'
            ).set(current_score)
        
        # Update baseline sentiment from batch layer (TMDB)
        if data_sources.get('batch'):
            # Get baseline from comparison data if available
            comparison = sentiment_data.get('comparison', {})
            genre_baseline = comparison.get('genre_baseline', {})
            baseline_value = genre_baseline.get('avg_sentiment', current_score)
            
            sentiment_baseline.labels(
                movie_id=movie_id,
                movie_title=title,
                genre=str(genre),
                source='tmdb'
            ).set(baseline_value)
            
    except Exception as e:
        # Don't fail the request if metrics update fails
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to update sentiment metrics for {movie_data.get('title', 'unknown')}: {e}", exc_info=True)


def update_crisis_count(crisis_movies: list):
    """
    Update the count of movies in crisis state
    
    Args:
        crisis_movies: List of movies with crisis information
    """
    try:
        warning_count = sum(1 for m in crisis_movies if m.get('severity') == 'warning')
        critical_count = sum(1 for m in crisis_movies if m.get('severity') == 'critical')
        
        movies_in_crisis.labels(severity='warning').set(warning_count)
        movies_in_crisis.labels(severity='critical').set(critical_count)
        
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to update crisis count: {e}")
