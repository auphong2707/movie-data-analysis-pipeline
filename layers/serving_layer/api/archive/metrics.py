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
    dual_success_score
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
