"""
Crisis Detection Response Schemas

Pydantic models for Goal #1: PR Crisis Detection & Sentiment Monitoring
"""
from pydantic import BaseModel
from typing import Optional, Dict


class BaselineInfo(BaseModel):
    """Baseline information"""
    type: str
    value: str
    avg_sentiment: float
    sentiment_stddev: float
    movie_count: int


class BaselineAvailability(BaseModel):
    """Baseline availability and values"""
    available: bool
    value: Optional[str] = None
    avg_sentiment: Optional[float] = None
    sentiment_stddev: Optional[float] = None
    movie_count: Optional[int] = None


class DeviationDetail(BaseModel):
    """Deviation details for a baseline"""
    deviation_sigma: float
    is_crisis: bool
    severity: str


class DeviationAnalysis(BaseModel):
    """Complete deviation analysis"""
    using_baseline: DeviationDetail
    all_baselines: Dict[str, DeviationDetail]
    comparison_note: str


class MovieSentimentResponse(BaseModel):
    """Response for movie sentiment endpoint"""
    movie_id: int
    movie_title: str
    current_sentiment: float
    sentiment_source: str
    baseline_used: BaselineInfo
    baseline_alternatives: Dict[str, BaselineAvailability]
    deviation_analysis: DeviationAnalysis
    last_updated: str


class CrisisAlert(BaseModel):
    """Individual crisis alert"""
    movie_id: int
    movie_title: str
    current_sentiment: float
    baseline_sentiment: float
    baseline_type: str
    deviation_sigma: float
    severity: str
    alert_timestamp: str
    data_age_hours: float


class CrisisAlertsResponse(BaseModel):
    """Response for crisis alerts listing"""
    total_alerts: int
    alerts: list[CrisisAlert]
    filters_applied: Dict[str, Optional[str]]


class Percentiles(BaseModel):
    """Statistical percentiles"""
    min: float
    q1: float
    median: float
    q3: float
    max: float


class DateRange(BaseModel):
    """Date range for data"""
    start_date: str
    end_date: str


class BaselineStatsResponse(BaseModel):
    """Response for baseline statistics endpoints (genre/franchise/year)"""
    dimension_type: str  # "genre", "franchise", or "year"
    dimension_value: str  # The actual genre/franchise/year value
    baseline_sentiment: float
    stddev_sentiment: float
    sample_size: int
    percentiles: Percentiles
    crisis_threshold: float
    data_range: Optional[DateRange] = None

