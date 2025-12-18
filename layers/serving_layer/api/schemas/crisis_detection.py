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
