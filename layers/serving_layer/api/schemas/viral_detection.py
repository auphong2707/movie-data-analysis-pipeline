"""
Viral Detection Response Schemas

Pydantic models for Goal #2: Viral Content Identification
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class ViralMetrics(BaseModel):
    """Viral engagement metrics"""
    viral_coefficient: float = Field(..., description="Viral score / threshold (current Reddit buzz / current TMDB buzz)")
    viral_score: float = Field(..., description="Total viral score summed across all windows in 48h period")
    viral_status: str = Field(..., description="Status: viral, trending, growing, or stable")
    upvote_velocity: float = Field(..., description="Maximum upvote velocity across all windows")
    comment_velocity: float = Field(..., description="Maximum comment velocity across all windows")
    award_velocity: float = Field(..., description="Maximum award velocity across all windows")


class RedditEngagement(BaseModel):
    """Reddit engagement aggregated across all windows"""
    total_upvotes: int = Field(..., description="Total upvotes aggregated across all windows")
    total_comments: int = Field(..., description="Total comments aggregated across all windows")
    total_awards: int = Field(..., description="Total awards aggregated across all windows")
    avg_sentiment: float = Field(..., description="Average sentiment across all windows")


class MovieIntelligence(BaseModel):
    """Movie metadata from batch layer"""
    genre: str = Field(..., description="Movie genre")
    budget_tier: Optional[str] = Field(None, description="Budget tier classification")
    vote_average: Optional[float] = Field(None, description="TMDB vote average")
    vote_count: Optional[int] = Field(None, description="TMDB vote count")
    popularity: Optional[float] = Field(None, description="Current TMDB popularity score")
    release_year: Optional[int] = Field(None, description="Movie release year")
    franchise: Optional[str] = Field(None, description="Movie franchise if applicable")


class ThresholdContext(BaseModel):
    """Threshold context used for viral coefficient calculation"""
    threshold_used: float = Field(..., description="Threshold value used (avg_popularity)")
    threshold_type: str = Field(..., description="Type of threshold: avg_popularity")
    threshold_dimension: str = Field(..., description="Dimension: genre or global")


class TrendingMovieResponse(BaseModel):
    """Response for a single trending movie"""
    # Movie identification
    movie_id: int = Field(..., description="Unique movie ID")
    movie_title: str = Field(..., description="Movie title")
    
    # Viral metrics
    viral_metrics: ViralMetrics
    
    # Reddit engagement
    reddit_engagement: RedditEngagement
    
    # Movie intelligence
    movie_intelligence: MovieIntelligence
    
    # Threshold context
    threshold_context: ThresholdContext
    
    # Timestamps
    last_window_start: datetime = Field(..., description="Timestamp of the most recent data window")


class TrendingMoviesResponse(BaseModel):
    """Response for trending movies list"""
    movies: list[TrendingMovieResponse]
    count: int = Field(..., description="Number of movies returned")
    filters_applied: dict = Field(..., description="Filters applied to the query")


class ViralScoreDetailResponse(BaseModel):
    """Detailed viral score for a specific movie"""
    # Movie identification
    movie_id: int
    movie_title: str
    
    # Viral metrics
    viral_metrics: ViralMetrics
    
    # Reddit engagement breakdown
    reddit_engagement: RedditEngagement
    
    # Time series data
    window_count: int = Field(..., description="Number of time windows aggregated")
    time_range_hours: int = Field(..., description="Time range in hours")
    
    # Movie intelligence
    movie_intelligence: MovieIntelligence
    
    # Threshold context
    threshold_context: ThresholdContext
    
    # Timestamps
    first_window_start: datetime
    last_window_start: datetime


class ThresholdResponse(BaseModel):
    """Response for a single threshold"""
    dimension: str = Field(..., description="Dimension type: genre, budget_tier, or season")
    value: str = Field(..., description="Value of the dimension")
    threshold_used_in_calculation: float = Field(..., description="Threshold used in viral coefficient calculation (avg_popularity)")
    avg_popularity: float = Field(..., description="Average TMDB popularity (current TMDB buzz metric)")
    viral_threshold: float = Field(..., description="99th percentile of vote_count (NOT used in calculation, for reference only)")
    movie_count: int = Field(..., description="Number of movies in this dimension")
    note: Optional[str] = Field(None, description="Explanation of threshold usage")


class GenreThresholdSummary(BaseModel):
    """Summary of threshold for genre listing"""
    genre: str = Field(..., description="Genre name")
    threshold_used_in_calculation: float = Field(..., description="Threshold used in calculation")
    avg_popularity: float = Field(..., description="Average TMDB popularity")
    viral_threshold: float = Field(..., description="99th percentile vote_count (reference only)")
    movie_count: int = Field(..., description="Number of movies in genre")


class ThresholdsListResponse(BaseModel):
    """Response for thresholds list (all genres)"""
    thresholds: list[GenreThresholdSummary]
    count: int = Field(..., description="Number of thresholds returned")
    note: str = Field(default="avg_popularity is used as denominator in viral coefficient calculation")


class OpportunityFactors(BaseModel):
    """Breakdown of opportunity score factors"""
    recency: float = Field(..., description="Recency factor: exp(-age_hours / 24) - how fresh is the discussion (0-1)")
    momentum: float = Field(..., description="Momentum factor: velocity_now / velocity_24h_ago - is discussion accelerating? (>1 = yes)")
    reach: float = Field(..., description="Urgency: recency × momentum - how urgent is action needed? (stored in reach field for compatibility)")


class MarketingOpportunity(BaseModel):
    """Marketing opportunity for a single movie"""
    movie_id: int = Field(..., description="Unique movie ID")
    movie_title: str = Field(..., description="Movie title")
    viral_coefficient: float = Field(..., description="Viral coefficient (V)")
    opportunity_score: float = Field(..., description="Opportunity score (O)")
    recommended_action: str = Field(..., description="Recommended action: amplify_immediately, monitor_closely, organic_growth, or no_action")
    estimated_reach: float = Field(..., description="Estimated reach with marketing amplification (7-day projection)")
    factors: OpportunityFactors = Field(..., description="Breakdown of opportunity factors")
    age_hours: float = Field(..., description="Hours since latest engagement")
    velocity_now: float = Field(..., description="Current viral score from latest window")
    velocity_24h_ago: float = Field(..., description="Viral score from 24h ago (or earliest window)")


class OpportunitiesResponse(BaseModel):
    """Response for marketing opportunities list"""
    opportunities: list[MarketingOpportunity]
    count: int = Field(..., description="Number of opportunities returned")
    filters_applied: dict = Field(..., description="Filters applied to the query")

