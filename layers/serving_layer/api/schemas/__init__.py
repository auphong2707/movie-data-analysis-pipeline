"""
API Response Schemas Package
"""
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

from api.schemas.recommendations import (
    DualSuccessRecommendation,
    DualSuccessResponse,
    SimilarMovieRecommendation,
    SimilarMoviesResponse,
    RedditBuzzRecommendation,
    RedditBuzzResponse,
    TMDBQualityRecommendation,
    TMDBQualityResponse
)

__all__ = [
    # Crisis Detection
    "BaselineInfo",
    "BaselineAvailability",
    "DeviationDetail",
    "DeviationAnalysis",
    "MovieSentimentResponse",
    "CrisisAlert",
    "CrisisAlertsResponse",
    "Percentiles",
    "DateRange",
    "BaselineStatsResponse",
    "SeverityCounts",
    "SentimentVelocity",
    "MonitoringDashboardResponse",
    
    # Viral Detection
    "TrendingMoviesResponse",
    "TrendingMovieResponse",
    "ViralMetrics",
    "RedditEngagement",
    "MovieIntelligence",
    "ThresholdContext",
    "ViralScoreDetailResponse",
    "ThresholdResponse",
    "GenreThresholdSummary",
    "ThresholdsListResponse",
    
    # Recommendations
    "DualSuccessRecommendation",
    "DualSuccessResponse",
    "SimilarMovieRecommendation",
    "SimilarMoviesResponse",
    "RedditBuzzRecommendation",
    "RedditBuzzResponse",
    "TMDBQualityRecommendation",
    "TMDBQualityResponse",
]

