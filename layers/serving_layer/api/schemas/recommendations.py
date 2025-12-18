"""
Pydantic Schemas for Recommendation Endpoints
Goal #3: Content Recommendation Optimization
"""
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class DualSuccessRecommendation(BaseModel):
    """Single dual-success recommendation"""
    rank: int = Field(..., description="Ranking position (1-based)")
    movie_id: int = Field(..., description="TMDB movie ID")
    movie_title: str = Field(..., description="Movie title")
    genre: Optional[str] = Field(None, description="Primary genre")
    dual_success_score: float = Field(..., description="Combined score (60% Reddit + 40% TMDB)", ge=0, le=100)
    reddit_buzz_score: float = Field(..., description="Normalized Reddit engagement score", ge=0, le=100)
    tmdb_score: float = Field(..., description="Normalized TMDB quality score", ge=0, le=100)
    vote_average: float = Field(..., description="TMDB vote average", ge=0, le=10)
    vote_count: int = Field(..., description="TMDB vote count", ge=0)
    popularity: float = Field(..., description="TMDB popularity score", ge=0)
    reddit_mentions: int = Field(0, description="Number of Reddit discussions", ge=0)
    speed_layer_contribution: bool = Field(..., description="Whether speed layer data contributed to score")
    
    class Config:
        json_schema_extra = {
            "example": {
                "rank": 1,
                "movie_id": 12345,
                "movie_title": "The Matrix",
                "genre": "Science Fiction",
                "dual_success_score": 87.5,
                "reddit_buzz_score": 92.0,
                "tmdb_score": 80.5,
                "vote_average": 8.7,
                "vote_count": 25000,
                "popularity": 125.3,
                "reddit_mentions": 156,
                "speed_layer_contribution": True
            }
        }


class DualSuccessResponse(BaseModel):
    """Response for dual-success recommendations endpoint"""
    recommendations: List[DualSuccessRecommendation]
    total_count: int = Field(..., description="Total number of recommendations returned")
    filters_applied: dict = Field(..., description="Query filters used")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "recommendations": [
                    {
                        "rank": 1,
                        "movie_id": 12345,
                        "movie_title": "The Matrix",
                        "genre": "Science Fiction",
                        "dual_success_score": 87.5,
                        "reddit_buzz_score": 92.0,
                        "tmdb_score": 80.5,
                        "vote_average": 8.7,
                        "vote_count": 25000,
                        "popularity": 125.3,
                        "reddit_mentions": 156,
                        "speed_layer_contribution": True
                    }
                ],
                "total_count": 1,
                "filters_applied": {
                    "genre": None,
                    "min_rating": 6.0,
                    "limit": 20
                },
                "timestamp": "2025-12-18T10:30:00Z"
            }
        }


class SimilarMovieRecommendation(BaseModel):
    """Single similar movie recommendation"""
    rank: int = Field(..., description="Ranking position (1-based)")
    movie_id: int = Field(..., description="TMDB movie ID")
    movie_title: str = Field(..., description="Movie title")
    similarity_score: float = Field(..., description="Cosine similarity score", ge=-1, le=1)
    genre: Optional[str] = Field(None, description="Primary genre")
    vote_average: float = Field(..., description="TMDB vote average", ge=0, le=10)
    vote_count: int = Field(..., description="TMDB vote count", ge=0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "rank": 1,
                "movie_id": 12346,
                "movie_title": "The Matrix Reloaded",
                "similarity_score": 0.95,
                "genre": "Science Fiction",
                "vote_average": 7.2,
                "vote_count": 18000
            }
        }


class SimilarMoviesResponse(BaseModel):
    """Response for similar movies endpoint"""
    source_movie_id: int = Field(..., description="ID of the source movie")
    source_movie_title: str = Field(..., description="Title of the source movie")
    similar_movies: List[SimilarMovieRecommendation]
    total_count: int = Field(..., description="Total number of similar movies found")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")


class RedditBuzzRecommendation(BaseModel):
    """Single Reddit buzz recommendation"""
    rank: int = Field(..., description="Ranking position (1-based)")
    movie_id: int = Field(..., description="TMDB movie ID")
    movie_title: str = Field(..., description="Movie title")
    genre: Optional[str] = Field(None, description="Primary genre")
    reddit_buzz_score: float = Field(..., description="Reddit engagement score", ge=0, le=100)
    total_engagement: int = Field(..., description="Total Reddit engagement", ge=0)
    reddit_mentions: int = Field(..., description="Number of Reddit discussions", ge=0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "rank": 1,
                "movie_id": 12345,
                "movie_title": "Dune",
                "genre": "Science Fiction",
                "reddit_buzz_score": 95.2,
                "total_engagement": 8500,
                "reddit_mentions": 245
            }
        }


class RedditBuzzResponse(BaseModel):
    """Response for Reddit buzz recommendations endpoint"""
    recommendations: List[RedditBuzzRecommendation]
    total_count: int = Field(..., description="Total number of recommendations returned")
    filters_applied: dict = Field(..., description="Query filters used")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")


class TMDBQualityRecommendation(BaseModel):
    """Single TMDB quality recommendation"""
    rank: int = Field(..., description="Ranking position (1-based)")
    movie_id: int = Field(..., description="TMDB movie ID")
    movie_title: str = Field(..., description="Movie title")
    genre: Optional[str] = Field(None, description="Primary genre")
    tmdb_score: float = Field(..., description="TMDB quality score", ge=0, le=100)
    vote_average: float = Field(..., description="TMDB vote average", ge=0, le=10)
    vote_count: int = Field(..., description="TMDB vote count", ge=0)
    popularity: float = Field(..., description="TMDB popularity score", ge=0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "rank": 1,
                "movie_id": 12345,
                "movie_title": "The Shawshank Redemption",
                "genre": "Drama",
                "tmdb_score": 98.5,
                "vote_average": 9.3,
                "vote_count": 38000,
                "popularity": 95.2
            }
        }


class TMDBQualityResponse(BaseModel):
    """Response for TMDB quality recommendations endpoint"""
    recommendations: List[TMDBQualityRecommendation]
    total_count: int = Field(..., description="Total number of recommendations returned")
    filters_applied: dict = Field(..., description="Query filters used")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
