"""
RESTful API for serving movie analytics data.
"""
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

from .mongodb_service import MongoDBManager, MovieQueryService, PersonQueryService, AnalyticsQueryService
from config.config import config

logger = logging.getLogger(__name__)

# Pydantic models for API responses
class MovieResponse(BaseModel):
    movie_id: int
    title: str
    release_year: Optional[int] = None
    language: Optional[str] = None
    vote_average: Optional[float] = None
    popularity: Optional[float] = None
    genres: Optional[List[Dict]] = None

class PersonResponse(BaseModel):
    person_id: int
    name: str
    known_for_department: Optional[str] = None
    popularity: Optional[float] = None
    profile_path: Optional[str] = None

class ReviewMetricsResponse(BaseModel):
    movie_id: int
    total_reviews: int
    avg_sentiment: float
    unique_reviewers: int

class TrendingMovieResponse(BaseModel):
    movie_id: int
    title: str
    trend_score: float
    window_start: datetime
    window_end: datetime

class GenreTrendResponse(BaseModel):
    genre: str
    year: int
    avg_rating: float
    avg_popularity: float
    movie_count: int

class ApiResponse(BaseModel):
    success: bool
    data: Any = None
    message: str = ""
    total_count: Optional[int] = None

# Initialize FastAPI app
app = FastAPI(
    title="Movie Analytics API",
    description="RESTful API for movie data analytics and insights",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
mongodb_manager = MongoDBManager()
movie_service = MovieQueryService(mongodb_manager)
person_service = PersonQueryService(mongodb_manager)
analytics_service = AnalyticsQueryService(mongodb_manager)

@app.get("/", response_model=ApiResponse)
async def root():
    """Root endpoint."""
    return ApiResponse(
        success=True,
        message="Movie Analytics API is running",
        data={"version": "1.0.0", "status": "healthy"}
    )

@app.get("/health", response_model=ApiResponse)
async def health_check():
    """Health check endpoint."""
    try:
        # Test MongoDB connection
        mongodb_manager.client.admin.command('ping')
        
        # Get basic stats
        performance_metrics = analytics_service.get_performance_metrics()
        
        return ApiResponse(
            success=True,
            message="Service is healthy",
            data={
                "mongodb_status": "connected",
                "collections": list(performance_metrics.keys()),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Service unhealthy: {str(e)}")

# Movie endpoints
@app.get("/movies/{movie_id}", response_model=ApiResponse)
async def get_movie(movie_id: int = Path(..., description="Movie ID")):
    """Get movie details by ID."""
    try:
        movie = movie_service.get_movie_by_id(movie_id)
        if not movie:
            raise HTTPException(status_code=404, detail="Movie not found")
        
        return ApiResponse(
            success=True,
            data=movie,
            message="Movie retrieved successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/movies/search", response_model=ApiResponse)
async def search_movies(
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Number of results")
):
    """Search movies by title."""
    try:
        movies = movie_service.search_movies(q, limit)
        return ApiResponse(
            success=True,
            data=movies,
            total_count=len(movies),
            message=f"Found {len(movies)} movies"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/movies/popular", response_model=ApiResponse)
async def get_popular_movies(
    limit: int = Query(50, ge=1, le=100, description="Number of results"),
    year: Optional[int] = Query(None, description="Filter by release year")
):
    """Get popular movies."""
    try:
        movies = movie_service.get_popular_movies(limit, year)
        return ApiResponse(
            success=True,
            data=movies,
            total_count=len(movies),
            message=f"Retrieved {len(movies)} popular movies"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/movies/top-rated", response_model=ApiResponse)
async def get_top_rated_movies(
    limit: int = Query(50, ge=1, le=100, description="Number of results"),
    min_reviews: int = Query(10, ge=1, description="Minimum number of reviews")
):
    """Get top rated movies."""
    try:
        movies = movie_service.get_top_rated_movies(limit, min_reviews)
        return ApiResponse(
            success=True,
            data=movies,
            total_count=len(movies),
            message=f"Retrieved {len(movies)} top rated movies"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/movies/trending", response_model=ApiResponse)
async def get_trending_movies(
    hours: int = Query(24, ge=1, le=168, description="Time window in hours")
):
    """Get trending movies."""
    try:
        movies = movie_service.get_trending_movies(hours)
        return ApiResponse(
            success=True,
            data=movies,
            total_count=len(movies),
            message=f"Retrieved {len(movies)} trending movies"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/movies/genre/{genre}", response_model=ApiResponse)
async def get_movies_by_genre(
    genre: str = Path(..., description="Genre name"),
    limit: int = Query(20, ge=1, le=100, description="Number of results")
):
    """Get movies by genre."""
    try:
        movies = movie_service.get_movies_by_genre(genre, limit)
        return ApiResponse(
            success=True,
            data=movies,
            total_count=len(movies),
            message=f"Retrieved {len(movies)} movies in {genre} genre"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/movies/{movie_id}/sentiment", response_model=ApiResponse)
async def get_movie_sentiment(movie_id: int = Path(..., description="Movie ID")):
    """Get sentiment analysis for a movie."""
    try:
        sentiment = movie_service.get_movie_sentiment(movie_id)
        if not sentiment:
            raise HTTPException(status_code=404, detail="Sentiment data not found")
        
        return ApiResponse(
            success=True,
            data=sentiment,
            message="Sentiment data retrieved successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# People endpoints
@app.get("/people/{person_id}", response_model=ApiResponse)
async def get_person(person_id: int = Path(..., description="Person ID")):
    """Get person details by ID."""
    try:
        person = person_service.get_person_by_id(person_id)
        if not person:
            raise HTTPException(status_code=404, detail="Person not found")
        
        return ApiResponse(
            success=True,
            data=person,
            message="Person retrieved successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/people/search", response_model=ApiResponse)
async def search_people(
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Number of results")
):
    """Search people by name."""
    try:
        people = person_service.search_people(q, limit)
        return ApiResponse(
            success=True,
            data=people,
            total_count=len(people),
            message=f"Found {len(people)} people"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/people/popular", response_model=ApiResponse)
async def get_popular_people(
    limit: int = Query(50, ge=1, le=100, description="Number of results"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """Get popular people."""
    try:
        people = person_service.get_popular_people(department, limit)
        return ApiResponse(
            success=True,
            data=people,
            total_count=len(people),
            message=f"Retrieved {len(people)} popular people"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/people/{person_id}/movies", response_model=ApiResponse)
async def get_person_movies(
    person_id: int = Path(..., description="Person ID"),
    credit_type: Optional[str] = Query(None, description="Filter by credit type (cast/crew)")
):
    """Get movies for a person."""
    try:
        movies = person_service.get_person_movies(person_id, credit_type)
        return ApiResponse(
            success=True,
            data=movies,
            total_count=len(movies),
            message=f"Retrieved {len(movies)} movies"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/movies/{movie_id}/cast", response_model=ApiResponse)
async def get_movie_cast(
    movie_id: int = Path(..., description="Movie ID"),
    limit: int = Query(20, ge=1, le=100, description="Number of results")
):
    """Get cast for a movie."""
    try:
        cast = person_service.get_movie_cast(movie_id, limit)
        return ApiResponse(
            success=True,
            data=cast,
            total_count=len(cast),
            message=f"Retrieved {len(cast)} cast members"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/movies/{movie_id}/crew", response_model=ApiResponse)
async def get_movie_crew(
    movie_id: int = Path(..., description="Movie ID"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """Get crew for a movie."""
    try:
        crew = person_service.get_movie_crew(movie_id, department)
        return ApiResponse(
            success=True,
            data=crew,
            total_count=len(crew),
            message=f"Retrieved {len(crew)} crew members"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Analytics endpoints
@app.get("/analytics/genre-trends", response_model=ApiResponse)
async def get_genre_trends(
    years: Optional[str] = Query(None, description="Comma-separated list of years")
):
    """Get genre popularity trends over time."""
    try:
        year_list = None
        if years:
            year_list = [int(y.strip()) for y in years.split(',')]
        
        trends = analytics_service.get_genre_trends(year_list)
        return ApiResponse(
            success=True,
            data=trends,
            total_count=len(trends),
            message=f"Retrieved {len(trends)} genre trend records"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/sentiment-trends", response_model=ApiResponse)
async def get_sentiment_trends(
    movie_id: Optional[int] = Query(None, description="Filter by movie ID"),
    days: int = Query(30, ge=1, le=365, description="Time window in days")
):
    """Get sentiment trends over time."""
    try:
        trends = analytics_service.get_sentiment_trends_over_time(movie_id, days)
        return ApiResponse(
            success=True,
            data=trends,
            total_count=len(trends),
            message=f"Retrieved {len(trends)} sentiment trend records"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/performance", response_model=ApiResponse)
async def get_performance_metrics():
    """Get database performance metrics."""
    try:
        metrics = analytics_service.get_performance_metrics()
        return ApiResponse(
            success=True,
            data=metrics,
            message="Performance metrics retrieved successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )