"""
Prediction Endpoints - Trend prediction and forecasting

Analyzes time-series signals:
- Popularity trends
- Vote count velocity
- Rating velocity
- Short-term demand forecasting
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
import logging

from mongodb.client import get_database
from query_engine.prediction_engine import PredictionEngine
from query_engine.cache_manager import get_cache_manager

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/predictions",
    tags=["predictions"]
)


def get_prediction_engine():
    """Get PredictionEngine instance"""
    db = get_database()
    return PredictionEngine(db)


def get_cache():
    """Get cache manager instance"""
    return get_cache_manager()


@router.get("/movies/{movie_id}/trend")
async def predict_movie_trend(
    movie_id: int,
    forecast_hours: int = Query(24, ge=6, le=168, description="Hours to forecast"),
    engine: PredictionEngine = Depends(get_prediction_engine),
    cache = Depends(get_cache)
):
    """
    Predict movie trend direction (rising/declining/stable)
    
    Analyzes:
    - Popularity velocity and acceleration
    - Vote count growth rate
    - Rating velocity
    
    Args:
        movie_id: TMDB movie ID
        forecast_hours: Hours to forecast (6-168)
    
    Returns:
        Trend prediction with confidence score
    """
    try:
        cache_key = f"pred:trend:{movie_id}:{forecast_hours}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        prediction = engine.predict_trend(
            movie_id=movie_id,
            forecast_hours=forecast_hours
        )
        
        if not prediction:
            raise HTTPException(
                status_code=404,
                detail=f"Insufficient data to predict trend for movie {movie_id}"
            )
        
        cache.set(cache_key, prediction, ttl_seconds=300)  # 5 minutes
        
        return prediction
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error predicting trend for movie {movie_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/movies/{movie_id}/popularity")
async def forecast_popularity(
    movie_id: int,
    hours_ahead: int = Query(24, ge=6, le=168),
    engine: PredictionEngine = Depends(get_prediction_engine),
    cache = Depends(get_cache)
):
    """
    Forecast popularity score using historical patterns
    
    Uses simple linear extrapolation based on recent velocity
    
    Args:
        movie_id: TMDB movie ID
        hours_ahead: Hours to forecast
    
    Returns:
        Forecasted popularity with confidence interval
    """
    try:
        cache_key = f"pred:popularity:{movie_id}:{hours_ahead}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        forecast = engine.forecast_popularity(
            movie_id=movie_id,
            hours_ahead=hours_ahead
        )
        
        if not forecast:
            raise HTTPException(
                status_code=404,
                detail=f"Insufficient data to forecast popularity for movie {movie_id}"
            )
        
        cache.set(cache_key, forecast, ttl_seconds=300)
        
        return forecast
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error forecasting popularity: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/trending/forecast")
async def forecast_trending(
    genre: Optional[str] = Query(None, description="Filter by genre"),
    hours_ahead: int = Query(6, ge=1, le=48),
    limit: int = Query(20, ge=1, le=100),
    engine: PredictionEngine = Depends(get_prediction_engine),
    cache = Depends(get_cache)
):
    """
    Forecast which movies will be trending in the near future
    
    Identifies movies with:
    - High positive velocity
    - Accelerating popularity
    - Rising sentiment
    
    Args:
        genre: Optional genre filter
        hours_ahead: Hours to forecast
        limit: Number of results
    
    Returns:
        Predicted trending movies
    """
    try:
        cache_key = f"pred:trending:{genre}:{hours_ahead}:{limit}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        forecast = engine.forecast_trending(
            genre=genre,
            hours_ahead=hours_ahead,
            limit=limit
        )
        
        response = {
            'forecast_time': hours_ahead,
            'genre': genre,
            'predicted_trending': forecast,
            'total': len(forecast)
        }
        
        cache.set(cache_key, response, ttl_seconds=600)  # 10 minutes
        
        return response
        
    except Exception as e:
        logger.error(f"Error forecasting trending: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/genres/{genre}/demand")
async def predict_genre_demand(
    genre: str,
    days_ahead: int = Query(7, ge=1, le=30),
    engine: PredictionEngine = Depends(get_prediction_engine),
    cache = Depends(get_cache)
):
    """
    Predict short-term demand for a genre
    
    Based on:
    - Historical popularity patterns
    - Current trending velocity
    - Seasonal patterns
    
    Args:
        genre: Genre name
        days_ahead: Days to forecast
    
    Returns:
        Demand forecast with trend direction
    """
    try:
        cache_key = f"pred:demand:{genre}:{days_ahead}"
        cached = cache.get(cache_key)
        if cached:
            return cached
        
        forecast = engine.predict_genre_demand(
            genre=genre,
            days_ahead=days_ahead
        )
        
        if not forecast:
            raise HTTPException(
                status_code=404,
                detail=f"Insufficient data to predict demand for genre {genre}"
            )
        
        cache.set(cache_key, forecast, ttl_seconds=1800)  # 30 minutes
        
        return forecast
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error predicting genre demand: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
