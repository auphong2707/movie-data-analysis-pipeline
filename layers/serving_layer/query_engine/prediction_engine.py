"""
Prediction Engine - Time-series forecasting for trends and demand

Implements:
1. Trend detection (rising/declining/stable)
2. Popularity forecasting
3. Genre demand prediction
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pymongo.database import Database
import logging

logger = logging.getLogger(__name__)


class PredictionEngine:
    """
    Time-series prediction engine for movie trends
    """
    
    def __init__(self, db: Database):
        self.db = db
        self.batch_views = db.batch_views
        self.speed_views = db.speed_views
    
    def predict_trend(
        self,
        movie_id: int,
        forecast_hours: int = 24
    ) -> Optional[Dict[str, Any]]:
        """
        Predict trend direction for a movie
        
        Analyzes:
        - Popularity velocity (rate of change)
        - Popularity acceleration (velocity change)
        - Vote count velocity
        - Rating velocity
        """
        # Get recent stats from speed layer
        cutoff = datetime.utcnow() - timedelta(hours=48)
        stats = list(self.speed_views.find({
            'movie_id': movie_id,
            'data_type': 'stats',
            'hour': {'$gte': cutoff}
        }).sort('hour', 1))
        
        if len(stats) < 3:
            return None
        
        # Extract time series with safe access
        popularity_series = [s['stats'].get('popularity', 0) for s in stats if 'stats' in s and s['stats'].get('popularity')]
        vote_count_series = [s['stats'].get('vote_count', 0) for s in stats if 'stats' in s and s['stats'].get('vote_count')]
        rating_series = [s['stats'].get('vote_average', 0) for s in stats if 'stats' in s and s['stats'].get('vote_average')]
        
        if not popularity_series or not vote_count_series or not rating_series:
            return None
        
        # Calculate velocities
        pop_velocity = self._calculate_velocity(popularity_series)
        pop_acceleration = self._calculate_acceleration(popularity_series)
        vote_velocity = self._calculate_velocity(vote_count_series)
        rating_velocity = self._calculate_velocity(rating_series)
        
        # Determine trend
        trend_score = (
            pop_velocity * 0.4 +
            pop_acceleration * 0.3 +
            vote_velocity * 0.2 +
            rating_velocity * 0.1
        )
        
        if trend_score > 0.5:
            trend = 'rising'
            confidence = min(trend_score, 1.0)
        elif trend_score < -0.5:
            trend = 'declining'
            confidence = min(abs(trend_score), 1.0)
        else:
            trend = 'stable'
            confidence = 1.0 - abs(trend_score)
        
        # Get movie title
        movie = self.batch_views.find_one({
            'movie_id': movie_id,
            'view_type': 'movie_details'
        })
        title = movie.get('data', {}).get('title', 'Unknown') if movie else 'Unknown'
        
        return {
            'movie_id': movie_id,
            'title': title,
            'trend': trend,
            'confidence': round(confidence, 3),
            'metrics': {
                'popularity_velocity': round(pop_velocity, 3),
                'popularity_acceleration': round(pop_acceleration, 3),
                'vote_velocity': round(vote_velocity, 3),
                'rating_velocity': round(rating_velocity, 3)
            },
            'current_stats': {
                'popularity': round(popularity_series[-1], 2),
                'vote_count': int(vote_count_series[-1]),
                'vote_average': round(rating_series[-1], 2)
            },
            'forecast_hours': forecast_hours,
            'predicted_at': datetime.utcnow().isoformat()
        }
    
    def forecast_popularity(
        self,
        movie_id: int,
        hours_ahead: int = 24
    ) -> Optional[Dict[str, Any]]:
        """
        Forecast popularity using linear extrapolation
        """
        # Get recent stats
        cutoff = datetime.utcnow() - timedelta(hours=48)
        stats = list(self.speed_views.find({
            'movie_id': movie_id,
            'data_type': 'stats',
            'hour': {'$gte': cutoff}
        }).sort('hour', 1))
        
        if len(stats) < 2:
            return None
        
        popularity_series = [s['stats'].get('popularity', 0) for s in stats if 'stats' in s and s['stats'].get('popularity')]
        
        if not popularity_series:
            return None
        
        # Calculate velocity
        velocity = self._calculate_velocity(popularity_series)
        current_popularity = popularity_series[-1]
        
        # Simple linear forecast
        forecasted_popularity = current_popularity + (velocity * hours_ahead)
        
        # Calculate confidence (decreases with forecast distance)
        confidence = max(0.3, 1.0 - (hours_ahead / 168))  # 168 hours = 1 week
        
        # Confidence interval (±20%)
        margin = forecasted_popularity * 0.2
        
        return {
            'movie_id': movie_id,
            'current_popularity': round(current_popularity, 2),
            'forecasted_popularity': round(forecasted_popularity, 2),
            'hours_ahead': hours_ahead,
            'confidence': round(confidence, 3),
            'confidence_interval': {
                'lower': round(forecasted_popularity - margin, 2),
                'upper': round(forecasted_popularity + margin, 2)
            },
            'velocity': round(velocity, 3),
            'forecasted_at': datetime.utcnow().isoformat()
        }
    
    def forecast_trending(
        self,
        genre: Optional[str] = None,
        hours_ahead: int = 6,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Forecast which movies will be trending
        
        Identifies movies with:
        - High positive velocity
        - Positive acceleration
        """
        cutoff = datetime.utcnow() - timedelta(hours=24)
        
        # Get all recent stats
        query = {
            'data_type': 'stats',
            'hour': {'$gte': cutoff}
        }
        
        # Group by movie and calculate velocities
        pipeline = [
            {'$match': query},
            {'$sort': {'hour': 1}},
            {'$group': {
                '_id': '$movie_id',
                'stats': {'$push': '$stats'},
                'hours': {'$push': '$hour'}
            }}
        ]
        
        movie_stats = list(self.speed_views.aggregate(pipeline))
        
        predictions = []
        for movie in movie_stats:
            movie_id = movie['_id']
            stats = movie['stats']
            
            if len(stats) < 3:
                continue
            
            popularity_series = [s.get('popularity', 0) for s in stats if s.get('popularity')]
            
            if len(popularity_series) < 3:
                continue
            
            velocity = self._calculate_velocity(popularity_series)
            acceleration = self._calculate_acceleration(popularity_series)
            
            # Predict future popularity
            current_pop = popularity_series[-1]
            predicted_pop = current_pop + (velocity * hours_ahead) + (0.5 * acceleration * hours_ahead ** 2)
            
            # Calculate trending score
            trending_score = (
                predicted_pop * 0.5 +
                velocity * 30 +
                acceleration * 10
            )
            
            # Get movie metadata
            movie_doc = self.batch_views.find_one({
                'movie_id': movie_id,
                'view_type': 'movie_details'
            })
            
            if not movie_doc:
                continue
            
            data = movie_doc.get('data', {})
            movie_genres = data.get('genres', [])
            
            # Apply genre filter
            if genre and genre not in movie_genres:
                continue
            
            predictions.append({
                'movie_id': movie_id,
                'title': data.get('title', 'Unknown'),
                'genres': movie_genres,
                'current_popularity': round(current_pop, 2),
                'predicted_popularity': round(predicted_pop, 2),
                'velocity': round(velocity, 3),
                'acceleration': round(acceleration, 3),
                'trending_score': round(trending_score, 2)
            })
        
        # Sort by trending score
        predictions.sort(key=lambda x: x['trending_score'], reverse=True)
        
        return predictions[:limit]
    
    def predict_genre_demand(
        self,
        genre: str,
        days_ahead: int = 7
    ) -> Optional[Dict[str, Any]]:
        """
        Predict demand for a genre based on historical patterns
        """
        # Get genre analytics from batch layer
        genre_analytics = list(self.batch_views.find({
            'view_type': 'genre_analytics',
            'genre': {'$regex': f'^{genre}$', '$options': 'i'}
        }).sort('computed_at', 1).limit(30))
        
        if len(genre_analytics) < 3:
            return None
        
        # Extract popularity trend
        popularity_series = [g.get('avg_popularity', 0) for g in genre_analytics]
        movie_count_series = [g.get('total_movies', 0) for g in genre_analytics]
        
        # Calculate trends
        pop_velocity = self._calculate_velocity(popularity_series)
        count_velocity = self._calculate_velocity(movie_count_series)
        
        current_popularity = popularity_series[-1]
        current_count = movie_count_series[-1]
        
        # Forecast
        predicted_popularity = current_popularity + (pop_velocity * days_ahead)
        predicted_count = current_count + (count_velocity * days_ahead)
        
        # Determine demand trend
        if pop_velocity > 0.5:
            demand_trend = 'increasing'
        elif pop_velocity < -0.5:
            demand_trend = 'decreasing'
        else:
            demand_trend = 'stable'
        
        return {
            'genre': genre,
            'days_ahead': days_ahead,
            'current_metrics': {
                'avg_popularity': round(current_popularity, 2),
                'total_movies': int(current_count)
            },
            'predicted_metrics': {
                'avg_popularity': round(predicted_popularity, 2),
                'total_movies': int(predicted_count)
            },
            'demand_trend': demand_trend,
            'velocity': round(pop_velocity, 3),
            'confidence': 0.7,  # Medium confidence for genre-level predictions
            'predicted_at': datetime.utcnow().isoformat()
        }
    
    def _calculate_velocity(self, series: List[float]) -> float:
        """Calculate average rate of change"""
        if len(series) < 2:
            return 0.0
        
        velocities = []
        for i in range(1, len(series)):
            velocities.append(series[i] - series[i-1])
        
        return sum(velocities) / len(velocities)
    
    def _calculate_acceleration(self, series: List[float]) -> float:
        """Calculate rate of velocity change"""
        if len(series) < 3:
            return 0.0
        
        velocities = []
        for i in range(1, len(series)):
            velocities.append(series[i] - series[i-1])
        
        accelerations = []
        for i in range(1, len(velocities)):
            accelerations.append(velocities[i] - velocities[i-1])
        
        return sum(accelerations) / len(accelerations) if accelerations else 0.0
