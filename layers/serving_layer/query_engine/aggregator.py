"""
Aggregator - Data aggregation logic for analytics

Provides aggregation functions for various analytics queries
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pymongo.database import Database
from pymongo import ASCENDING, DESCENDING
import logging

logger = logging.getLogger(__name__)


class DataAggregator:
    """
    Aggregates data from batch and speed layers for analytics
    """
    
    def __init__(self, db: Database):
        """
        Initialize aggregator
        
        Args:
            db: MongoDB database instance
        """
        self.db = db
        self.batch_views = db.batch_views
        self.speed_views = db.speed_views
    
    def aggregate_genre_stats(
        self,
        genre: str,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Aggregate statistics for a specific genre
        
        Args:
            genre: Genre name
            year: Filter by year (optional)
            month: Filter by month (optional)
        
        Returns:
            Aggregated genre statistics
        """
        pipeline = [
            {
                "$match": {
                    "view_type": "genre_analytics",
                    "data.genre": genre
                }
            }
        ]
        
        # Add time filters if provided
        if year:
            pipeline[0]["$match"]["data.year"] = year
        if month:
            pipeline[0]["$match"]["data.month"] = month
        
        # Aggregate statistics
        pipeline.extend([
            {
                "$group": {
                    "_id": None,
                    "total_movies": {"$sum": "$data.total_movies"},
                    "avg_rating": {"$avg": "$data.avg_rating"},
                    "avg_sentiment": {"$avg": "$data.avg_sentiment"},
                    "total_revenue": {"$sum": "$data.total_revenue"},
                    "avg_budget": {"$avg": "$data.avg_budget"},
                    "avg_runtime": {"$avg": "$data.avg_runtime"}
                }
            }
        ])
        
        result = list(self.batch_views.aggregate(pipeline))
        
        if result:
            return result[0]
        
        return {
            "total_movies": 0,
            "avg_rating": 0.0,
            "avg_sentiment": 0.0,
            "total_revenue": 0,
            "avg_budget": 0,
            "avg_runtime": 0
        }
    
    def aggregate_temporal_trends(
        self,
        movie_id: Optional[int] = None,
        genre: Optional[str] = None,
        metric: str = "rating",
        window_days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Aggregate temporal trends for rating, sentiment, or popularity
        
        Args:
            movie_id: Specific movie (optional)
            genre: Specific genre (optional)
            metric: Metric to aggregate (rating, sentiment, popularity)
            window_days: Time window in days
        
        Returns:
            List of daily aggregated data points
        """
        cutoff_date = datetime.utcnow() - timedelta(days=window_days)
        
        # Build match criteria
        match_criteria = {
            "view_type": "temporal_analytics",
            "computed_at": {"$gte": cutoff_date}
        }
        
        if movie_id:
            match_criteria["movie_id"] = movie_id
        if genre:
            match_criteria["data.genre"] = genre
        
        # Map metric to data field
        metric_field_map = {
            "rating": "data.avg_rating",
            "sentiment": "data.avg_sentiment",
            "popularity": "data.avg_popularity"
        }
        
        metric_field = metric_field_map.get(metric, "data.avg_rating")
        
        pipeline = [
            {"$match": match_criteria},
            {
                "$project": {
                    "date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$computed_at"
                        }
                    },
                    "value": metric_field,
                    "count": "$data.movie_count"
                }
            },
            {
                "$group": {
                    "_id": "$date",
                    "value": {"$avg": "$value"},
                    "count": {"$sum": "$count"}
                }
            },
            {"$sort": {"_id": 1}},
            {
                "$project": {
                    "_id": 0,
                    "date": "$_id",
                    "value": 1,
                    "count": 1
                }
            }
        ]
        
        return list(self.batch_views.aggregate(pipeline))
    
    def aggregate_top_movies(
        self,
        genre: Optional[str] = None,
        year: Optional[int] = None,
        metric: str = "rating",
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top movies by rating, revenue, or sentiment
        
        Args:
            genre: Filter by genre
            year: Filter by year
            metric: Sort metric (rating, revenue, sentiment)
            limit: Number of results
        
        Returns:
            List of top movies
        """
        match_criteria = {"view_type": "movie_details"}
        
        if genre:
            match_criteria["data.genres"] = genre
        if year:
            match_criteria["data.release_year"] = year
        
        # Map metric to sort field
        sort_field_map = {
            "rating": "data.vote_average",
            "revenue": "data.revenue",
            "sentiment": "data.avg_sentiment",
            "popularity": "data.popularity"
        }
        
        sort_field = sort_field_map.get(metric, "data.vote_average")
        
        pipeline = [
            {"$match": match_criteria},
            {"$sort": {sort_field: DESCENDING}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "movie_id": 1,
                    "title": "$data.title",
                    "rating": "$data.vote_average",
                    "revenue": "$data.revenue",
                    "sentiment": "$data.avg_sentiment",
                    "popularity": "$data.popularity",
                    "genres": "$data.genres"
                }
            }
        ]
        
        return list(self.batch_views.aggregate(pipeline))
    
    def calculate_trend_direction(self, data_points: List[Dict]) -> str:
        """
        Calculate trend direction from time series data
        
        Args:
            data_points: List of data points with 'date' and 'value'
        
        Returns:
            Trend direction: 'increasing', 'decreasing', or 'stable'
        """
        if len(data_points) < 2:
            return "stable"
        
        # Calculate linear regression slope
        n = len(data_points)
        sum_x = sum(range(n))
        sum_y = sum(point.get("value", 0) for point in data_points)
        sum_xy = sum(i * point.get("value", 0) for i, point in enumerate(data_points))
        sum_x2 = sum(i * i for i in range(n))
        
        # Slope = (n*sum_xy - sum_x*sum_y) / (n*sum_x2 - sum_x*sum_x)
        denominator = n * sum_x2 - sum_x * sum_x
        if denominator == 0:
            return "stable"
        
        slope = (n * sum_xy - sum_x * sum_y) / denominator
        
        # Classify based on slope
        if slope > 0.01:
            return "increasing"
        elif slope < -0.01:
            return "decreasing"
        else:
            return "stable"
    
    def aggregate_sentiment_breakdown(
        self,
        movie_id: int,
        window_hours: int = 48
    ) -> Dict[str, Any]:
        """
        Aggregate sentiment breakdown for a movie
        
        Args:
            movie_id: TMDB movie ID
            window_hours: Time window in hours
        
        Returns:
            Sentiment breakdown statistics
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=window_hours)
        
        # Query speed layer for recent data
        pipeline = [
            {
                "$match": {
                    "movie_id": movie_id,
                    "data_type": "sentiment",
                    "hour": {"$gte": cutoff_time}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "avg_sentiment": {"$avg": "$data.avg_sentiment"},
                    "total_reviews": {"$sum": "$data.review_count"},
                    "positive_count": {"$sum": "$data.positive_count"},
                    "negative_count": {"$sum": "$data.negative_count"},
                    "neutral_count": {"$sum": "$data.neutral_count"}
                }
            }
        ]
        
        result = list(self.speed_views.aggregate(pipeline))
        
        if result:
            data = result[0]
            total = data["total_reviews"]
            
            return {
                "overall_score": round(data["avg_sentiment"], 3),
                "label": self._classify_sentiment(data["avg_sentiment"]),
                "positive_count": data["positive_count"],
                "negative_count": data["negative_count"],
                "neutral_count": data["neutral_count"],
                "total_reviews": total,
                "positive_pct": round(data["positive_count"] / total * 100, 1) if total > 0 else 0,
                "negative_pct": round(data["negative_count"] / total * 100, 1) if total > 0 else 0,
                "neutral_pct": round(data["neutral_count"] / total * 100, 1) if total > 0 else 0
            }
        
        return {
            "overall_score": 0.0,
            "label": "unknown",
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0,
            "total_reviews": 0,
            "positive_pct": 0,
            "negative_pct": 0,
            "neutral_pct": 0
        }
    
    def _classify_sentiment(self, score: float) -> str:
        """
        Classify sentiment score into label
        
        Args:
            score: Sentiment score (-1 to 1)
        
        Returns:
            Label: positive, negative, or neutral
        """
        if score >= 0.2:
            return "positive"
        elif score <= -0.2:
            return "negative"
        else:
            return "neutral"
    
    def aggregate_trending_velocity(
        self,
        genre: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Calculate trending velocity (rate of popularity change)
        
        Args:
            genre: Filter by genre
            limit: Number of results
        
        Returns:
            Movies sorted by trending velocity
        """
        # Get recent popularity data from speed layer
        recent_cutoff = datetime.utcnow() - timedelta(hours=6)
        older_cutoff = datetime.utcnow() - timedelta(hours=12)
        
        match_criteria = {"data_type": "stats"}
        if genre:
            match_criteria["data.genre"] = genre
        
        pipeline = [
            {"$match": match_criteria},
            {
                "$group": {
                    "_id": "$movie_id",
                    "recent_popularity": {
                        "$avg": {
                            "$cond": [
                                {"$gte": ["$hour", recent_cutoff]},
                                "$data.popularity",
                                None
                            ]
                        }
                    },
                    "older_popularity": {
                        "$avg": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$lt": ["$hour", recent_cutoff]},
                                        {"$gte": ["$hour", older_cutoff]}
                                    ]
                                },
                                "$data.popularity",
                                None
                            ]
                        }
                    },
                    "title": {"$first": "$data.title"},
                    "genres": {"$first": "$data.genres"}
                }
            },
            {
                "$addFields": {
                    "velocity": {
                        "$subtract": ["$recent_popularity", "$older_popularity"]
                    }
                }
            },
            {"$sort": {"velocity": DESCENDING}},
            {"$limit": limit},
            {
                "$project": {
                    "_id": 0,
                    "movie_id": "$_id",
                    "title": 1,
                    "genres": 1,
                    "velocity": 1,
                    "recent_popularity": 1
                }
            }
        ]
        
        return list(self.speed_views.aggregate(pipeline))
