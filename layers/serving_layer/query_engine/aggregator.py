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
    
    def get_sentiment_baseline(self, genre: str) -> Optional[Dict[str, Any]]:
        """
        Get sentiment baseline for a genre from batch layer
        
        Args:
            genre: Genre name
        
        Returns:
            Sentiment baseline document or None
        """
        try:
            result = self.batch_views.find_one({
                "view_type": "sentiment_baseline",
                "genre": genre
            })
            
            if result:
                return {
                    "genre": result.get("genre"),
                    "avg_sentiment": result.get("avg_sentiment", 0.0),
                    "sentiment_stddev": result.get("sentiment_stddev", 0.0),
                    "review_count": result.get("review_count", 0),
                    "positive_ratio": result.get("positive_ratio", 0.0),
                    "negative_ratio": result.get("negative_ratio", 0.0),
                    "sample_size": result.get("sample_size", 0)
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting sentiment baseline for genre {genre}: {e}")
            return None
    
    def get_viral_threshold(
        self, 
        genre: str, 
        budget_tier: Optional[str] = None,
        season: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get viral threshold for a genre from batch layer
        
        Args:
            genre: Genre name
            budget_tier: Budget tier (low, medium, high, blockbuster) - optional
            season: Season (spring, summer, fall, winter) - optional
        
        Returns:
            Viral threshold document or None
        """
        try:
            match_criteria = {
                "view_type": "viral_threshold",
                "genre": genre
            }
            
            if budget_tier:
                match_criteria["budget_tier"] = budget_tier
            if season:
                match_criteria["season"] = season
            
            result = self.batch_views.find_one(match_criteria)
            
            if result:
                return {
                    "genre": result.get("genre"),
                    "budget_tier": result.get("budget_tier"),
                    "season": result.get("season"),
                    "vote_velocity_p99": result.get("vote_velocity_p99", 0),
                    "vote_velocity_p95": result.get("vote_velocity_p95", 0),
                    "vote_velocity_p90": result.get("vote_velocity_p90", 0),
                    "comment_velocity_p99": result.get("comment_velocity_p99", 0),
                    "engagement_velocity_p99": result.get("engagement_velocity_p99", 0),
                    "sample_size": result.get("sample_size", 0)
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting viral threshold for genre {genre}: {e}")
            return None
    
    def get_movie_intelligence(self, movie_id: int) -> Optional[Dict[str, Any]]:
        """
        Get movie intelligence (TMDB metadata) from batch layer
        
        Args:
            movie_id: TMDB movie ID
        
        Returns:
            Movie intelligence document or None
        """
        try:
            result = self.batch_views.find_one({
                "view_type": "movie_intelligence",
                "movie_id": movie_id
            })
            
            if result and "data" in result:
                data = result["data"]
                return {
                    "movie_id": movie_id,
                    "title": data.get("title"),
                    "original_title": data.get("original_title"),
                    "release_date": data.get("release_date"),
                    "genres": data.get("genres", []),
                    "overview": data.get("overview"),
                    "vote_average": data.get("vote_average", 0.0),
                    "vote_count": data.get("vote_count", 0),
                    "popularity": data.get("popularity", 0.0),
                    "original_language": data.get("original_language"),
                    "production_companies": data.get("production_companies", []),
                    "production_countries": data.get("production_countries", []),
                    "runtime": data.get("runtime"),
                    "budget": data.get("budget"),
                    "revenue": data.get("revenue"),
                    "keywords": data.get("keywords", [])
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting movie intelligence for movie {movie_id}: {e}")
            return None
    
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
