"""
MongoDB Query Builders

Pre-built queries for common operations across batch and speed views
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database
import logging

logger = logging.getLogger(__name__)


class MovieQueries:
    """
    Query builder for movie-related operations
    """
    
    def __init__(self, db: Database):
        """
        Initialize query builder
        
        Args:
            db: MongoDB database instance
        """
        self.db = db
        self.batch_views = db.batch_views
        self.speed_views = db.speed_views
    
    # --- Batch Layer Queries ---
    
    def get_batch_genre_analytics(
        self,
        genre: Optional[str] = None,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> List[Dict]:
        """
        Get genre analytics from batch layer
        
        Args:
            genre: Filter by genre
            year: Filter by year
            month: Filter by month
        
        Returns:
            List of genre analytics documents
        """
        query = {'view_type': 'genre_analytics'}
        
        if genre:
            query['genre'] = genre
        if year:
            query['year'] = year
        if month:
            query['month'] = month
        
        return list(self.batch_views.find(query).sort('computed_at', DESCENDING))
    
    def get_batch_movie_by_id(self, movie_id: int) -> Optional[Dict]:
        """
        Get movie from batch layer by ID
        
        Args:
            movie_id: TMDB movie ID
        
        Returns:
            Movie document or None
        """
        return self.batch_views.find_one({
            'movie_id': movie_id,
            'view_type': 'movie_details'
        })
    
    def get_batch_temporal_trends(
        self,
        metric: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Get temporal trends from batch layer
        
        Args:
            metric: Metric type (rating, sentiment, popularity)
            start_date: Start date filter
            end_date: End date filter
        
        Returns:
            List of temporal trend documents
        """
        query = {
            'view_type': 'temporal_trends',
            'data.metric': metric
        }
        
        if start_date:
            query['computed_at'] = {'$gte': start_date}
        if end_date:
            query.setdefault('computed_at', {})['$lte'] = end_date
        
        return list(self.batch_views.find(query).sort('computed_at', ASCENDING))
    
    # --- Speed Layer Queries ---
    
    def get_speed_movie_stats(
        self,
        movie_id: int,
        hours_back: int = 48
    ) -> List[Dict]:
        """
        Get real-time movie statistics from speed layer
        
        Args:
            movie_id: TMDB movie ID
            hours_back: How many hours back to query
        
        Returns:
            List of speed view documents
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        query = {
            'movie_id': movie_id,
            'data_type': 'stats',
            'hour': {'$gte': cutoff_time}
        }
        
        return list(self.speed_views.find(query).sort('hour', DESCENDING))
    
    def get_speed_sentiment(
        self,
        movie_id: Optional[int] = None,
        hours_back: int = 48
    ) -> List[Dict]:
        """
        Get real-time sentiment from speed layer
        
        Args:
            movie_id: TMDB movie ID (optional)
            hours_back: How many hours back to query
        
        Returns:
            List of sentiment documents
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        query = {
            'data_type': 'sentiment',
            'hour': {'$gte': cutoff_time}
        }
        
        if movie_id:
            query['movie_id'] = movie_id
        
        return list(self.speed_views.find(query).sort('hour', DESCENDING))
    
    def get_speed_trending(
        self,
        limit: int = 20,
        hours_back: int = 6
    ) -> List[Dict]:
        """
        Get trending movies from speed layer
        
        Args:
            limit: Maximum number of results
            hours_back: Time window for trending calculation
        
        Returns:
            List of trending movie documents
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        # Get recent stats and sort by popularity velocity
        pipeline = [
            {
                '$match': {
                    'data_type': 'stats',
                    'hour': {'$gte': cutoff_time}
                }
            },
            {
                '$sort': {'hour': -1}
            },
            {
                '$group': {
                    '_id': '$movie_id',
                    'latest_stats': {'$first': '$stats'},
                    'latest_hour': {'$first': '$hour'}
                }
            },
            {
                '$sort': {'latest_stats.rating_velocity': -1}
            },
            {
                '$limit': limit
            }
        ]
        
        return list(self.speed_views.aggregate(pipeline))
    
    # --- Combined Queries ---
    
    def get_movie_complete_view(
        self,
        movie_id: int,
        cutoff_hours: int = 48
    ) -> Dict[str, Any]:
        """
        Get complete movie view from both batch and speed layers
        
        Args:
            movie_id: TMDB movie ID
            cutoff_hours: Cutoff time for speed vs batch
        
        Returns:
            Combined movie data
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=cutoff_hours)
        
        # Get from batch layer (historical)
        batch_data = self.batch_views.find_one({
            'movie_id': movie_id,
            'computed_at': {'$lt': cutoff_time}
        })
        
        # Get from speed layer (recent)
        speed_data = list(self.speed_views.find({
            'movie_id': movie_id,
            'hour': {'$gte': cutoff_time}
        }).sort('hour', DESCENDING))
        
        return {
            'movie_id': movie_id,
            'batch_data': batch_data,
            'speed_data': speed_data,
            'cutoff_time': cutoff_time.isoformat()
        }
    
    # --- Search Queries ---
    
    def search_movies(
        self,
        query: Optional[str] = None,
        genre: Optional[str] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        rating_min: Optional[float] = None,
        rating_max: Optional[float] = None,
        sort_by: str = 'popularity',
        limit: int = 20,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Search movies with multiple filters
        
        Args:
            query: Text search query
            genre: Genre filter
            year_from: Minimum year
            year_to: Maximum year
            rating_min: Minimum rating
            rating_max: Maximum rating
            sort_by: Sort field (popularity, rating, release_date)
            limit: Maximum results
            offset: Pagination offset
        
        Returns:
            Search results with pagination info
        """
        # Build query
        search_query = {'view_type': 'genre_analytics'}
        
        if genre:
            search_query['genre'] = genre
        
        if year_from or year_to:
            search_query['year'] = {}
            if year_from:
                search_query['year']['$gte'] = year_from
            if year_to:
                search_query['year']['$lte'] = year_to
        
        if rating_min or rating_max:
            search_query['avg_rating'] = {}
            if rating_min:
                search_query['avg_rating']['$gte'] = rating_min
            if rating_max:
                search_query['avg_rating']['$lte'] = rating_max
        
        # Sort mapping
        sort_map = {
            'popularity': ('avg_popularity', DESCENDING),
            'rating': ('avg_rating', DESCENDING),
            'release_date': ('year', DESCENDING)
        }
        sort_field, sort_order = sort_map.get(sort_by, ('data.avg_popularity', DESCENDING))
        
        # Execute query
        cursor = self.batch_views.find(search_query).sort(sort_field, sort_order)
        total = self.batch_views.count_documents(search_query)
        
        results = list(cursor.skip(offset).limit(limit))
        
        return {
            'results': results,
            'total_results': total,
            'page': (offset // limit) + 1,
            'total_pages': (total + limit - 1) // limit,
            'limit': limit
        }
