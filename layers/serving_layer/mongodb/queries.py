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
    
    def get_movie_id_by_title(self, title: str) -> Optional[int]:
        """
        Get movie ID by exact title match
        
        Args:
            title: Movie title (case-insensitive)
        
        Returns:
            Movie ID or None if not found
        """
        # Try exact match first in old schema (case-insensitive)
        movie = self.batch_views.find_one({
            'view_type': 'movie_details',
            'data.title': {'$regex': f'^{title}$', '$options': 'i'}
        })
        
        if movie:
            return movie.get('movie_id')
        
        # Try exact match in new schema (movie_intelligence)
        movie = self.batch_views.find_one({
            'view_type': 'movie_intelligence',
            'title': {'$regex': f'^{title}$', '$options': 'i'}
        })
        
        if movie:
            return movie.get('movie_id')
        
        # Try partial match as fallback in old schema
        movie = self.batch_views.find_one({
            'view_type': 'movie_details',
            'data.title': {'$regex': title, '$options': 'i'}
        })
        
        if movie:
            return movie.get('movie_id')
        
        # Try partial match in new schema
        movie = self.batch_views.find_one({
            'view_type': 'movie_intelligence',
            'title': {'$regex': title, '$options': 'i'}
        })
        
        return movie.get('movie_id') if movie else None
    
    # --- Speed Layer Queries ---
    
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
        sort_by: str = 'rating',
        limit: int = 20,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Search movies with multiple filters (aligned with actual data schema)
        
        Args:
            query: Text search query
            genre: Genre filter
            year_from: Minimum year
            year_to: Maximum year
            rating_min: Minimum rating
            rating_max: Maximum rating
            sort_by: Sort field (rating, sentiment, viral_score, release_date)
            limit: Maximum results
            offset: Pagination offset
        
        Returns:
            Search results with pagination info
        """
        # Build query for movie_intelligence view
        search_query = {'view_type': 'movie_intelligence'}
        
        # Text search on title
        if query:
            search_query['title'] = {'$regex': query, '$options': 'i'}
        
        # Genre filter (handle both flat 'genre' and array 'genres')
        if genre:
            search_query['$or'] = [
                {'genre': genre},
                {'genres': genre}
            ]
        
        # Year filter (extract from release_year or release_date)
        if year_from or year_to:
            year_query = {}
            if year_from:
                year_query['$gte'] = year_from
            if year_to:
                year_query['$lte'] = year_to
            search_query['release_year'] = year_query
        
        # Rating filter
        if rating_min or rating_max:
            rating_query = {}
            if rating_min:
                rating_query['$gte'] = rating_min
            if rating_max:
                rating_query['$lte'] = rating_max
            search_query['vote_average'] = rating_query
        
        # Sort mapping for movie_intelligence view
        sort_map = {
            'rating': ('vote_average', DESCENDING),
            'sentiment': ('avg_sentiment', DESCENDING),
            'viral_score': ('popularity', DESCENDING),  # Use popularity as proxy
            'release_date': ('release_year', DESCENDING)
        }
        sort_field, sort_order = sort_map.get(sort_by, ('vote_average', DESCENDING))
        
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
