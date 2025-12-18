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
    
    Updated to work with 3 separate batch collections:
    - sentiment_baselines: Genre/franchise/yearly sentiment patterns
    - viral_thresholds: Genre/budget-tier/seasonal viral cutoffs
    - movie_intelligence: Individual movie competitive data
    """
    
    def __init__(self, db: Database):
        """
        Initialize query builder
        
        Args:
            db: MongoDB database instance
        """
        self.db = db
        # 3 separate batch collections (no more view_type discriminator)
        self.sentiment_baselines = db.sentiment_baselines
        self.viral_thresholds = db.viral_thresholds
        self.movie_intelligence = db.movie_intelligence
        # Speed layer collection
        self.speed_views = db.speed_views
    
    # --- Batch Layer Queries ---
    
    def get_sentiment_baselines(
        self,
        genre: Optional[str] = None,
        franchise: Optional[str] = None,
        year: Optional[int] = None
    ) -> List[Dict]:
        """
        Get sentiment baselines from batch layer (for PR Crisis Detection)
        
        Args:
            genre: Filter by genre
            franchise: Filter by franchise
            year: Filter by year
        
        Returns:
            List of sentiment baseline documents
        """
        query = {}
        
        if genre:
            query['genre'] = genre
        if franchise:
            query['franchise'] = franchise
        if year:
            query['year'] = year
        
        return list(self.sentiment_baselines.find(query).sort('batch_run_timestamp', DESCENDING))
    
    def get_viral_thresholds(
        self,
        genre: Optional[str] = None,
        budget_tier: Optional[str] = None,
        season: Optional[str] = None
    ) -> List[Dict]:
        """
        Get viral thresholds from batch layer (for Viral Content Detection)
        
        Args:
            genre: Filter by genre
            budget_tier: Filter by budget tier (low, medium, high)
            season: Filter by season (winter, spring, summer, fall)
        
        Returns:
            List of viral threshold documents
        """
        query = {}
        
        if genre:
            query['genre'] = genre
        if budget_tier:
            query['budget_tier'] = budget_tier
        if season:
            query['season'] = season
        
        return list(self.viral_thresholds.find(query).sort('batch_run_timestamp', DESCENDING))
    
    def get_batch_movie_by_id(self, movie_id: int) -> Optional[Dict]:
        """
        Get movie from movie_intelligence collection by ID
        
        Args:
            movie_id: TMDB movie ID
        
        Returns:
            Movie document or None
        """
        return self.movie_intelligence.find_one({'movie_id': movie_id})
    
    def get_movie_id_by_title(self, title: str) -> Optional[int]:
        """
        Get movie ID by exact title match from movie_intelligence collection
        
        Args:
            title: Movie title (case-insensitive)
        
        Returns:
            Movie ID or None if not found
        """
        # Try exact match (case-insensitive)
        movie = self.movie_intelligence.find_one({
            'title': {'$regex': f'^{title}$', '$options': 'i'}
        })
        
        if movie:
            return movie.get('movie_id')
        
        # Try partial match as fallback
        movie = self.movie_intelligence.find_one({
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
        
        # Get from movie_intelligence collection (no view_type filter needed)
        batch_data = self.movie_intelligence.find_one({
            'movie_id': movie_id,
            'batch_run_timestamp': {'$exists': True}
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
        Search movies with multiple filters from movie_intelligence collection
        
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
        # Build query for movie_intelligence collection
        search_query = {}
        
        # Text search on title
        if query:
            search_query['title'] = {'$regex': query, '$options': 'i'}
        
        # Genre filter (handle both flat 'genre' and array 'genres')
        if genre:
            search_query['$or'] = [
                {'genre': genre},
                {'genres': genre}
            ]
        
        # Year filter
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
        
        # Sort mapping for movie_intelligence collection
        sort_map = {
            'rating': ('vote_average', DESCENDING),
            'sentiment': ('avg_sentiment', DESCENDING),
            'viral_score': ('popularity', DESCENDING),
            'release_date': ('release_year', DESCENDING)
        }
        sort_field, sort_order = sort_map.get(sort_by, ('vote_average', DESCENDING))
        
        # Execute query on movie_intelligence collection
        cursor = self.movie_intelligence.find(search_query).sort(sort_field, sort_order)
        total = self.movie_intelligence.count_documents(search_query)
        
        results = list(cursor.skip(offset).limit(limit))
        
        return {
            'results': results,
            'total_results': total,
            'page': (offset // limit) + 1,
            'total_pages': (total + limit - 1) // limit,
            'limit': limit
        }
    
    # --- Recommendations Queries ---
    
    def get_batch_movies_for_recommendations(
        self,
        min_rating: float = 6.0,
        genre: Optional[str] = None,
        min_popularity: float = 1.0,
        min_vote_count: int = 50
    ) -> List[Dict]:
        """
        Get batch layer movies for dual-success recommendations
        
        Args:
            min_rating: Minimum vote_average threshold
            genre: Optional genre filter
            min_popularity: Minimum TMDB popularity
            min_vote_count: Minimum vote count for credibility
        
        Returns:
            List of movie documents from movie_intelligence collection
        """
        query = {
            'vote_average': {'$gte': min_rating},
            'popularity': {'$gte': min_popularity},
            'vote_count': {'$gte': min_vote_count}
        }
        
        if genre:
            # Handle both flat 'genre' and array 'genres'
            query['$or'] = [
                {'genre': genre},
                {'genres': genre}
            ]
        
        # Return all necessary fields for scoring
        projection = {
            'movie_id': 1,
            'title': 1,
            'genre': 1,
            'genres': 1,
            'vote_average': 1,
            'vote_count': 1,
            'popularity': 1,
            'release_year': 1,
            '_id': 0
        }
        
        return list(self.movie_intelligence.find(query, projection))
    
    def get_speed_layer_engagement(
        self,
        movie_titles: Optional[List[str]] = None,
        days_back: int = 30
    ) -> Dict[str, Dict]:
        """
        Get Reddit engagement from speed layer for dual-success recommendations
        
        Args:
            movie_titles: Optional list of movie titles to filter by
            days_back: How many days back to aggregate
        
        Returns:
            Dictionary mapping movie_title to aggregated engagement metrics
        """
        cutoff_time = datetime.utcnow() - timedelta(days=days_back)
        
        # Base match query
        match_query = {
            'window_start': {'$gte': cutoff_time}
        }
        
        if movie_titles:
            match_query['movie_title'] = {'$in': movie_titles}
        
        # Aggregation pipeline
        pipeline = [
            {'$match': match_query},
            {
                '$group': {
                    '_id': '$movie_title',
                    'total_upvotes': {'$sum': '$total_upvotes'},
                    'total_comments': {'$sum': '$total_comments'},
                    'total_awards': {'$sum': '$total_awards'},
                    'discussion_count': {'$sum': 1},
                    'last_window_start': {'$max': '$window_start'}
                }
            }
        ]
        
        results = list(self.speed_views.aggregate(pipeline))
        
        # Convert to dictionary for easy lookup
        engagement_map = {}
        for result in results:
            engagement_map[result['_id']] = {
                'total_upvotes': result.get('total_upvotes', 0),
                'total_comments': result.get('total_comments', 0),
                'total_awards': result.get('total_awards', 0),
                'discussion_count': result.get('discussion_count', 0),
                'last_window_start': result.get('last_window_start')
            }
        
        return engagement_map
    
    def get_movies_by_ids(self, movie_ids: List[int]) -> List[Dict]:
        """
        Get movies by their IDs from movie_intelligence collection
        
        Args:
            movie_ids: List of TMDB movie IDs
        
        Returns:
            List of movie documents
        """
        projection = {
            'movie_id': 1,
            'title': 1,
            'genre': 1,
            'director': 1,
            'franchise': 1,
            'budget_tier': 1,
            'release_year': 1,
            'vote_average': 1,
            'vote_count': 1,
            'popularity': 1,
            'avg_sentiment': 1,
            '_id': 0
        }
        
        return list(self.movie_intelligence.find(
            {'movie_id': {'$in': movie_ids}},
            projection
        ))
    
    def get_candidate_movies_for_similarity(
        self,
        exclude_ids: List[int],
        genres: Optional[List[str]] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        limit: int = 500
    ) -> List[Dict]:
        """
        Get candidate movies for similarity comparison
        
        Args:
            exclude_ids: Movie IDs to exclude (the input movies)
            genres: Optional list of genres to filter by
            year_min: Optional minimum release year
            year_max: Optional maximum release year
            limit: Maximum number of candidates to return
        
        Returns:
            List of candidate movie documents
        """
        query = {'movie_id': {'$nin': exclude_ids}}
        
        # Build OR conditions for genre/year filtering
        or_conditions = []
        
        if genres:
            or_conditions.append({'genre': {'$in': genres}})
        
        if year_min is not None and year_max is not None:
            or_conditions.append({
                'release_year': {
                    '$gte': year_min,
                    '$lte': year_max
                }
            })
        
        if or_conditions:
            query['$or'] = or_conditions
        
        projection = {
            'movie_id': 1,
            'title': 1,
            'genre': 1,
            'director': 1,
            'franchise': 1,
            'budget_tier': 1,
            'release_year': 1,
            'vote_average': 1,
            'vote_count': 1,
            'popularity': 1,
            'avg_sentiment': 1,
            '_id': 0
        }
        
        return list(self.movie_intelligence.find(query, projection).limit(limit))
    
    def get_reddit_buzz_data(
        self,
        genre: Optional[str] = None,
        days_back: int = 7
    ) -> List[Dict]:
        """
        Get Reddit buzz data from speed layer for reddit-buzz recommendations
        
        Args:
            genre: Optional genre filter
            days_back: How many days back to aggregate (default: 7)
        
        Returns:
            List of movies with aggregated Reddit metrics and timestamps
        """
        cutoff_time = datetime.utcnow() - timedelta(days=days_back)
        
        # Base match query - filter for reddit data types
        match_query = {
            'window_start': {'$gte': cutoff_time},
            'data_type': {'$in': ['reddit_post', 'reddit_comment']}
        }
        
        # Aggregation pipeline
        pipeline = [
            {'$match': match_query},
            {
                '$group': {
                    '_id': '$movie_title',
                    'total_upvotes': {'$sum': '$metrics.total_upvotes'},
                    'total_comments': {'$sum': '$metrics.total_comments'},
                    'total_awards': {'$sum': '$metrics.total_awards'},
                    'post_count': {'$sum': '$metrics.post_count'},
                    'last_window_start': {'$max': '$window_start'},
                    'viral_score': {'$max': '$metrics.viral_score'}
                }
            },
            {
                '$match': {
                    # Require at least some engagement
                    '$or': [
                        {'total_upvotes': {'$gt': 0}},
                        {'total_comments': {'$gt': 0}},
                        {'total_awards': {'$gt': 0}}
                    ]
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'movie_title': '$_id',
                    'total_upvotes': 1,
                    'total_comments': 1,
                    'total_awards': 1,
                    'post_count': 1,
                    'last_window_start': 1,
                    'viral_score': 1
                }
            }
        ]
        
        results = list(self.speed_views.aggregate(pipeline))
        
        # If genre filter is specified, filter by genre from batch layer
        if genre:
            movie_titles = [r['movie_title'] for r in results]
            genre_movies = self.movie_intelligence.find(
                {
                    'title': {'$in': movie_titles},
                    '$or': [
                        {'genre': genre},
                        {'genres': genre}
                    ]
                },
                {'title': 1, '_id': 0}
            )
            genre_titles = {m['title'] for m in genre_movies}
            results = [r for r in results if r['movie_title'] in genre_titles]
        
        return results
    
    def get_tmdb_quality_data(self, min_vote_count: int = 100, genre: Optional[str] = None) -> List[dict]:
        """
        Get movies from batch layer for TMDB quality ranking.
        
        Args:
            min_vote_count: Minimum vote count threshold (default 100)
            genre: Optional genre filter
            
        Returns:
            List of movie documents with TMDB metrics
        """
        query = {'vote_count': {'$gte': min_vote_count}}
        
        if genre:
            query['$or'] = [
                {'genre': genre},
                {'genres': genre}
            ]
        
        projection = {
            'movie_id': 1,
            'title': 1,
            'genre': 1,
            'genres': 1,
            'vote_average': 1,
            'vote_count': 1,
            'popularity': 1,
            'release_date': 1,
            '_id': 0
        }
        
        return list(self.movie_intelligence.find(query, projection))
    
    def get_mean_vote_average(self) -> float:
        """
        Calculate mean vote_average across all movies in database.
        Used for Bayesian average calculation.
        
        Returns:
            Mean vote average (C parameter)
        """
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'avg': {'$avg': '$vote_average'}
                }
            }
        ]
        
        result = list(self.movie_intelligence.aggregate(pipeline))
        return result[0]['avg'] if result else 7.0  # Default to 7.0 if no data

