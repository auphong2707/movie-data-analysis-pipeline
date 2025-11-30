"""
Query Router - Route queries to appropriate data sources

Routes queries based on timestamp and data freshness requirements
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from pymongo.database import Database
import logging

logger = logging.getLogger(__name__)


class QueryRouter:
    """
    Routes queries to batch or speed layer based on timestamp and freshness
    """
    
    def __init__(self, db: Database, cutoff_hours: int = 48):
        """
        Initialize query router
        
        Args:
            db: MongoDB database instance
            cutoff_hours: Cutoff time in hours for routing decision
        """
        self.db = db
        self.batch_views = db.batch_views
        self.speed_views = db.speed_views
        self.cutoff_hours = cutoff_hours
    
    def route_query(
        self,
        query_type: str,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Route query to appropriate data source
        
        Args:
            query_type: Type of query (movie, sentiment, trending, analytics)
            params: Query parameters
        
        Returns:
            Query result with routing metadata
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=self.cutoff_hours)
        
        # Determine routing strategy based on query type
        if query_type == 'trending':
            # Trending always uses speed layer (real-time)
            return self._route_to_speed(params, query_type)
        
        elif query_type == 'recent_sentiment':
            # Recent sentiment prioritizes speed layer
            return self._route_to_speed(params, query_type)
        
        elif query_type == 'historical_analytics':
            # Historical data uses batch layer
            return self._route_to_batch(params, query_type)
        
        elif query_type == 'movie_details':
            # Movie details can come from either, prefer fresh
            return self._route_hybrid(params, query_type, cutoff_time)
        
        else:
            # Default: hybrid approach
            return self._route_hybrid(params, query_type, cutoff_time)
    
    def _route_to_batch(
        self,
        params: Dict[str, Any],
        query_type: str
    ) -> Dict[str, Any]:
        """
        Route query to batch layer only
        
        Args:
            params: Query parameters
            query_type: Type of query
        
        Returns:
            Query result from batch layer
        """
        logger.info(f"Routing {query_type} query to batch layer")
        
        result = self._execute_batch_query(params, query_type)
        
        return {
            'data': result,
            'routing': {
                'source': 'batch',
                'reason': 'historical_data',
                'query_type': query_type
            }
        }
    
    def _route_to_speed(
        self,
        params: Dict[str, Any],
        query_type: str
    ) -> Dict[str, Any]:
        """
        Route query to speed layer only
        
        Args:
            params: Query parameters
            query_type: Type of query
        
        Returns:
            Query result from speed layer
        """
        logger.info(f"Routing {query_type} query to speed layer")
        
        result = self._execute_speed_query(params, query_type)
        
        return {
            'data': result,
            'routing': {
                'source': 'speed',
                'reason': 'real_time_data',
                'query_type': query_type
            }
        }
    
    def _route_hybrid(
        self,
        params: Dict[str, Any],
        query_type: str,
        cutoff_time: datetime
    ) -> Dict[str, Any]:
        """
        Route query to both batch and speed layers
        
        Args:
            params: Query parameters
            query_type: Type of query
            cutoff_time: Cutoff time for separation
        
        Returns:
            Merged query result from both layers
        """
        logger.info(f"Routing {query_type} query to hybrid (batch + speed)")
        
        # Execute both queries in parallel (conceptually)
        batch_result = self._execute_batch_query(params, query_type)
        speed_result = self._execute_speed_query(params, query_type)
        
        # Merge results
        merged_result = self._merge_results(batch_result, speed_result)
        
        return {
            'data': merged_result,
            'routing': {
                'source': 'hybrid',
                'reason': 'best_accuracy_and_freshness',
                'cutoff_time': cutoff_time.isoformat(),
                'batch_count': len(batch_result) if isinstance(batch_result, list) else 1,
                'speed_count': len(speed_result) if isinstance(speed_result, list) else 1,
                'query_type': query_type
            }
        }
    
    def _execute_batch_query(
        self,
        params: Dict[str, Any],
        query_type: str
    ) -> Any:
        """
        Execute query on batch layer
        
        Args:
            params: Query parameters
            query_type: Type of query
        
        Returns:
            Query result
        """
        # Build batch query based on type
        if query_type == 'movie_details':
            return self.batch_views.find_one({
                'movie_id': params.get('movie_id'),
                'view_type': 'movie_details'
            })
        
        elif query_type == 'historical_analytics':
            query = {'view_type': 'genre_analytics'}
            if 'genre' in params:
                query['genre'] = params['genre']
            if 'year' in params:
                query['year'] = params['year']
            return list(self.batch_views.find(query))
        
        else:
            # Generic query
            return list(self.batch_views.find(params))
    
    def _execute_speed_query(
        self,
        params: Dict[str, Any],
        query_type: str
    ) -> Any:
        """
        Execute query on speed layer
        
        Args:
            params: Query parameters
            query_type: Type of query
        
        Returns:
            Query result
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=self.cutoff_hours)
        
        # Build speed query based on type
        if query_type == 'trending':
            return self._execute_trending_query(params)
        
        elif query_type == 'recent_sentiment':
            return list(self.speed_views.find({
                'movie_id': params.get('movie_id'),
                'data_type': 'sentiment',
                'hour': {'$gte': cutoff_time}
            }).sort('hour', -1))
        
        elif query_type == 'movie_details':
            return list(self.speed_views.find({
                'movie_id': params.get('movie_id'),
                'data_type': 'stats',
                'hour': {'$gte': cutoff_time}
            }).sort('hour', -1).limit(1))
        
        else:
            # Generic query
            query = params.copy()
            query['hour'] = {'$gte': cutoff_time}
            return list(self.speed_views.find(query))
    
    def _execute_trending_query(self, params: Dict[str, Any]) -> List[Dict]:
        """
        Execute trending query on speed layer
        
        Args:
            params: Query parameters (limit, window_hours, genre)
        
        Returns:
            List of trending movies
        """
        limit = params.get('limit', 20)
        window_hours = params.get('window_hours', 6)
        cutoff_time = datetime.utcnow() - timedelta(hours=window_hours)
        
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
                '$project': {
                    'movie_id': '$_id',
                    'popularity': '$latest_stats.popularity',
                    'vote_average': '$latest_stats.vote_average',
                    'trending_score': {
                        '$add': [
                            {'$multiply': ['$latest_stats.popularity', 0.5]},
                            {'$multiply': ['$latest_stats.rating_velocity', 100]}
                        ]
                    }
                }
            },
            {
                '$sort': {'trending_score': -1}
            },
            {
                '$limit': limit
            }
        ]
        
        return list(self.speed_views.aggregate(pipeline))
    
    def _merge_results(self, batch_result: Any, speed_result: Any) -> Any:
        """
        Merge results from batch and speed layers
        
        Args:
            batch_result: Result from batch layer
            speed_result: Result from speed layer
        
        Returns:
            Merged result
        """
        # If speed result exists and is recent, prefer it
        if speed_result:
            if isinstance(speed_result, list) and len(speed_result) > 0:
                return speed_result[0]  # Most recent from speed
            return speed_result
        
        # Otherwise use batch result
        return batch_result
    
    def get_routing_stats(self) -> Dict[str, Any]:
        """
        Get routing statistics for monitoring
        
        Returns:
            Routing statistics
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=self.cutoff_hours)
        
        # Count documents in each layer
        batch_count = self.batch_views.count_documents({})
        speed_count = self.speed_views.count_documents({})
        
        # Count recent vs old in speed layer
        speed_recent = self.speed_views.count_documents({
            'hour': {'$gte': cutoff_time}
        })
        
        return {
            'cutoff_hours': self.cutoff_hours,
            'cutoff_time': cutoff_time.isoformat(),
            'batch_layer': {
                'total_documents': batch_count,
                'coverage': 'historical (> 48h)'
            },
            'speed_layer': {
                'total_documents': speed_count,
                'recent_documents': speed_recent,
                'coverage': 'recent (≤ 48h)'
            }
        }
