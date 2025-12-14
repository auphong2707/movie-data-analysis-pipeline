"""
View Merger - Merge Batch and Speed Layer Views

Implements the 48-hour cutoff strategy for merging batch accuracy 
with speed freshness in the Lambda Architecture.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pymongo.database import Database
import logging
import math

logger = logging.getLogger(__name__)


class ViewMerger:
    """
    Merges batch and speed layer views with 48-hour cutoff strategy
    
    Strategy:
    - Data older than 48 hours: Use batch layer (accurate, complete)
    - Data within 48 hours: Use speed layer (fresh, approximate)
    - On overlap: Speed layer takes precedence (fresher data)
    """
    
    def __init__(self, db: Database, cutoff_hours: int = 48):
        """
        Initialize view merger
        
        Args:
            db: MongoDB database instance
            cutoff_hours: Cutoff time in hours (default: 48)
        """
        self.db = db
        self.batch_views = db.batch_views
        self.speed_views = db.speed_views
        self.cutoff_hours = cutoff_hours
    
    def get_cutoff_time(self) -> datetime:
        """
        Calculate cutoff time for batch/speed separation
        
        Returns:
            Cutoff datetime (UTC)
        """
        return datetime.utcnow() - timedelta(hours=self.cutoff_hours)
    
    def merge_movie_views(
        self,
        movie_id: int
    ) -> Dict[str, Any]:
        """
        Merge batch and speed views for a specific movie
        
        Returns complete movie information by merging:
        - Batch: movie_details view (static metadata)
        - Speed: stats data_type (real-time stats)
        
        Args:
            movie_id: TMDB movie ID
        
        Returns:
            Complete movie information with both metadata and current stats
        """
        cutoff_time = self.get_cutoff_time()
        
        # Get movie_details from batch layer (static metadata)
        batch_details = self.batch_views.find_one({
            'movie_id': movie_id,
            'view_type': 'movie_details'
        })
        
        # Get latest stats from speed layer (real-time)
        speed_stats = self.speed_views.find_one(
            {
                'movie_id': movie_id,
                'data_type': 'stats',
                'hour': {'$gte': cutoff_time}
            },
            sort=[('hour', -1)]
        )
        
        if not batch_details and not speed_stats:
            return {'movie_id': movie_id, 'found': False}
        
        # Build response
        result = {
            'movie_id': movie_id,
            'found': True,
            'data_source': None,
            'last_updated': None
        }
        
        # Add metadata from batch layer
        if batch_details and 'data' in batch_details:
            data = batch_details['data']
            result.update({
                'title': data.get('title'),
                'release_date': data.get('release_date'),
                'genres': data.get('genres', []),
                'runtime': data.get('runtime'),
                'budget': data.get('budget'),
                'revenue': data.get('revenue'),
                'overview': data.get('overview'),
                'original_language': data.get('original_language')
            })
        
        # Add current stats - prefer speed layer if available
        if speed_stats and 'stats' in speed_stats:
            stats = speed_stats['stats']
            result.update({
                'vote_average': stats.get('vote_average'),
                'vote_count': stats.get('vote_count'),
                'popularity': stats.get('popularity'),
                'data_source': 'speed',
                'last_updated': speed_stats.get('hour')
            })
        elif batch_details and 'data' in batch_details:
            # Fallback to batch layer stats
            data = batch_details['data']
            result.update({
                'vote_average': data.get('vote_average'),
                'vote_count': data.get('vote_count'),
                'popularity': data.get('popularity'),
                'data_source': 'batch',
                'last_updated': batch_details.get('computed_at')
            })
        
        return result
    
    def merge_sentiment_views(
        self,
        movie_id: int,
        window: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Merge sentiment data from batch and speed layers
        
        Schema:
        - Batch: view_type='sentiment', data={avg_sentiment, review_count, positive_count, ...}
        - Speed: data_type='sentiment', data={avg_sentiment, review_count, sentiment_velocity, ...}
        
        Args:
            movie_id: TMDB movie ID
            window: Time window (7d, 30d, all)
        
        Returns:
            Merged sentiment analysis with breakdown
        """
        cutoff_time = self.get_cutoff_time()
        
        # Get movie title from batch layer
        movie_details = self.batch_views.find_one({
            'movie_id': movie_id,
            'view_type': 'movie_details'
        })
        
        title = movie_details.get('data', {}).get('title') if movie_details else None
        
        # Query batch layer for sentiment (historical > 48h)
        batch_sentiment = self.batch_views.find_one({
            'movie_id': movie_id,
            'view_type': 'sentiment'
        })
        
        # Query speed layer for recent sentiment (< 48h)
        speed_sentiment_docs = list(
            self.speed_views.find({
                'movie_id': movie_id,
                'data_type': 'sentiment',
                'hour': {'$gte': cutoff_time}
            }).sort('hour', -1)
        )
        
        # Calculate overall sentiment
        overall_sentiment = {}
        breakdown = []
        
        if batch_sentiment and 'data' in batch_sentiment:
            batch_data = batch_sentiment['data']
            # Schema: col1=avg_sentiment, col2=review_count, col3=positive_count, col4=negative_count, col5=neutral_count
            overall_sentiment = {
                'overall_score': batch_data.get('col1', 0),
                'label': self._get_sentiment_label(batch_data.get('col1', 0)),
                'positive_count': batch_data.get('col3', 0),
                'negative_count': batch_data.get('col4', 0),
                'neutral_count': batch_data.get('col5', 0),
                'total_reviews': batch_data.get('col2', 0),
                'velocity': 0,
                'confidence': 0.9
            }
        
        # Add speed layer sentiment (recent updates)
        if speed_sentiment_docs:
            latest_speed = speed_sentiment_docs[0]
            if 'data' in latest_speed:
                speed_data = latest_speed['data']
                # Schema: avg_sentiment, review_count, positive_count, negative_count, neutral_count, sentiment_velocity
                
                # Update with recent data
                total_reviews = overall_sentiment.get('total_reviews', 0) + speed_data.get('review_count', 0)
                
                # Weighted average of sentiments
                if overall_sentiment and total_reviews > 0:
                    batch_weight = overall_sentiment.get('total_reviews', 0) / total_reviews
                    speed_weight = speed_data.get('review_count', 0) / total_reviews
                    
                    overall_score = (
                        overall_sentiment.get('overall_score', 0) * batch_weight +
                        speed_data.get('avg_sentiment', 0) * speed_weight
                    )
                else:
                    overall_score = speed_data.get('avg_sentiment', 0)
                
                overall_sentiment.update({
                    'overall_score': overall_score,
                    'label': self._get_sentiment_label(overall_score),
                    'positive_count': overall_sentiment.get('positive_count', 0) + speed_data.get('positive_count', 0),
                    'negative_count': overall_sentiment.get('negative_count', 0) + speed_data.get('negative_count', 0),
                    'neutral_count': overall_sentiment.get('neutral_count', 0) + speed_data.get('neutral_count', 0),
                    'total_reviews': total_reviews,
                    'velocity': speed_data.get('sentiment_velocity', 0),
                    'confidence': 0.85
                })
            
            # Create breakdown from speed layer hourly data
            for doc in speed_sentiment_docs:
                if 'data' in doc and 'hour' in doc:
                    breakdown.append({
                        'date': doc['hour'].strftime('%Y-%m-%d') if isinstance(doc['hour'], datetime) else doc['hour'],
                        'avg_sentiment': doc['data'].get('avg_sentiment', 0),
                        'review_count': doc['data'].get('review_count', 0)
                    })
        
        return {
            'movie_id': movie_id,
            'title': title,
            'sentiment': overall_sentiment,
            'breakdown': breakdown,
            'data_sources': {
                'batch': cutoff_time.isoformat() if batch_sentiment else None,
                'speed': datetime.utcnow().isoformat() if speed_sentiment_docs else None
            }
        }
    
    def _get_sentiment_label(self, score: float) -> str:
        """Convert sentiment score to label"""
        if score > 0.3:
            return 'positive'
        elif score < -0.3:
            return 'negative'
        else:
            return 'neutral'
    
    def merge_trending_views(
        self,
        genre: Optional[str] = None,
        limit: int = 20,
        window_hours: int = 6
    ) -> Dict[str, Any]:
        """
        Get trending movies from speed layer with real-time stats
        
        Calculates trending_score based on:
        - Popularity (weight: 0.4)
        - Rating velocity (weight: 0.3)
        - Vote average (weight: 0.2)
        - Vote count growth (weight: 0.1)
        
        Args:
            genre: Optional genre filter
            limit: Maximum number of results
            window_hours: Time window for trending calculation
        
        Returns:
            List of trending movies with metadata
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=window_hours)
        
        # Aggregate trending from speed layer
        pipeline = [
            {'$match': {
                'data_type': 'stats',
                'hour': {'$gte': cutoff_time}
            }},
            {'$sort': {'hour': 1}},  # Sort ascending to get earliest first
            # Get stats per movie
            {'$group': {
                '_id': '$movie_id',
                'earliest_stats': {'$first': '$stats'},
                'earliest_hour': {'$first': '$hour'},
                'latest_stats': {'$last': '$stats'},
                'latest_hour': {'$last': '$hour'},
                'data_points': {'$sum': 1}
            }},
            # Calculate trending score and velocity
            {'$project': {
                'movie_id': '$_id',
                'popularity': '$latest_stats.popularity',
                'vote_average': '$latest_stats.vote_average',
                'vote_count': '$latest_stats.vote_count',
                'rating_velocity': '$latest_stats.rating_velocity',
                'popularity_velocity': {
                    '$divide': [
                        {
                            '$subtract': [
                                '$latest_stats.popularity',
                                '$earliest_stats.popularity'
                            ]
                        },
                        {'$max': ['$data_points', 1]}
                    ]
                },
                'trending_score': {
                    '$add': [
                        # Popularity contribution (40%)
                        {'$multiply': [
                            {'$ifNull': ['$latest_stats.popularity', 0]},
                            0.4
                        ]},
                        # Rating velocity contribution (30%)
                        {'$multiply': [
                            {'$ifNull': ['$latest_stats.rating_velocity', 0]},
                            300
                        ]},
                        # Vote average contribution (20%)
                        {'$multiply': [
                            {'$ifNull': ['$latest_stats.vote_average', 0]},
                            2
                        ]},
                        # Vote velocity contribution (10%)
                        {'$multiply': [
                            {'$ifNull': ['$latest_stats.vote_velocity', 0]},
                            0.01
                        ]}
                    ]
                },
                'latest_hour': 1
            }},
            {'$sort': {'trending_score': -1}},
            {'$limit': limit}
        ]
        
        trending_movies = list(self.speed_views.aggregate(pipeline))
        
        # Enrich with metadata from batch layer
        enriched_movies = []
        for rank, movie in enumerate(trending_movies, 1):
            batch_metadata = self.batch_views.find_one({
                'movie_id': movie['movie_id'],
                'view_type': 'movie_details'
            })
            
            enriched = {
                'rank': rank,
                'movie_id': movie['movie_id'],
                'title': 'Unknown',
                'genres': [],
                'trending_score': round(movie.get('trending_score', 0), 2),
                'velocity': round(movie.get('popularity_velocity', 0), 2),
                'acceleration': round(movie.get('rating_velocity', 0), 3),
                'popularity': round(movie.get('popularity', 0), 2),
                'vote_average': round(movie.get('vote_average', 0), 2),
                'vote_count': movie.get('vote_count', 0)
            }
            
            if batch_metadata and 'data' in batch_metadata:
                data = batch_metadata['data']
                enriched['title'] = data.get('title', 'Unknown')
                enriched['genres'] = data.get('genres', [])
                
                # Apply genre filter if specified
                if genre and genre not in enriched['genres']:
                    continue
            
            enriched_movies.append(enriched)
        
        return {
            'trending_movies': enriched_movies[:limit],
            'generated_at': datetime.utcnow().isoformat(),
            'window': f'{window_hours}h',
            'data_source': 'speed'
        }
    
    def merge_analytics_views(
        self,
        genre: str,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get genre analytics from batch layer
        
        Schema: view_type='genre_analytics'
        Fields: genre, year, month, total_movies, avg_rating, avg_sentiment, 
                avg_popularity, total_revenue, top_movies
        
        Args:
            genre: Genre to analyze
            year: Optional year filter
            month: Optional month filter
        
        Returns:
            Genre analytics with statistics and top movies
        """
        # Get batch layer analytics
        query = {
            'view_type': 'genre_analytics',
            'genre': {'$regex': f'^{genre}$', '$options': 'i'}
        }
        
        if year:
            query['year'] = year
        if month:
            query['month'] = month
        
        batch_analytics = self.batch_views.find_one(
            query,
            sort=[('computed_at', -1)]
        )
        
        if not batch_analytics:
            return {
                'genre': genre,
                'year': year,
                'month': month,
                'statistics': {},
                'top_movies': [],
                'trends': {},
                'data_source': 'batch',
                'found': False
            }
        
        # Calculate avg_sentiment from sentiment collection
        genre_movies_query = {
            'view_type': 'movie_details',
            'data.genres': genre
        }
        
        # Apply year and month filters to release_date
        if year and month:
            # Filter by YYYY-MM format
            month_str = f"{year}-{month:02d}"
            genre_movies_query['data.release_date'] = {'$regex': f'^{month_str}'}
        elif year:
            # Filter by YYYY format only
            genre_movies_query['data.release_date'] = {'$regex': f'^{year}'}
        
        genre_movie_ids = [m['movie_id'] for m in self.batch_views.find(genre_movies_query, {'movie_id': 1})]
        
        # Aggregate sentiment: col1 = avg_sentiment, col2 = review_count
        sentiment_pipeline = [
            {'$match': {
                'view_type': 'sentiment',
                'movie_id': {'$in': genre_movie_ids},
                'data.col1': {'$ne': None, '$exists': True}
            }},
            {'$group': {
                '_id': None,
                'avg_sentiment': {'$avg': '$data.col1'},
                'total_reviews': {'$sum': '$data.col2'}
            }}
        ]
        
        sentiment_result = list(self.batch_views.aggregate(sentiment_pipeline))
        calculated_avg_sentiment = sentiment_result[0]['avg_sentiment'] if sentiment_result else None
        
        # Get ALL movies matching filters (remove limit to get complete list)
        top_movies_cursor = self.batch_views.find(
            genre_movies_query
        ).sort('data.vote_average', -1)
        
        top_movies = []
        for movie in top_movies_cursor:
            data = movie.get('data', {})
            top_movies.append({
                'movie_id': movie.get('movie_id'),
                'title': data.get('title', 'Unknown'),
                'vote_average': round(data.get('vote_average', 0), 2),
                'popularity': round(data.get('popularity', 0), 2),
                'vote_count': data.get('vote_count', 0),
                'release_date': data.get('release_date')
            })
        
        # Extract statistics
        # total_movies always equals actual count of movies in top_movies
        total_movies_count = len(top_movies)
        
        result = {
            'genre': genre,
            'year': batch_analytics.get('year'),
            'month': batch_analytics.get('month'),
            'statistics': {
                'total_movies': total_movies_count,
                'avg_rating': round(batch_analytics.get('avg_rating', 0), 2),
                'avg_sentiment': round(calculated_avg_sentiment, 3) if calculated_avg_sentiment is not None else None,
                'avg_popularity': round(batch_analytics.get('avg_popularity', 0), 2),
                'total_revenue': batch_analytics.get('total_revenue'),
                'avg_budget': batch_analytics.get('avg_budget'),
                'avg_runtime': batch_analytics.get('avg_runtime')
            },
            'top_movies': top_movies,
            'trends': {
                'rating_trend': 'stable',
                'sentiment_trend': 'stable',
                'popularity_trend': 'stable'
            },
            'data_source': 'batch'
        }
        
        return result
    
    def get_temporal_trends(
        self,
        metric: str,
        movie_id: Optional[int] = None,
        genre: Optional[str] = None,
        window: str = '30d'
    ) -> Dict[str, Any]:
        """
        Get temporal trends from batch layer
        
        Schema: view_type='temporal_trends'
        data: {metric, col2 (value), col3 (count), date}
        
        Args:
            metric: rating, sentiment, or popularity
            movie_id: Optional movie filter
            genre: Optional genre filter  
            window: Time window (7d, 30d, 90d)
        
        Returns:
            Time-series data points with summary statistics
        """
        # Parse window
        days = int(window.replace('d', ''))
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # Build query
        query = {
            'view_type': 'temporal_trends',
            'data.metric': metric,
            'data.date': {'$gte': start_date}
        }
        
        # Note: movie_id and genre filters not available in current schema
        
        # Get trends from batch layer
        trends = list(
            self.batch_views.find(query).sort('data.date', 1).limit(1000)
        )
        
        # Extract data points
        data_points = []
        values = []
        
        for doc in trends:
            if 'data' in doc:
                data = doc['data']
                value = data.get('col2', 0)  # col2 is the value
                count = data.get('col3', 0)  # col3 is the count
                point = {
                    'date': data.get('date'),
                    'value': round(value, 3) if value else 0,
                    'count': int(count) if count else 0
                }
                data_points.append(point)
                if value:
                    values.append(value)
        
        # Calculate summary statistics
        summary = {}
        if values:
            summary = {
                'avg': round(sum(values) / len(values), 3),
                'min': round(min(values), 3),
                'max': round(max(values), 3),
                'trend': self._calculate_trend_direction(values),
                'change_rate': self._calculate_change_rate(values, days)
            }
        else:
            summary = {
                'avg': 0,
                'min': 0,
                'max': 0,
                'trend': 'no_data',
                'change_rate': 0
            }
        
        return {
            'metric': metric,
            'window': window,
            'movie_id': movie_id,
            'genre': genre,
            'data_points': data_points,
            'summary': summary
        }
    
    def _calculate_trend_direction(self, values: List[float]) -> str:
        """Calculate trend direction from values"""
        if len(values) < 2:
            return 'stable'
        
        # Simple linear trend
        first_third = sum(values[:len(values)//3]) / (len(values)//3)
        last_third = sum(values[-len(values)//3:]) / (len(values)//3)
        
        change = (last_third - first_third) / first_third if first_third != 0 else 0
        
        if change > 0.05:
            return 'increasing'
        elif change < -0.05:
            return 'decreasing'
        else:
            return 'stable'
    
    def _calculate_change_rate(self, values: List[float], days: int) -> float:
        """Calculate change rate per day"""
        if len(values) < 2:
            return 0.0
        
        total_change = values[-1] - values[0]
        return round(total_change / days, 6)
        
        if batch_analytics:
            result['statistics'] = {
                'total_movies': batch_analytics.get('total_movies', 0),
                'avg_rating': batch_analytics.get('avg_rating', 0),
                'avg_sentiment': batch_analytics.get('avg_sentiment', 0),
                'avg_popularity': batch_analytics.get('avg_popularity', 0),
            }
            result['data_sources']['batch'] = batch_analytics.get('computed_at')
        
        # Add speed layer adjustments
        if speed_stats:
            result['statistics']['recent_avg_rating'] = speed_stats.get('avg_rating')
            result['statistics']['recent_avg_popularity'] = speed_stats.get('avg_popularity')
            result['data_sources']['speed'] = datetime.utcnow().isoformat()
        
        return result
    
    # --- Helper Methods ---
    
    def _merge_data_points(
        self,
        batch_data: List[Dict],
        speed_data: List[Dict]
    ) -> List[Dict]:
        """
        Merge batch and speed data points, speed takes precedence
        
        Args:
            batch_data: List of batch layer documents
            speed_data: List of speed layer documents
        
        Returns:
            Merged and deduplicated list
        """
        # Create a dict with speed data (takes precedence)
        merged = {}
        
        # Add speed data first
        for doc in speed_data:
            timestamp = doc.get('hour')
            if timestamp:
                merged[timestamp] = {
                    'timestamp': timestamp,
                    'data': doc,
                    'source': 'speed'
                }
        
        # Add batch data for non-overlapping times
        for doc in batch_data:
            timestamp = doc.get('computed_at')
            if timestamp and timestamp not in merged:
                merged[timestamp] = {
                    'timestamp': timestamp,
                    'data': doc,
                    'source': 'batch'
                }
        
        # Sort by timestamp (newest first)
        return sorted(
            merged.values(),
            key=lambda x: x['timestamp'],
            reverse=True
        )
    
    def _calculate_overall_sentiment(
        self,
        batch_sentiment: List[Dict],
        speed_sentiment: List[Dict]
    ) -> Dict[str, Any]:
        """
        Calculate overall sentiment from batch and speed data
        
        Args:
            batch_sentiment: Batch sentiment documents
            speed_sentiment: Speed sentiment documents
        
        Returns:
            Overall sentiment metrics
        """
        # Prefer speed layer for most recent sentiment
        if speed_sentiment:
            latest = speed_sentiment[0]
            return {
                'overall_score': latest.get('data', {}).get('avg_sentiment', 0),
                'label': self._get_sentiment_label(
                    latest.get('data', {}).get('avg_sentiment', 0)
                ),
                'positive_count': latest.get('data', {}).get('positive_count', 0),
                'negative_count': latest.get('data', {}).get('negative_count', 0),
                'neutral_count': latest.get('data', {}).get('neutral_count', 0),
                'total_reviews': latest.get('data', {}).get('review_count', 0),
                'velocity': latest.get('data', {}).get('sentiment_velocity', 0),
                'source': 'speed'
            }
        elif batch_sentiment:
            latest = batch_sentiment[0]
            return {
                'overall_score': latest.get('data', {}).get('avg_sentiment', 0),
                'label': self._get_sentiment_label(
                    latest.get('data', {}).get('avg_sentiment', 0)
                ),
                'source': 'batch'
            }
        else:
            return {
                'overall_score': 0,
                'label': 'neutral',
                'source': 'none'
            }
    
    def _create_sentiment_breakdown(
        self,
        batch_sentiment: List[Dict],
        speed_sentiment: List[Dict]
    ) -> List[Dict]:
        """
        Create daily sentiment breakdown
        
        Args:
            batch_sentiment: Batch sentiment documents
            speed_sentiment: Speed sentiment documents
        
        Returns:
            List of daily sentiment breakdowns
        """
        breakdown = []
        
        # Add speed data (recent)
        for doc in speed_sentiment:
            breakdown.append({
                'date': doc.get('hour'),
                'avg_sentiment': doc.get('data', {}).get('avg_sentiment', 0),
                'review_count': doc.get('data', {}).get('review_count', 0),
                'source': 'speed'
            })
        
        # Add batch data (historical)
        for doc in batch_sentiment:
            breakdown.append({
                'date': doc.get('computed_at'),
                'avg_sentiment': doc.get('data', {}).get('avg_sentiment', 0),
                'review_count': doc.get('data', {}).get('review_count', 0),
                'source': 'batch'
            })
        
        # Sort by date (newest first)
        breakdown.sort(key=lambda x: x['date'], reverse=True)
        
        return breakdown
    
    @staticmethod
    def _get_sentiment_label(score: float) -> str:
        """
        Convert sentiment score to label
        
        Args:
            score: Sentiment score (-1 to 1)
        
        Returns:
            Sentiment label (positive/neutral/negative)
        """
        if score > 0.05:
            return 'positive'
        elif score < -0.05:
            return 'negative'
        else:
            return 'neutral'
    
    def merge_sentiment_data(
        self,
        movie_title: str,
        window_hours: int = 48,
        crisis_threshold_sigma: float = 3.0
    ) -> Dict[str, Any]:
        """
        Enhanced sentiment analysis with crisis detection (Business Goal #1)
        
        Compares current Reddit sentiment against TMDB historical baselines
        to detect PR crises using statistical deviation.
        
        Args:
            movie_title: Movie title to analyze
            window_hours: Time window for speed layer data (default: 48h)
            crisis_threshold_sigma: Number of standard deviations for crisis (default: 3.0)
        
        Returns:
            Comprehensive sentiment analysis with crisis detection
        """
        cutoff_time = self.get_cutoff_time()
        
        result = {
            "movie_title": movie_title,
            "current_sentiment": None,
            "genre_baseline": None,
            "franchise_baseline": None,
            "crisis_detection": {
                "is_crisis": False,
                "severity": "none",
                "deviation_sigma": 0.0,
                "message": None
            },
            "sentiment_velocity": None,
            "breakdown": [],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Step 1: Get current Reddit sentiment from speed layer
        speed_posts = list(self.speed_views.find({
            "movie_title": movie_title,
            "data_type": "reddit_post",
            "hour": {"$gte": cutoff_time}
        }).sort("hour", -1).limit(50))
        
        if not speed_posts:
            result["crisis_detection"]["message"] = "No recent Reddit data available"
            return result
        
        # Aggregate current sentiment from Reddit posts
        total_sentiment = 0
        total_upvotes = 0
        sentiment_by_hour = {}
        
        for post in speed_posts:
            metrics = post.get("metrics", {})
            sentiment = metrics.get("sentiment_score", 0)
            upvotes = metrics.get("upvotes", 0)
            hour = post.get("hour")
            
            total_sentiment += sentiment * upvotes  # Weight by upvotes
            total_upvotes += upvotes
            
            # Track hourly sentiment for velocity calculation
            hour_key = hour.strftime("%Y-%m-%d %H:00") if isinstance(hour, datetime) else str(hour)
            if hour_key not in sentiment_by_hour:
                sentiment_by_hour[hour_key] = {"sentiment": 0, "count": 0}
            sentiment_by_hour[hour_key]["sentiment"] += sentiment
            sentiment_by_hour[hour_key]["count"] += 1
        
        current_sentiment = total_sentiment / total_upvotes if total_upvotes > 0 else 0
        result["current_sentiment"] = {
            "score": round(current_sentiment, 3),
            "label": self._get_sentiment_label(current_sentiment),
            "sample_size": len(speed_posts),
            "total_upvotes": total_upvotes
        }
        
        # Step 2: Calculate sentiment velocity (6-hour window)
        if len(sentiment_by_hour) >= 2:
            sorted_hours = sorted(sentiment_by_hour.items())
            recent_hours = sorted_hours[-6:]  # Last 6 hours
            
            if len(recent_hours) >= 2:
                first_sentiment = recent_hours[0][1]["sentiment"] / recent_hours[0][1]["count"]
                last_sentiment = recent_hours[-1][1]["sentiment"] / recent_hours[-1][1]["count"]
                velocity = last_sentiment - first_sentiment
                
                result["sentiment_velocity"] = {
                    "value": round(velocity, 3),
                    "direction": "increasing" if velocity > 0 else ("decreasing" if velocity < 0 else "stable"),
                    "window_hours": len(recent_hours)
                }
        
        # Step 3: Get genre baseline from batch layer
        # First, get movie intelligence to find genre
        movie_intel = self.batch_views.find_one({
            "view_type": "movie_intelligence",
            "data.title": movie_title
        })
        
        if movie_intel and "data" in movie_intel:
            genres = movie_intel["data"].get("genres", [])
            
            if genres:
                primary_genre = genres[0] if isinstance(genres, list) else genres
                
                # Get genre sentiment baseline
                genre_baseline = self.batch_views.find_one({
                    "view_type": "sentiment_baseline",
                    "genre": primary_genre
                })
                
                if genre_baseline:
                    baseline_sentiment = genre_baseline.get("avg_sentiment", 0)
                    baseline_stddev = genre_baseline.get("sentiment_stddev", 0.15)
                    
                    result["genre_baseline"] = {
                        "genre": primary_genre,
                        "avg_sentiment": round(baseline_sentiment, 3),
                        "stddev": round(baseline_stddev, 3),
                        "sample_size": genre_baseline.get("sample_size", 0)
                    }
                    
                    # Step 4: Crisis Detection - Calculate deviation
                    deviation = current_sentiment - baseline_sentiment
                    sigma_deviation = abs(deviation) / baseline_stddev if baseline_stddev > 0 else 0
                    
                    result["crisis_detection"]["deviation_sigma"] = round(sigma_deviation, 2)
                    
                    # Determine crisis severity
                    if deviation < 0 and sigma_deviation >= crisis_threshold_sigma:
                        result["crisis_detection"]["is_crisis"] = True
                        result["crisis_detection"]["severity"] = "critical"
                        result["crisis_detection"]["message"] = (
                            f"CRITICAL: Sentiment dropped {abs(deviation):.2f} "
                            f"({sigma_deviation:.1f}σ below baseline). Immediate PR response needed."
                        )
                    elif deviation < 0 and sigma_deviation >= (crisis_threshold_sigma * 0.67):
                        result["crisis_detection"]["is_crisis"] = True
                        result["crisis_detection"]["severity"] = "warning"
                        result["crisis_detection"]["message"] = (
                            f"WARNING: Sentiment {abs(deviation):.2f} below baseline "
                            f"({sigma_deviation:.1f}σ). Monitor closely."
                        )
                    else:
                        result["crisis_detection"]["message"] = f"Sentiment within normal range ({sigma_deviation:.1f}σ)"
        
        # Step 5: Get franchise baseline (if applicable)
        if movie_intel and "data" in movie_intel:
            franchise_info = movie_intel["data"].get("belongs_to_collection")
            if franchise_info:
                franchise_name = franchise_info.get("name") if isinstance(franchise_info, dict) else None
                
                if franchise_name:
                    franchise_baseline = self.batch_views.find_one({
                        "view_type": "sentiment_baseline",
                        "franchise": franchise_name
                    })
                    
                    if franchise_baseline:
                        result["franchise_baseline"] = {
                            "franchise": franchise_name,
                            "avg_sentiment": round(franchise_baseline.get("avg_sentiment", 0), 3),
                            "stddev": round(franchise_baseline.get("sentiment_stddev", 0.15), 3)
                        }
        
        # Step 6: Create sentiment breakdown (hourly)
        for hour_key, data in sorted(sentiment_by_hour.items()):
            result["breakdown"].append({
                "hour": hour_key,
                "avg_sentiment": round(data["sentiment"] / data["count"], 3),
                "sample_size": data["count"]
            })
        
        return result
    
    def merge_viral_data(
        self,
        movie_title: Optional[str] = None,
        genre: Optional[str] = None,
        limit: int = 20,
        viral_coefficient_threshold: float = 1.0
    ) -> Dict[str, Any]:
        """
        Viral content detection with threshold comparison (Business Goal #2)
        
        Identifies viral content by comparing current Reddit engagement velocity
        against historical viral thresholds from batch layer.
        
        Args:
            movie_title: Specific movie to analyze (optional)
            genre: Filter by genre (optional)
            limit: Number of results to return
            viral_coefficient_threshold: Minimum viral coefficient (default: 1.0)
        
        Returns:
            List of viral movies with engagement metrics and viral coefficients
        """
        cutoff_time = self.get_cutoff_time()
        
        result = {
            "viral_movies": [],
            "total_trending": 0,
            "threshold_used": viral_coefficient_threshold,
            "window_hours": self.cutoff_hours,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Step 1: Query speed layer for Reddit engagement data
        match_criteria = {
            "data_type": "reddit_post",
            "hour": {"$gte": cutoff_time}
        }
        
        if movie_title:
            match_criteria["movie_title"] = movie_title
        
        # Aggregate Reddit metrics by movie
        pipeline = [
            {"$match": match_criteria},
            {
                "$group": {
                    "_id": "$movie_title",
                    "total_upvotes": {"$sum": "$metrics.upvotes"},
                    "total_comments": {"$sum": "$metrics.num_comments"},
                    "total_awards": {"$sum": "$metrics.awards"},
                    "avg_sentiment": {"$avg": "$metrics.sentiment_score"},
                    "post_count": {"$sum": 1},
                    "subreddits": {"$addToSet": "$subreddit"},
                    "earliest_hour": {"$min": "$hour"},
                    "latest_hour": {"$max": "$hour"}
                }
            }
        ]
        
        reddit_aggregated = list(self.speed_views.aggregate(pipeline))
        
        if not reddit_aggregated:
            result["message"] = "No recent Reddit data found"
            return result
        
        # Step 2: Calculate velocities and match with viral thresholds
        viral_movies = []
        
        for movie_data in reddit_aggregated:
            movie_title_key = movie_data["_id"]
            
            # Calculate time span in hours
            earliest = movie_data["earliest_hour"]
            latest = movie_data["latest_hour"]
            time_span_hours = (latest - earliest).total_seconds() / 3600 if isinstance(earliest, datetime) else 1
            time_span_hours = max(time_span_hours, 1)  # Avoid division by zero
            
            # Calculate velocities (per hour)
            upvote_velocity = movie_data["total_upvotes"] / time_span_hours
            comment_velocity = movie_data["total_comments"] / time_span_hours
            award_velocity = movie_data["total_awards"] / time_span_hours
            
            # Step 3: Get movie intelligence for genre
            movie_intel = self.batch_views.find_one({
                "view_type": "movie_intelligence",
                "data.title": movie_title_key
            })
            
            if not movie_intel:
                continue
            
            movie_genres = movie_intel.get("data", {}).get("genres", [])
            primary_genre = movie_genres[0] if isinstance(movie_genres, list) and movie_genres else "Unknown"
            
            # Apply genre filter if specified
            if genre and primary_genre != genre:
                continue
            
            # Step 4: Lookup viral threshold from batch layer
            viral_threshold = self.batch_views.find_one({
                "view_type": "viral_threshold",
                "genre": primary_genre
            })
            
            if not viral_threshold:
                # Use default thresholds if no genre-specific threshold found
                viral_threshold = {
                    "vote_velocity_p99": 300,  # Default threshold
                    "comment_velocity_p99": 50,
                    "engagement_velocity_p99": 400
                }
            
            # Step 5: Calculate viral coefficient
            # Viral coefficient = current velocity / threshold velocity
            upvote_threshold = viral_threshold.get("vote_velocity_p99", 300)
            comment_threshold = viral_threshold.get("comment_velocity_p99", 50)
            engagement_threshold = viral_threshold.get("engagement_velocity_p99", 400)
            
            # Calculate combined engagement velocity
            engagement_velocity = upvote_velocity + comment_velocity
            
            # Viral coefficient (how many times above threshold)
            viral_coefficient = engagement_velocity / engagement_threshold if engagement_threshold > 0 else 0
            
            # Step 6: Calculate percentile ranking
            percentile = min(99.9, 50 + (viral_coefficient - 1) * 20)  # Rough percentile estimate
            
            # Step 7: Track cross-subreddit spread
            subreddit_count = len(movie_data["subreddits"])
            cross_subreddit_spread = subreddit_count > 3  # Viral if spreading across 4+ subreddits
            
            # Only include if above viral threshold
            if viral_coefficient >= viral_coefficient_threshold:
                viral_entry = {
                    "movie_title": movie_title_key,
                    "genre": primary_genre,
                    "viral_metrics": {
                        "viral_coefficient": round(viral_coefficient, 2),
                        "percentile_rank": round(percentile, 1),
                        "upvote_velocity": round(upvote_velocity, 1),
                        "comment_velocity": round(comment_velocity, 1),
                        "award_velocity": round(award_velocity, 2),
                        "engagement_velocity": round(engagement_velocity, 1)
                    },
                    "thresholds": {
                        "upvote_p99": upvote_threshold,
                        "comment_p99": comment_threshold,
                        "engagement_p99": engagement_threshold
                    },
                    "reddit_stats": {
                        "total_upvotes": movie_data["total_upvotes"],
                        "total_comments": movie_data["total_comments"],
                        "total_awards": movie_data["total_awards"],
                        "avg_sentiment": round(movie_data["avg_sentiment"], 3),
                        "post_count": movie_data["post_count"],
                        "subreddit_count": subreddit_count,
                        "cross_subreddit_spread": cross_subreddit_spread,
                        "subreddits": movie_data["subreddits"]
                    },
                    "viral_status": "viral" if viral_coefficient >= 1.0 else "trending",
                    "time_window": {
                        "start": earliest.isoformat() if isinstance(earliest, datetime) else str(earliest),
                        "end": latest.isoformat() if isinstance(latest, datetime) else str(latest),
                        "hours": round(time_span_hours, 1)
                    }
                }
                
                viral_movies.append(viral_entry)
        
        # Step 8: Sort by viral coefficient (descending)
        viral_movies.sort(key=lambda x: x["viral_metrics"]["viral_coefficient"], reverse=True)
        
        result["viral_movies"] = viral_movies[:limit]
        result["total_trending"] = len(viral_movies)
        
        return result
