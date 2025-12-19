"""
Recommendation Engine - Content-based filtering with real-time re-ranking

Implements:
1. Content-based similarity (genres, cast, keywords)
2. Trending boost from speed layer
3. Sentiment-based re-ranking
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pymongo.database import Database
from collections import Counter
import logging

logger = logging.getLogger(__name__)


class RecommendationEngine:
    """
    Content-based recommendation engine with real-time adjustments
    
    Updated to work with 3 separate batch collections:
    - sentiment_baselines: Genre/franchise/yearly sentiment patterns
    - viral_thresholds: Genre/budget-tier/seasonal viral cutoffs
    - movie_intelligence: Individual movie competitive data
    """
    
    def __init__(self, db: Database):
        self.db = db
        # 3 separate batch collections
        self.sentiment_baselines = db.sentiment_baselines
        self.viral_thresholds = db.viral_thresholds
        self.movie_intelligence = db.movie_intelligence
        # Speed layer collection
        self.speed_views = db.speed_views
    
    def get_similar_movies(
        self,
        movie_id: int,
        limit: int = 10,
        boost_trending: bool = True,
        boost_sentiment: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Find similar movies using content-based filtering
        
        Similarity based on:
        - Genre overlap (weight: 0.5)
        - Release year proximity (weight: 0.2)
        - Rating similarity (weight: 0.3)
        
        Then re-ranked by trending and sentiment
        """
        # Get source movie from movie_intelligence collection
        source = self.movie_intelligence.find_one({
            'movie_id': movie_id
        })
        
        if not source:
            logger.warning(f"Movie {movie_id} not found in database")
            return []
        
        # Extract genres - check both 'genre' (string) and 'genres' (array)
        genres_field = source.get('genres', [])
        genre_field = source.get('genre')
        if genre_field and not genres_field:
            genres_field = [genre_field] if isinstance(genre_field, str) else genre_field
        
        source_genres = self._extract_genres(genres_field)
        source_year = self._extract_year(source.get('release_date'))
        source_rating = source.get('vote_average', 0)
        
        # Find candidates with genre overlap from movie_intelligence collection
        candidates = []
        
        # Query both flat and nested genre fields
        query = {
            'movie_id': {'$ne': movie_id},
            '$or': [
                {'genres': {'$in': list(source_genres)}},
                {'genre': {'$in': list(source_genres)}}
            ]
        }
        
        for movie in self.movie_intelligence.find(query).limit(200):
            # Extract genres - check both 'genre' (string) and 'genres' (array)
            genres_field = movie.get('genres', [])
            genre_field = movie.get('genre')
            if genre_field and not genres_field:
                genres_field = [genre_field] if isinstance(genre_field, str) else genre_field
            
            movie_genres = self._extract_genres(genres_field)
            movie_year = self._extract_year(movie.get('release_date'))
            movie_rating = movie.get('vote_average', 0)
            
            # Calculate content similarity
            genre_sim = len(source_genres & movie_genres) / len(source_genres | movie_genres) if source_genres | movie_genres else 0
            year_sim = 1 - min(abs(source_year - movie_year) / 50, 1) if source_year and movie_year else 0
            rating_sim = 1 - abs(source_rating - movie_rating) / 10
            
            content_score = (
                genre_sim * 0.5 +
                year_sim * 0.2 +
                rating_sim * 0.3
            )
            
            candidates.append({
                'movie_id': movie['movie_id'],
                'title': movie.get('title', 'Unknown'),
                'genres': list(movie_genres),
                'release_date': movie.get('release_date'),
                'vote_average': movie_rating,
                'popularity': movie.get('popularity', 0),
                'content_score': content_score
            })
        
        # Sort by content similarity
        candidates.sort(key=lambda x: x['content_score'], reverse=True)
        
        # Apply trending boost
        if boost_trending:
            try:
                candidates = self._apply_trending_boost(candidates)
            except Exception as e:
                logger.warning(f"Error applying trending boost: {e}")
        
        # Apply sentiment boost
        if boost_sentiment:
            try:
                candidates = self._apply_sentiment_boost(candidates)
            except Exception as e:
                logger.warning(f"Error applying sentiment boost: {e}")
        
        # Calculate final hybrid score
        for c in candidates:
            c['hybrid_score'] = (
                c.get('content_score', 0) * 0.5 +
                c.get('trending_boost', 0) * 0.3 +
                c.get('sentiment_boost', 0) * 0.2
            )
        
        # Sort by hybrid score
        candidates.sort(key=lambda x: x['hybrid_score'], reverse=True)
        
        # Format response
        return [
            {
                'movie_id': c['movie_id'],
                'title': c['title'],
                'genres': c['genres'],
                'release_date': c['release_date'],
                'vote_average': round(c['vote_average'], 2),
                'similarity_score': round(c['content_score'], 3),
                'trending_score': round(c.get('trending_boost', 0), 3),
                'sentiment_score': round(c.get('sentiment_boost', 0), 3),
                'hybrid_score': round(c['hybrid_score'], 3)
            }
            for c in candidates[:limit]
        ]
    
    def get_genre_recommendations(
        self,
        genre: str,
        limit: int = 20,
        min_rating: float = 6.0,
        sort_by: str = 'hybrid'
    ) -> List[Dict[str, Any]]:
        """
        Get top movies in genre with hybrid ranking from movie_intelligence collection
        """
        # Query movie_intelligence collection
        query = {
            '$and': [
                {
                    '$or': [
                        {'genres': genre},  # Array field
                        {'genre': genre}    # String field
                    ]
                },
                {'vote_average': {'$gte': min_rating}}
            ]
        }
        
        movies = list(self.movie_intelligence.find(query).limit(100))
        
        candidates = []
        for movie in movies:
            # Extract genres - check both 'genre' (string) and 'genres' (array)
            genres_field = movie.get('genres', [])
            genre_field = movie.get('genre')
            if genre_field and not genres_field:
                genres_field = [genre_field] if isinstance(genre_field, str) else genre_field
            
            candidates.append({
                'movie_id': movie['movie_id'],
                'title': movie.get('title', 'Unknown'),
                'genres': genres_field,
                'release_date': movie.get('release_date'),
                'vote_average': movie.get('vote_average', 0),
                'vote_count': movie.get('vote_count', 0),
                'popularity': movie.get('popularity', 0)
            })
        
        # Apply boosts based on sort strategy
        if sort_by in ['hybrid', 'trending']:
            candidates = self._apply_trending_boost(candidates)
        
        if sort_by in ['hybrid', 'sentiment']:
            candidates = self._apply_sentiment_boost(candidates)
        
        # Calculate scores based on strategy
        for c in candidates:
            if sort_by == 'hybrid':
                c['score'] = (
                    (c['vote_average'] / 10) * 0.4 +
                    c.get('trending_boost', 0) * 0.3 +
                    c.get('sentiment_boost', 0) * 0.3
                )
            elif sort_by == 'trending':
                c['score'] = c.get('trending_boost', 0)
            elif sort_by == 'sentiment':
                c['score'] = c.get('sentiment_boost', 0)
            else:  # rating
                c['score'] = c['vote_average'] / 10
        
        # Sort by score
        candidates.sort(key=lambda x: x['score'], reverse=True)
        
        return [
            {
                'movie_id': c['movie_id'],
                'title': c['title'],
                'genres': c['genres'],
                'vote_average': round(c['vote_average'], 2),
                'vote_count': c['vote_count'],
                'score': round(c['score'], 3)
            }
            for c in candidates[:limit]
        ]
    
    def _apply_trending_boost(self, candidates: List[Dict]) -> List[Dict]:
        """Apply trending boost from speed layer"""
        cutoff = datetime.utcnow() - timedelta(hours=6)
        
        # Get trending scores
        trending_data = {}
        for doc in self.speed_views.find({
            'data_type': 'stats',
            'hour': {'$gte': cutoff}
        }):
            movie_id = doc.get('movie_id')
            stats = doc.get('stats', {})
            popularity = stats.get('popularity', 0)
            velocity = stats.get('rating_velocity', 0)
            
            trending_score = popularity * 0.7 + velocity * 300
            
            if movie_id not in trending_data or trending_score > trending_data[movie_id]:
                trending_data[movie_id] = trending_score
        
        # Normalize trending scores
        max_trending = max(trending_data.values()) if trending_data else 1
        
        for c in candidates:
            raw_score = trending_data.get(c['movie_id'], 0)
            c['trending_boost'] = raw_score / max_trending if max_trending > 0 else 0
        
        return candidates
    
    def _apply_sentiment_boost(self, candidates: List[Dict]) -> List[Dict]:
        """Apply sentiment boost from batch and speed layers"""
        cutoff = datetime.utcnow() - timedelta(hours=48)
        
        # Get sentiment scores
        sentiment_data = {}
        
        # From speed layer (recent)
        for doc in self.speed_views.find({
            'data_type': 'sentiment',
            'hour': {'$gte': cutoff}
        }):
            movie_id = doc.get('movie_id')
            data = doc.get('data', {})
            sentiment = data.get('avg_sentiment', 0)
            
            if movie_id not in sentiment_data:
                sentiment_data[movie_id] = sentiment
        
        # Get sentiment from movie_intelligence collection
        for movie in self.movie_intelligence.find():
            movie_id = movie.get('movie_id')
            if movie_id not in sentiment_data:
                sentiment_data[movie_id] = movie.get('avg_sentiment', 0)
        
        # Normalize sentiment scores (-1 to 1) -> (0 to 1)
        for c in candidates:
            sentiment = sentiment_data.get(c['movie_id'], 0)
            c['sentiment_boost'] = (sentiment + 1) / 2  # Convert to 0-1 range
        
        return candidates
    
    def _extract_year(self, release_date: Optional[str]) -> Optional[int]:
        """Extract year from release date string"""
        if not release_date:
            return None
        try:
            return int(release_date[:4])
        except:
            return None
    
    def _extract_genres(self, genres: Any) -> set:
        """Extract genre names from genres field (handles both array of strings and array of objects)"""
        if not genres:
            return set()
        
        result = set()
        for genre in genres:
            if isinstance(genre, str):
                result.add(genre)
            elif isinstance(genre, dict) and 'name' in genre:
                result.add(genre['name'])
        
        return result
    
    def get_dual_success_recommendations(
        self,
        genre: Optional[str] = None,
        limit: int = 20,
        min_reddit_score: float = 0.0,
        min_tmdb_rating: float = 6.0,
        reddit_weight: float = 0.6,
        tmdb_weight: float = 0.4
    ) -> List[Dict[str, Any]]:
        """
        Dual-Success Recommendations (Business Goal #3)
        
        Combines Reddit buzz (freshness, engagement) with TMDB quality (historical ratings)
        to surface movies that are both trending AND high quality.
        
        Args:
            genre: Filter by genre (optional)
            limit: Number of recommendations
            min_reddit_score: Minimum Reddit buzz score (0-1)
            min_tmdb_rating: Minimum TMDB rating (0-10)
            reddit_weight: Weight for Reddit buzz (default: 0.6)
            tmdb_weight: Weight for TMDB quality (default: 0.4)
        
        Returns:
            List of movies ranked by dual-success score
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        
        # Step 1: Calculate Reddit buzz scores from speed layer
        reddit_buzz = {}
        
        # Aggregate Reddit engagement metrics
        pipeline = [
            {
                "$match": {
                    "data_type": "reddit_post",
                    "hour": {"$gte": cutoff_time}
                }
            },
            {
                "$group": {
                    "_id": "$movie_title",
                    "total_upvotes": {"$sum": "$metrics.upvotes"},
                    "total_comments": {"$sum": "$metrics.num_comments"},
                    "total_awards": {"$sum": "$metrics.awards"},
                    "avg_sentiment": {"$avg": "$metrics.sentiment_score"},
                    "post_count": {"$sum": 1},
                    "subreddit_count": {"$addToSet": "$subreddit"}
                }
            }
        ]
        
        reddit_data = list(self.speed_views.aggregate(pipeline))
        
        # Calculate buzz scores
        max_upvotes = max([r["total_upvotes"] for r in reddit_data], default=1)
        max_comments = max([r["total_comments"] for r in reddit_data], default=1)
        
        for reddit_item in reddit_data:
            movie_title = reddit_item["_id"]
            
            # Normalize engagement metrics (0-1)
            upvote_score = reddit_item["total_upvotes"] / max_upvotes
            comment_score = reddit_item["total_comments"] / max_comments
            sentiment_score = (reddit_item["avg_sentiment"] + 1) / 2  # -1 to 1 -> 0 to 1
            subreddit_score = min(len(reddit_item["subreddit_count"]) / 10, 1)  # Max at 10 subreddits
            
            # Reddit buzz score (weighted combination)
            buzz_score = (
                upvote_score * 0.4 +
                comment_score * 0.3 +
                sentiment_score * 0.2 +
                subreddit_score * 0.1
            )
            
            reddit_buzz[movie_title] = {
                "buzz_score": buzz_score,
                "upvotes": reddit_item["total_upvotes"],
                "comments": reddit_item["total_comments"],
                "sentiment": round(reddit_item["avg_sentiment"], 3),
                "subreddit_count": len(reddit_item["subreddit_count"]),
                "post_count": reddit_item["post_count"]
            }
        
        # Step 2: Get TMDB quality scores from movie_intelligence collection
        match_criteria = {}
        if genre:
            match_criteria["$or"] = [
                {"genres": genre},
                {"genre": genre}
            ]
        
        candidates = []
        for movie in self.movie_intelligence.find(match_criteria).limit(500):
            movie_title = movie.get("title")
            
            if not movie_title:
                continue
            
            # Get TMDB quality metrics
            tmdb_rating = movie.get("vote_average", 0)
            tmdb_vote_count = movie.get("vote_count", 0)
            tmdb_popularity = movie.get("popularity", 0)
            
            # Skip if below minimum TMDB rating
            if tmdb_rating < min_tmdb_rating:
                continue
            
            # TMDB quality score (0-1)
            rating_score = tmdb_rating / 10
            vote_confidence = min(tmdb_vote_count / 1000, 1)  # Max confidence at 1000 votes
            popularity_score = min(tmdb_popularity / 100, 1)  # Normalize popularity
            
            tmdb_quality_score = (
                rating_score * 0.5 +
                vote_confidence * 0.3 +
                popularity_score * 0.2
            )
            
            # Step 3: Combine Reddit buzz + TMDB quality
            reddit_data = reddit_buzz.get(movie_title, {})
            reddit_score = reddit_data.get("buzz_score", 0)
            
            # Skip if below minimum Reddit score
            if reddit_score < min_reddit_score:
                continue
            
            # Calculate dual-success score
            dual_success_score = (
                reddit_score * reddit_weight +
                tmdb_quality_score * tmdb_weight
            )
            
            # Handle both genres array and genre string
            genres = movie.get("genres", [])
            if not genres:
                genre_field = movie.get("genre")
                genres = [genre_field] if genre_field else []
            
            candidates.append({
                "movie_id": movie.get("movie_id"),
                "title": movie_title,
                "genres": genres,
                "release_date": movie.get("release_date"),
                "dual_success_score": dual_success_score,
                "reddit_buzz": {
                    "score": round(reddit_score, 3),
                    "upvotes": reddit_data.get("upvotes", 0),
                    "comments": reddit_data.get("comments", 0),
                    "sentiment": reddit_data.get("sentiment", 0),
                    "subreddit_count": reddit_data.get("subreddit_count", 0)
                },
                "tmdb_quality": {
                    "score": round(tmdb_quality_score, 3),
                    "rating": round(tmdb_rating, 2),
                    "vote_count": tmdb_vote_count,
                    "popularity": round(tmdb_popularity, 2)
                }
            })
        
        # Step 4: Sort by dual-success score
        candidates.sort(key=lambda x: x["dual_success_score"], reverse=True)
        
        # Step 5: Format response
        return [
            {
                "movie_id": c["movie_id"],
                "title": c["title"],
                "genres": c["genres"],
                "release_date": c["release_date"],
                "dual_success_score": round(c["dual_success_score"], 3),
                "reddit_buzz": c["reddit_buzz"],
                "tmdb_quality": c["tmdb_quality"],
                "recommendation_reason": self._generate_recommendation_reason(c)
            }
            for c in candidates[:limit]
        ]
    
    def _generate_recommendation_reason(self, candidate: Dict) -> str:
        """Generate human-readable recommendation reason"""
        reddit = candidate["reddit_buzz"]
        tmdb = candidate["tmdb_quality"]
        
        reasons = []
        
        if reddit["score"] > 0.7:
            reasons.append(f"trending on Reddit ({reddit['upvotes']} upvotes)")
        if reddit["sentiment"] > 0.5:
            reasons.append("positive community sentiment")
        if reddit["subreddit_count"] >= 5:
            reasons.append(f"viral across {reddit['subreddit_count']} subreddits")
        if tmdb["rating"] >= 8.0:
            reasons.append(f"highly rated ({tmdb['rating']}/10)")
        if tmdb["vote_count"] >= 1000:
            reasons.append("well-established popularity")
        
        if not reasons:
            return "Balanced Reddit buzz and TMDB quality"
        
        return ", ".join(reasons[:3]).capitalize()
