"""
MongoDB serving layer for fast queries and real-time analytics.
"""
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import PyMongoError, BulkWriteError
import pandas as pd
from bson import ObjectId
import json

from config.config import config

logger = logging.getLogger(__name__)

class MongoDBManager:
    """Manages MongoDB connections and operations for the serving layer."""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.collections = {}
        self._connect()
        self._initialize_collections()
    
    def _connect(self):
        """Establish connection to MongoDB."""
        try:
            self.client = MongoClient(config.mongodb_connection_string)
            self.db = self.client[config.mongodb_database]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _initialize_collections(self):
        """Initialize collections and indexes."""
        collections_config = {
            'movies': {
                'indexes': [
                    ('movie_id', 1),
                    ('title', 1),
                    ('release_year', 1),
                    ('popularity', -1),
                    ('vote_average', -1),
                    [('release_year', 1), ('language', 1)],
                    [('genres.name', 1), ('release_year', 1)]
                ]
            },
            'movie_metrics': {
                'indexes': [
                    ('movie_id', 1),
                    ('avg_popularity', -1),
                    ('avg_rating', -1),
                    ('release_year', 1)
                ]
            },
            'reviews': {
                'indexes': [
                    ('review_id', 1),
                    ('movie_id', 1),
                    ('sentiment_label', 1),
                    ('sentiment_score', -1),
                    ('review_date', -1),
                    [('movie_id', 1), ('sentiment_label', 1)]
                ]
            },
            'review_metrics': {
                'indexes': [
                    ('movie_id', 1),
                    ('avg_sentiment', -1),
                    ('total_reviews', -1)
                ]
            },
            'trending_movies': {
                'indexes': [
                    ('window_start', -1),
                    ('trend_score', -1),
                    ('movie_id', 1),
                    [('window_start', -1), ('trend_score', -1)]
                ]
            },
            'sentiment_trends': {
                'indexes': [
                    ('movie_id', 1),
                    ('window_start', -1),
                    ('avg_sentiment', -1),
                    [('movie_id', 1), ('window_start', -1)]
                ]
            },
            'people': {
                'indexes': [
                    ('person_id', 1),
                    ('name', 1),
                    ('popularity', -1),
                    ('known_for_department', 1),
                    [('known_for_department', 1), ('popularity', -1)]
                ]
            },
            'credits': {
                'indexes': [
                    ('movie_id', 1),
                    ('person_id', 1),
                    ('credit_type', 1),
                    [('movie_id', 1), ('credit_type', 1)],
                    [('person_id', 1), ('credit_type', 1)]
                ]
            },
            'genre_metrics': {
                'indexes': [
                    ('genre_name', 1),
                    ('release_year', 1),
                    ('avg_rating', -1),
                    [('genre_name', 1), ('release_year', 1)]
                ]
            }
        }
        
        for collection_name, config_data in collections_config.items():
            collection = self.db[collection_name]
            self.collections[collection_name] = collection
            
            # Create indexes
            for index_spec in config_data['indexes']:
                try:
                    if isinstance(index_spec, tuple):
                        collection.create_index([index_spec])
                    elif isinstance(index_spec, list):
                        collection.create_index(index_spec)
                    else:
                        collection.create_index(index_spec)
                except Exception as e:
                    logger.warning(f"Index creation failed for {collection_name}: {e}")
        
        logger.info(f"Initialized {len(self.collections)} collections")
    
    def insert_movies(self, movies_data: List[Dict]) -> Dict[str, Any]:
        """Insert movie records."""
        return self._bulk_upsert('movies', movies_data, 'movie_id')
    
    def insert_movie_metrics(self, metrics_data: List[Dict]) -> Dict[str, Any]:
        """Insert movie metrics for fast lookups."""
        return self._bulk_upsert('movie_metrics', metrics_data, 'movie_id')
    
    def insert_reviews(self, reviews_data: List[Dict]) -> Dict[str, Any]:
        """Insert review records."""
        return self._bulk_upsert('reviews', reviews_data, 'review_id')
    
    def insert_review_metrics(self, metrics_data: List[Dict]) -> Dict[str, Any]:
        """Insert review metrics aggregations."""
        return self._bulk_upsert('review_metrics', metrics_data, 'movie_id')
    
    def insert_trending_movies(self, trending_data: List[Dict]) -> Dict[str, Any]:
        """Insert trending movies data."""
        return self._bulk_insert('trending_movies', trending_data)
    
    def insert_sentiment_trends(self, sentiment_data: List[Dict]) -> Dict[str, Any]:
        """Insert sentiment trend data."""
        return self._bulk_insert('sentiment_trends', sentiment_data)
    
    def insert_people(self, people_data: List[Dict]) -> Dict[str, Any]:
        """Insert people records."""
        return self._bulk_upsert('people', people_data, 'person_id')
    
    def insert_credits(self, credits_data: List[Dict]) -> Dict[str, Any]:
        """Insert credits records."""
        return self._bulk_upsert('credits', credits_data, ['movie_id', 'person_id', 'credit_type'])
    
    def _bulk_upsert(self, collection_name: str, data: List[Dict], 
                     unique_key: Union[str, List[str]]) -> Dict[str, Any]:
        """Perform bulk upsert operations."""
        if not data:
            return {'inserted': 0, 'updated': 0, 'errors': []}
        
        collection = self.collections[collection_name]
        operations = []
        
        for doc in data:
            # Add timestamp
            doc['updated_at'] = datetime.utcnow()
            
            # Create filter for unique key
            if isinstance(unique_key, str):
                filter_doc = {unique_key: doc[unique_key]}
            else:
                filter_doc = {key: doc[key] for key in unique_key if key in doc}
            
            # Create upsert operation
            operations.append(
                UpdateOne(
                    filter_doc,
                    {'$set': doc},
                    upsert=True
                )
            )
        
        try:
            result = collection.bulk_write(operations)
            return {
                'inserted': result.upserted_count,
                'updated': result.modified_count,
                'errors': []
            }
        except BulkWriteError as e:
            return {
                'inserted': e.details.get('nUpserted', 0),
                'updated': e.details.get('nModified', 0),
                'errors': e.details.get('writeErrors', [])
            }
        except Exception as e:
            logger.error(f"Bulk upsert failed for {collection_name}: {e}")
            return {'inserted': 0, 'updated': 0, 'errors': [str(e)]}
    
    def _bulk_insert(self, collection_name: str, data: List[Dict]) -> Dict[str, Any]:
        """Perform bulk insert operations."""
        if not data:
            return {'inserted': 0, 'errors': []}
        
        collection = self.collections[collection_name]
        
        # Add timestamps
        for doc in data:
            doc['inserted_at'] = datetime.utcnow()
        
        try:
            result = collection.insert_many(data)
            return {
                'inserted': len(result.inserted_ids),
                'errors': []
            }
        except Exception as e:
            logger.error(f"Bulk insert failed for {collection_name}: {e}")
            return {'inserted': 0, 'errors': [str(e)]}

class MovieQueryService:
    """Service for querying movie data from MongoDB."""
    
    def __init__(self, mongodb_manager: MongoDBManager):
        self.mongodb = mongodb_manager
    
    def get_movie_by_id(self, movie_id: int) -> Optional[Dict]:
        """Get movie details by ID."""
        return self.mongodb.collections['movies'].find_one({'movie_id': movie_id})
    
    def search_movies(self, query: str, limit: int = 20) -> List[Dict]:
        """Search movies by title."""
        regex_query = {'title': {'$regex': query, '$options': 'i'}}
        return list(self.mongodb.collections['movies'].find(regex_query).limit(limit))
    
    def get_popular_movies(self, limit: int = 50, year: Optional[int] = None) -> List[Dict]:
        """Get popular movies, optionally filtered by year."""
        filter_doc = {}
        if year:
            filter_doc['release_year'] = year
        
        return list(
            self.mongodb.collections['movie_metrics']
            .find(filter_doc)
            .sort('avg_popularity', -1)
            .limit(limit)
        )
    
    def get_top_rated_movies(self, limit: int = 50, min_reviews: int = 10) -> List[Dict]:
        """Get top rated movies with minimum review count."""
        pipeline = [
            {
                '$lookup': {
                    'from': 'review_metrics',
                    'localField': 'movie_id',
                    'foreignField': 'movie_id',
                    'as': 'review_data'
                }
            },
            {
                '$match': {
                    'review_data.total_reviews': {'$gte': min_reviews}
                }
            },
            {
                '$sort': {'avg_rating': -1}
            },
            {
                '$limit': limit
            }
        ]
        
        return list(self.mongodb.collections['movie_metrics'].aggregate(pipeline))
    
    def get_trending_movies(self, hours: int = 24) -> List[Dict]:
        """Get trending movies from the last N hours."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        return list(
            self.mongodb.collections['trending_movies']
            .find({'window_start': {'$gte': cutoff_time}})
            .sort('trend_score', -1)
            .limit(50)
        )
    
    def get_movies_by_genre(self, genre: str, limit: int = 20) -> List[Dict]:
        """Get movies by genre."""
        return list(
            self.mongodb.collections['movies']
            .find({'genres.name': genre})
            .sort('vote_average', -1)
            .limit(limit)
        )
    
    def get_movie_sentiment(self, movie_id: int) -> Optional[Dict]:
        """Get sentiment analysis for a movie."""
        return self.mongodb.collections['review_metrics'].find_one({'movie_id': movie_id})
    
    def get_movies_with_sentiment(self, sentiment_label: str, limit: int = 20) -> List[Dict]:
        """Get movies with specific sentiment (positive/negative/neutral)."""
        pipeline = [
            {
                '$match': {
                    'sentiment_label': sentiment_label
                }
            },
            {
                '$group': {
                    '_id': '$movie_id',
                    'avg_sentiment': {'$avg': '$sentiment_score'},
                    'review_count': {'$sum': 1}
                }
            },
            {
                '$match': {
                    'review_count': {'$gte': 5}  # Minimum reviews for reliable sentiment
                }
            },
            {
                '$sort': {'avg_sentiment': -1 if sentiment_label == 'positive' else 1}
            },
            {
                '$limit': limit
            },
            {
                '$lookup': {
                    'from': 'movies',
                    'localField': '_id',
                    'foreignField': 'movie_id',
                    'as': 'movie_details'
                }
            }
        ]
        
        return list(self.mongodb.collections['reviews'].aggregate(pipeline))

class PersonQueryService:
    """Service for querying people data from MongoDB."""
    
    def __init__(self, mongodb_manager: MongoDBManager):
        self.mongodb = mongodb_manager
    
    def get_person_by_id(self, person_id: int) -> Optional[Dict]:
        """Get person details by ID."""
        return self.mongodb.collections['people'].find_one({'person_id': person_id})
    
    def search_people(self, query: str, limit: int = 20) -> List[Dict]:
        """Search people by name."""
        regex_query = {'name': {'$regex': query, '$options': 'i'}}
        return list(self.mongodb.collections['people'].find(regex_query).limit(limit))
    
    def get_popular_people(self, department: Optional[str] = None, limit: int = 50) -> List[Dict]:
        """Get popular people, optionally filtered by department."""
        filter_doc = {}
        if department:
            filter_doc['known_for_department'] = department
        
        return list(
            self.mongodb.collections['people']
            .find(filter_doc)
            .sort('popularity', -1)
            .limit(limit)
        )
    
    def get_person_movies(self, person_id: int, credit_type: Optional[str] = None) -> List[Dict]:
        """Get movies for a person, optionally filtered by credit type."""
        filter_doc = {'person_id': person_id}
        if credit_type:
            filter_doc['credit_type'] = credit_type
        
        pipeline = [
            {'$match': filter_doc},
            {
                '$lookup': {
                    'from': 'movies',
                    'localField': 'movie_id',
                    'foreignField': 'movie_id',
                    'as': 'movie_details'
                }
            },
            {
                '$unwind': '$movie_details'
            },
            {
                '$sort': {'movie_details.release_date': -1}
            }
        ]
        
        return list(self.mongodb.collections['credits'].aggregate(pipeline))
    
    def get_movie_cast(self, movie_id: int, limit: int = 20) -> List[Dict]:
        """Get cast for a movie."""
        return list(
            self.mongodb.collections['credits']
            .find({'movie_id': movie_id, 'credit_type': 'cast'})
            .sort('cast_order', 1)
            .limit(limit)
        )
    
    def get_movie_crew(self, movie_id: int, department: Optional[str] = None) -> List[Dict]:
        """Get crew for a movie, optionally filtered by department."""
        filter_doc = {'movie_id': movie_id, 'credit_type': 'crew'}
        if department:
            filter_doc['department'] = department
        
        return list(self.mongodb.collections['credits'].find(filter_doc))

class AnalyticsQueryService:
    """Service for analytics and aggregation queries."""
    
    def __init__(self, mongodb_manager: MongoDBManager):
        self.mongodb = mongodb_manager
    
    def get_genre_trends(self, years: Optional[List[int]] = None) -> List[Dict]:
        """Get genre popularity trends over time."""
        match_stage = {}
        if years:
            match_stage['release_year'] = {'$in': years}
        
        pipeline = [
            {'$match': match_stage} if match_stage else {'$match': {}},
            {
                '$group': {
                    '_id': {
                        'genre': '$genre_name',
                        'year': '$release_year'
                    },
                    'avg_rating': {'$avg': '$avg_rating'},
                    'avg_popularity': {'$avg': '$avg_popularity'},
                    'movie_count': {'$sum': '$movie_count'}
                }
            },
            {
                '$sort': {'_id.year': 1, 'avg_popularity': -1}
            }
        ]
        
        return list(self.mongodb.collections['genre_metrics'].aggregate(pipeline))
    
    def get_sentiment_trends_over_time(self, movie_id: Optional[int] = None, 
                                     days: int = 30) -> List[Dict]:
        """Get sentiment trends over time."""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        match_stage = {'window_start': {'$gte': cutoff_date}}
        if movie_id:
            match_stage['movie_id'] = movie_id
        
        pipeline = [
            {'$match': match_stage},
            {
                '$group': {
                    '_id': {
                        'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$window_start'}},
                        'movie_id': '$movie_id' if movie_id else None
                    },
                    'avg_sentiment': {'$avg': '$avg_sentiment'},
                    'total_reviews': {'$sum': '$review_count'}
                }
            },
            {
                '$sort': {'_id.date': 1}
            }
        ]
        
        if not movie_id:
            # Remove movie_id from grouping if not specified
            pipeline[1]['$group']['_id'] = {
                'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$window_start'}}
            }
        
        return list(self.mongodb.collections['sentiment_trends'].aggregate(pipeline))
    
    def get_top_collaborations(self, limit: int = 20) -> List[Dict]:
        """Get top actor-director collaborations."""
        pipeline = [
            {
                '$match': {
                    'credit_type': {'$in': ['cast', 'crew']},
                    'job': {'$in': ['Director', None]}  # Directors or cast
                }
            },
            {
                '$group': {
                    '_id': '$movie_id',
                    'cast': {
                        '$push': {
                            '$cond': [
                                {'$eq': ['$credit_type', 'cast']},
                                '$person_id',
                                None
                            ]
                        }
                    },
                    'directors': {
                        '$push': {
                            '$cond': [
                                {'$eq': ['$job', 'Director']},
                                '$person_id',
                                None
                            ]
                        }
                    }
                }
            },
            # Further processing would be done in application logic
        ]
        
        return list(self.mongodb.collections['credits'].aggregate(pipeline))
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get database performance metrics."""
        metrics = {}
        
        for collection_name, collection in self.mongodb.collections.items():
            try:
                stats = self.mongodb.db.command('collStats', collection_name)
                metrics[collection_name] = {
                    'document_count': stats.get('count', 0),
                    'storage_size_mb': stats.get('storageSize', 0) / (1024 * 1024),
                    'index_size_mb': stats.get('totalIndexSize', 0) / (1024 * 1024),
                    'avg_document_size_bytes': stats.get('avgObjSize', 0)
                }
            except Exception as e:
                metrics[collection_name] = {'error': str(e)}
        
        return metrics