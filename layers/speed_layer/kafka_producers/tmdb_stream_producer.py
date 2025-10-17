"""
Speed Layer - Kafka Stream Producer

Real-time streaming of TMDB data to Kafka topics.
Continuously polls TMDB API for new data and produces to Kafka.
"""

from datetime import datetime
from typing import Dict, Any
import logging
import time
import json
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import requests

logger = logging.getLogger(__name__)


class TMDBStreamProducer:
    """
    Real-time stream producer for TMDB data
    
    Features:
    - Rate limiting (4 requests/second)
    - Avro serialization with schema registry
    - Delivery confirmations
    - Multiple topic support
    """
    
    # Avro schemas
    REVIEW_SCHEMA = """
    {
        "type": "record",
        "name": "MovieReview",
        "namespace": "com.moviepipeline.reviews",
        "fields": [
            {"name": "review_id", "type": "string"},
            {"name": "movie_id", "type": "int"},
            {"name": "author", "type": "string"},
            {"name": "content", "type": "string"},
            {"name": "rating", "type": ["null", "double"]},
            {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
            {"name": "url", "type": "string"}
        ]
    }
    """
    
    RATING_SCHEMA = """
    {
        "type": "record",
        "name": "MovieRating",
        "namespace": "com.moviepipeline.ratings",
        "fields": [
            {"name": "movie_id", "type": "int"},
            {"name": "vote_average", "type": "double"},
            {"name": "vote_count", "type": "int"},
            {"name": "popularity", "type": "double"},
            {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
        ]
    }
    """
    
    def __init__(
        self,
        api_key: str,
        kafka_config: Dict[str, str],
        schema_registry_url: str
    ):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        
        # Initialize Kafka producer
        self.producer = Producer(kafka_config)
        
        # Initialize schema registry
        self.schema_registry_client = SchemaRegistryClient({
            'url': schema_registry_url
        })
        
        # Initialize serializers
        self.review_serializer = AvroSerializer(
            self.schema_registry_client,
            self.REVIEW_SCHEMA
        )
        self.rating_serializer = AvroSerializer(
            self.schema_registry_client,
            self.RATING_SCHEMA
        )
        
        self.rate_limit_delay = 0.25  # 4 requests/second
        self.last_request_time = 0
        
        # Track latest seen items to avoid duplicates
        self.seen_reviews = set()
        self.seen_movies = set()
    
    def _rate_limited_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make rate-limited request to TMDB API"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        
        url = f"{self.base_url}/{endpoint}"
        params = params or {}
        params['api_key'] = self.api_key
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            self.last_request_time = time.time()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def _delivery_report(self, err, msg):
        """Kafka delivery confirmation callback"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def produce_review(self, review: Dict[str, Any]):
        """
        Produce review to Kafka topic
        
        Args:
            review: Review dictionary matching REVIEW_SCHEMA
        """
        try:
            self.producer.produce(
                topic='movie.reviews',
                key=str(review['review_id']),
                value=self.review_serializer(
                    review,
                    SerializationContext('movie.reviews', MessageField.VALUE)
                ),
                on_delivery=self._delivery_report
            )
            self.producer.poll(0)  # Trigger delivery callbacks
        except Exception as e:
            logger.error(f"Failed to produce review: {e}")
    
    def produce_rating(self, rating: Dict[str, Any]):
        """
        Produce rating update to Kafka topic
        
        Args:
            rating: Rating dictionary matching RATING_SCHEMA
        """
        try:
            self.producer.produce(
                topic='movie.ratings',
                key=str(rating['movie_id']),
                value=self.rating_serializer(
                    rating,
                    SerializationContext('movie.ratings', MessageField.VALUE)
                ),
                on_delivery=self._delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to produce rating: {e}")
    
    def poll_new_reviews(self, movie_ids: list[int]) -> int:
        """
        Poll for new reviews and stream to Kafka
        
        Args:
            movie_ids: List of movie IDs to check
        
        Returns:
            Number of new reviews produced
        """
        new_reviews_count = 0
        
        for movie_id in movie_ids:
            try:
                data = self._rate_limited_request(f"movie/{movie_id}/reviews")
                reviews = data.get("results", [])
                
                for review in reviews:
                    review_id = review.get("id")
                    
                    # Skip if already seen
                    if review_id in self.seen_reviews:
                        continue
                    
                    # Prepare review for Kafka
                    kafka_review = {
                        "review_id": review_id,
                        "movie_id": movie_id,
                        "author": review.get("author", "Unknown"),
                        "content": review.get("content", ""),
                        "rating": review.get("author_details", {}).get("rating"),
                        "created_at": int(datetime.now().timestamp() * 1000),
                        "url": review.get("url", "")
                    }
                    
                    # Produce to Kafka
                    self.produce_review(kafka_review)
                    self.seen_reviews.add(review_id)
                    new_reviews_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to poll reviews for movie {movie_id}: {e}")
                continue
        
        self.producer.flush()  # Ensure all messages are sent
        return new_reviews_count
    
    def poll_trending_movies(self) -> int:
        """
        Poll trending movies and stream ratings to Kafka
        
        Returns:
            Number of rating updates produced
        """
        try:
            data = self._rate_limited_request("trending/movie/day")
            movies = data.get("results", [])
            
            ratings_count = 0
            for movie in movies:
                movie_id = movie.get("id")
                
                # Prepare rating for Kafka
                kafka_rating = {
                    "movie_id": movie_id,
                    "vote_average": movie.get("vote_average", 0.0),
                    "vote_count": movie.get("vote_count", 0),
                    "popularity": movie.get("popularity", 0.0),
                    "timestamp": int(datetime.now().timestamp() * 1000)
                }
                
                # Produce to Kafka
                self.produce_rating(kafka_rating)
                ratings_count += 1
            
            self.producer.flush()
            return ratings_count
            
        except Exception as e:
            logger.error(f"Failed to poll trending movies: {e}")
            return 0
    
    def run_streaming(
        self,
        trending_interval_seconds: int = 300,
        reviews_interval_seconds: int = 300
    ):
        """
        Run continuous streaming loop
        
        Args:
            trending_interval_seconds: Interval to poll trending movies
            reviews_interval_seconds: Interval to poll reviews
        """
        logger.info("Starting streaming producer")
        
        last_trending_poll = 0
        last_reviews_poll = 0
        
        # Get initial movie IDs for review polling
        data = self._rate_limited_request("movie/popular", {"page": 1})
        movie_ids = [m["id"] for m in data.get("results", [])[:20]]
        
        try:
            while True:
                current_time = time.time()
                
                # Poll trending movies
                if current_time - last_trending_poll >= trending_interval_seconds:
                    count = self.poll_trending_movies()
                    logger.info(f"Produced {count} rating updates")
                    last_trending_poll = current_time
                
                # Poll reviews
                if current_time - last_reviews_poll >= reviews_interval_seconds:
                    count = self.poll_new_reviews(movie_ids)
                    logger.info(f"Produced {count} new reviews")
                    last_reviews_poll = current_time
                
                time.sleep(10)  # Sleep between checks
                
        except KeyboardInterrupt:
            logger.info("Stopping streaming producer")
        finally:
            self.producer.flush()


# TODO: Implement in next phase
# - Add metadata topic for movie updates
# - Implement error handling and retry logic
# - Add metrics and monitoring
# - Implement graceful shutdown
# - Add health check endpoint
