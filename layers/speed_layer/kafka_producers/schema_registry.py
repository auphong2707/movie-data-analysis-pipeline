"""
Schema Registry for TMDB streaming data.
Manages Avro schemas for Kafka topics.
"""

import json
import logging
from typing import Dict, Any, Optional
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.avro import AvroProducer
import avro.schema
import avro.io
import io

logger = logging.getLogger(__name__)


class TMDBSchemaRegistry:
    """Manages Avro schemas for TMDB streaming data."""
    
    def __init__(self, schema_registry_url: str = "http://localhost:8081"):
        """Initialize Schema Registry client."""
        self.client = SchemaRegistryClient({'url': schema_registry_url})
        self.schemas = {}
        self._register_schemas()
    
    def _register_schemas(self):
        """Register all TMDB schemas with Schema Registry."""
        
        # Movie Reviews Schema
        review_schema = {
            "type": "record",
            "name": "MovieReview",
            "namespace": "com.movieanalytics.tmdb",
            "fields": [
                {"name": "review_id", "type": "string"},
                {"name": "movie_id", "type": "long"},
                {"name": "author", "type": "string"},
                {"name": "content", "type": "string"},
                {"name": "rating", "type": ["null", "double"], "default": None},
                {"name": "created_at", "type": "string"},
                {"name": "url", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "language", "type": ["null", "string"], "default": None}
            ]
        }
        
        # Movie Ratings Schema
        rating_schema = {
            "type": "record", 
            "name": "MovieRating",
            "namespace": "com.movieanalytics.tmdb",
            "fields": [
                {"name": "movie_id", "type": "long"},
                {"name": "user_id", "type": "string"},
                {"name": "rating", "type": "double"},
                {"name": "timestamp", "type": "long"},
                {"name": "source", "type": "string", "default": "tmdb"}
            ]
        }
        
        # Movie Metadata Schema
        metadata_schema = {
            "type": "record",
            "name": "MovieMetadata", 
            "namespace": "com.movieanalytics.tmdb",
            "fields": [
                {"name": "movie_id", "type": "long"},
                {"name": "title", "type": "string"},
                {"name": "release_date", "type": ["null", "string"], "default": None},
                {"name": "popularity", "type": "double"},
                {"name": "vote_average", "type": "double"},
                {"name": "vote_count", "type": "long"},
                {"name": "adult", "type": "boolean", "default": False},
                {"name": "genre_ids", "type": {"type": "array", "items": "int"}},
                {"name": "overview", "type": ["null", "string"], "default": None},
                {"name": "poster_path", "type": ["null", "string"], "default": None},
                {"name": "backdrop_path", "type": ["null", "string"], "default": None},
                {"name": "original_language", "type": ["null", "string"], "default": None},
                {"name": "original_title", "type": ["null", "string"], "default": None},
                {"name": "video", "type": "boolean", "default": False},
                {"name": "timestamp", "type": "long"}
            ]
        }
        
        # Trending Movie Event Schema
        trending_schema = {
            "type": "record",
            "name": "TrendingEvent",
            "namespace": "com.movieanalytics.tmdb",
            "fields": [
                {"name": "movie_id", "type": "long"},
                {"name": "title", "type": "string"},
                {"name": "trend_score", "type": "double"},
                {"name": "popularity_delta", "type": "double"},
                {"name": "rating_velocity", "type": "double"},
                {"name": "review_volume", "type": "long"},
                {"name": "time_window", "type": "string"},
                {"name": "timestamp", "type": "long"}
            ]
        }
        
        try:
            # Register schemas
            self.schemas['movie.reviews'] = self._register_schema(
                'movie.reviews-value', json.dumps(review_schema)
            )
            self.schemas['movie.ratings'] = self._register_schema(
                'movie.ratings-value', json.dumps(rating_schema)
            )
            self.schemas['movie.metadata'] = self._register_schema(
                'movie.metadata-value', json.dumps(metadata_schema)
            )
            self.schemas['movie.trending'] = self._register_schema(
                'movie.trending-value', json.dumps(trending_schema)
            )
            
            logger.info("Successfully registered all TMDB schemas")
            
        except Exception as e:
            logger.error(f"Failed to register schemas: {e}")
            raise
    
    def _register_schema(self, subject: str, schema_str: str) -> Schema:
        """Register a schema with the Schema Registry."""
        try:
            schema = Schema(schema_str, "AVRO")
            schema_id = self.client.register_schema(subject, schema)
            logger.info(f"Registered schema {subject} with ID {schema_id}")
            return schema
        except Exception as e:
            logger.error(f"Failed to register schema {subject}: {e}")
            raise
    
    def get_schema(self, topic: str) -> Optional[Schema]:
        """Get schema for a topic."""
        return self.schemas.get(topic)
    
    def get_avro_producer(self, kafka_config: Dict[str, Any]) -> AvroProducer:
        """Create an Avro producer with schema registry integration."""
        producer_config = {
            **kafka_config,
            'schema.registry.url': self.client.conf['url']
        }
        
        return AvroProducer(producer_config)
    
    def serialize_message(self, topic: str, message: Dict[str, Any]) -> bytes:
        """Manually serialize a message using Avro schema."""
        schema = self.get_schema(topic)
        if not schema:
            raise ValueError(f"No schema found for topic {topic}")
        
        try:
            # Parse Avro schema
            avro_schema = avro.schema.parse(schema.schema_str)
            
            # Serialize message
            writer = avro.io.DatumWriter(avro_schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(message, encoder)
            
            return bytes_writer.getvalue()
            
        except Exception as e:
            logger.error(f"Failed to serialize message for topic {topic}: {e}")
            raise
    
    def validate_message(self, topic: str, message: Dict[str, Any]) -> bool:
        """Validate a message against its schema."""
        try:
            self.serialize_message(topic, message)
            return True
        except Exception as e:
            logger.warning(f"Message validation failed for topic {topic}: {e}")
            return False


class SchemaEvolution:
    """Handles schema evolution and compatibility checks."""
    
    def __init__(self, schema_registry: TMDBSchemaRegistry):
        self.registry = schema_registry
    
    def check_compatibility(self, subject: str, new_schema: str) -> bool:
        """Check if new schema is compatible with existing ones."""
        try:
            schema = Schema(new_schema, "AVRO")
            is_compatible = self.registry.client.check_compatibility(subject, schema)
            logger.info(f"Schema compatibility check for {subject}: {is_compatible}")
            return is_compatible
        except Exception as e:
            logger.error(f"Compatibility check failed for {subject}: {e}")
            return False
    
    def evolve_schema(self, subject: str, new_schema: str) -> Optional[int]:
        """Evolve schema if compatible."""
        if self.check_compatibility(subject, new_schema):
            try:
                schema = Schema(new_schema, "AVRO")
                schema_id = self.registry.client.register_schema(subject, schema)
                logger.info(f"Schema evolved for {subject} with new ID {schema_id}")
                return schema_id
            except Exception as e:
                logger.error(f"Schema evolution failed for {subject}: {e}")
                return None
        else:
            logger.warning(f"Schema evolution rejected for {subject} - not compatible")
            return None


# Schema validation utilities
def validate_review_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and normalize review message."""
    required_fields = ['review_id', 'movie_id', 'author', 'content', 'created_at', 'url', 'timestamp']
    
    for field in required_fields:
        if field not in message:
            raise ValueError(f"Missing required field: {field}")
    
    # Normalize types
    message['movie_id'] = int(message['movie_id'])
    message['timestamp'] = int(message['timestamp'])
    
    if 'rating' in message and message['rating'] is not None:
        message['rating'] = float(message['rating'])
    
    return message


def validate_rating_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and normalize rating message."""
    required_fields = ['movie_id', 'user_id', 'rating', 'timestamp']
    
    for field in required_fields:
        if field not in message:
            raise ValueError(f"Missing required field: {field}")
    
    # Normalize types
    message['movie_id'] = int(message['movie_id'])
    message['rating'] = float(message['rating'])
    message['timestamp'] = int(message['timestamp'])
    
    if 'source' not in message:
        message['source'] = 'tmdb'
    
    return message


def validate_metadata_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and normalize metadata message."""
    required_fields = ['movie_id', 'title', 'popularity', 'vote_average', 'vote_count', 'genre_ids', 'timestamp']
    
    for field in required_fields:
        if field not in message:
            raise ValueError(f"Missing required field: {field}")
    
    # Normalize types
    message['movie_id'] = int(message['movie_id'])
    message['popularity'] = float(message['popularity'])
    message['vote_average'] = float(message['vote_average'])
    message['vote_count'] = int(message['vote_count'])
    message['timestamp'] = int(message['timestamp'])
    
    if 'adult' not in message:
        message['adult'] = False
    if 'video' not in message:
        message['video'] = False
    
    # Ensure genre_ids is a list of integers
    if isinstance(message['genre_ids'], list):
        message['genre_ids'] = [int(gid) for gid in message['genre_ids']]
    else:
        message['genre_ids'] = []
    
    return message