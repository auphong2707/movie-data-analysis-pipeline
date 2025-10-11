"""
Kafka producer for streaming movie data.
"""
import json
import logging
from typing import Dict, Any, Optional
from confluent_kafka import Producer, KafkaError
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import threading
import time
from config.config import config

logger = logging.getLogger(__name__)

class MovieDataProducer:
    """Kafka producer for movie data streaming."""
    
    def __init__(self, bootstrap_servers: str, schema_registry_url: str):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        
        # Kafka producer configuration
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'error_cb': self._error_callback,
            'acks': 'all',
            'retries': 3,
            'batch.size': 16384,
            'linger.ms': 100,
            'compression.type': 'gzip'
        }
        
        self.producer = Producer(producer_config)
        self.schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
        
        # Serializers
        self.string_serializer = StringSerializer('utf_8')
        
        # Delivery tracking
        self.delivery_reports = []
        self.lock = threading.Lock()
        
        # DataHub lineage tracking
        self.lineage_tracker = None
        if config.enable_datahub_lineage:
            try:
                from src.governance.datahub_lineage import get_lineage_tracker
                self.lineage_tracker = get_lineage_tracker()
                logger.info("DataHub lineage tracking enabled")
            except ImportError as e:
                logger.warning(f"DataHub lineage tracking disabled: {e}")
        
        # Track Kafka topics creation in DataHub
        self._track_kafka_topics()
    
    def _error_callback(self, error: KafkaError):
        """Callback for producer errors."""
        logger.error(f"Kafka producer error: {error}")
    
    def _delivery_callback(self, error: Optional[KafkaError], message):
        """Callback for message delivery reports."""
        with self.lock:
            if error:
                self.delivery_reports.append({
                    'status': 'error',
                    'error': str(error),
                    'topic': message.topic(),
                    'partition': message.partition(),
                    'offset': message.offset(),
                    'timestamp': time.time()
                })
                logger.error(f"Message delivery failed: {error}")
            else:
                self.delivery_reports.append({
                    'status': 'success',
                    'topic': message.topic(),
                    'partition': message.partition(),
                    'offset': message.offset(),
                    'timestamp': time.time()
                })
                logger.debug(f"Message delivered to {message.topic()}[{message.partition()}] at offset {message.offset()}")
    
    def produce_movie_data(self, topic: str, movie_data: Dict[str, Any], key: Optional[str] = None):
        """Produce movie data to Kafka topic."""
        try:
            # Add metadata
            enriched_data = {
                **movie_data,
                'ingestion_timestamp': time.time(),
                'data_source': 'tmdb'
            }
            
            # Serialize data
            value = json.dumps(enriched_data, ensure_ascii=False).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None
            
            # Produce message
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value,
                callback=self._delivery_callback
            )
            
            # Trigger delivery reports
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Error producing message to {topic}: {e}")
            raise
    
    def produce_movies(self, movies: list, topic: str = 'movies'):
        """Produce movie records to movies topic."""
        for movie in movies:
            key = str(movie.get('id', ''))
            self.produce_movie_data(topic, movie, key)
    
    def produce_people(self, people: list, topic: str = 'people'):
        """Produce person records to people topic."""
        for person in people:
            key = str(person.get('id', ''))
            self.produce_movie_data(topic, person, key)
    
    def produce_credits(self, credits: Dict[str, Any], movie_id: int, topic: str = 'credits'):
        """Produce credits data to credits topic."""
        # Process cast
        for cast_member in credits.get('cast', []):
            credit_data = {
                'movie_id': movie_id,
                'person_id': cast_member.get('id'),
                'credit_type': 'cast',
                'character': cast_member.get('character'),
                'order': cast_member.get('order'),
                **cast_member
            }
            key = f"{movie_id}_{cast_member.get('id')}_cast"
            self.produce_movie_data(topic, credit_data, key)
        
        # Process crew
        for crew_member in credits.get('crew', []):
            credit_data = {
                'movie_id': movie_id,
                'person_id': crew_member.get('id'),
                'credit_type': 'crew',
                'job': crew_member.get('job'),
                'department': crew_member.get('department'),
                **crew_member
            }
            key = f"{movie_id}_{crew_member.get('id')}_crew"
            self.produce_movie_data(topic, credit_data, key)
    
    def produce_reviews(self, reviews: list, movie_id: int, topic: str = 'reviews'):
        """Produce review records to reviews topic."""
        for review in reviews:
            review_data = {
                'movie_id': movie_id,
                **review
            }
            key = review.get('id', f"{movie_id}_{review.get('author')}")
            self.produce_movie_data(topic, review_data, key)
    
    def flush(self, timeout: float = 10.0):
        """Flush pending messages."""
        self.producer.flush(timeout)
    
    def close(self):
        """Close the producer."""
        self.flush()
        self.producer = None
    
    def _track_kafka_topics(self):
        """Track Kafka topics in DataHub for lineage."""
        if not self.lineage_tracker:
            return
        
        topics = ['movies', 'people', 'credits', 'reviews', 'ratings']
        for topic in topics:
            try:
                self.lineage_tracker.track_kafka_ingestion(
                    topic=topic,
                    source_system='tmdb',
                    properties={
                        'bootstrap_servers': self.bootstrap_servers,
                        'schema_registry_url': self.schema_registry_url,
                        'topic_description': f'Kafka topic for {topic} data from TMDB API'
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to track lineage for topic {topic}: {e}")
    
    def get_delivery_reports(self) -> list:
        """Get delivery reports."""
        with self.lock:
            return self.delivery_reports.copy()
    
    def clear_delivery_reports(self):
        """Clear delivery reports."""
        with self.lock:
            self.delivery_reports.clear()