"""
Event Producer - Generic Kafka event producer with JSON serialization.
Used by TMDB stream producer for publishing events.
"""

import json
import logging
import time
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

logger = logging.getLogger(__name__)


class EventProducer:
    """Generic event producer for Kafka topics with JSON serialization."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Kafka producer with configuration."""
        self.config = config
        
        # Producer configuration
        producer_config = {
            'bootstrap_servers': config['bootstrap_servers'],
            'client_id': config.get('client_id', 'event-producer'),
            'acks': config.get('acks', 1),
            'retries': config.get('retries', 3),
            'compression_type': config.get('compression_type', 'gzip'),
            'linger_ms': config.get('linger_ms', 5),
            'batch_size': config.get('batch_size', 16384),
            'buffer_memory': config.get('buffer_memory', 33554432),
            'max_block_ms': config.get('max_block_ms', 60000),
            'request_timeout_ms': config.get('request_timeout_ms', 30000),
            'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None
        }
        
        try:
            self.producer = KafkaProducer(**producer_config)
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
        
        # Metrics
        self.message_count = 0
        self.error_count = 0
        self.success_count = 0
        
        # Topics from config
        self.topics = config.get('topics', {})
    
    def _delivery_callback(self, record_metadata, exception):
        """Callback for message delivery confirmation."""
        if exception:
            self.error_count += 1
            logger.error(f"Message delivery failed: {exception}")
        else:
            self.success_count += 1
            logger.debug(f"Message delivered to {record_metadata.topic} "
                        f"[{record_metadata.partition}] @ {record_metadata.offset}")
    
    def produce_event(self, topic: str, key: Optional[str], value: Dict[str, Any], 
                     headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Produce an event to specified Kafka topic.
        
        Args:
            topic: Kafka topic name
            key: Message key (optional)
            value: Message value as dictionary
            headers: Optional message headers
            
        Returns:
            bool: True if message was successfully queued
        """
        try:
            # Add timestamp if not present
            if 'timestamp' not in value:
                value['timestamp'] = int(time.time() * 1000)
            
            # Convert headers to bytes
            kafka_headers = None
            if headers:
                kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
            
            # Send message
            future = self.producer.send(
                topic=topic,
                key=key,
                value=value,
                headers=kafka_headers
            )
            
            # Add callback
            future.add_callback(self._delivery_callback)
            future.add_errback(self._delivery_callback)
            
            self.message_count += 1
            return True
            
        except KafkaTimeoutError:
            logger.error(f"Timeout sending message to topic {topic}")
            self.error_count += 1
            return False
        except Exception as e:
            logger.error(f"Failed to produce message to topic {topic}: {e}")
            self.error_count += 1
            return False
    
    def produce_review_event(self, review_data: Dict[str, Any]) -> bool:
        """Produce a movie review event."""
        return self.produce_event(
            topic=self.topics.get('reviews', 'movie.reviews'),
            key=str(review_data.get('review_id')),
            value=review_data,
            headers={'event_type': 'review'}
        )
    
    def produce_rating_event(self, rating_data: Dict[str, Any]) -> bool:
        """Produce a movie rating event."""
        return self.produce_event(
            topic=self.topics.get('ratings', 'movie.ratings'),
            key=str(rating_data.get('movie_id')),
            value=rating_data,
            headers={'event_type': 'rating'}
        )
    
    def produce_metadata_event(self, metadata: Dict[str, Any]) -> bool:
        """Produce a movie metadata event."""
        return self.produce_event(
            topic=self.topics.get('metadata', 'movie.metadata'),
            key=str(metadata.get('movie_id')),
            value=metadata,
            headers={'event_type': 'metadata'}
        )
    
    def produce_trending_event(self, trending_data: Dict[str, Any]) -> bool:
        """Produce a trending movie event."""
        return self.produce_event(
            topic=self.topics.get('trending', 'movie.trending'),
            key=str(trending_data.get('movie_id')),
            value=trending_data,
            headers={'event_type': 'trending'}
        )
    
    def flush(self, timeout: Optional[float] = None) -> int:
        """
        Flush pending messages.
        
        Args:
            timeout: Maximum time to wait for flush (seconds)
            
        Returns:
            Number of messages still pending
        """
        try:
            if timeout:
                return self.producer.flush(timeout=timeout)
            else:
                return self.producer.flush()
        except Exception as e:
            logger.error(f"Error during flush: {e}")
            return -1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        return {
            'messages_sent': self.message_count,
            'messages_success': self.success_count,
            'messages_error': self.error_count,
            'success_rate': self.success_count / max(1, self.message_count),
            'producer_metrics': self.producer.metrics() if hasattr(self.producer, 'metrics') else {}
        }
    
    def close(self, timeout: Optional[float] = None):
        """Close the producer and flush pending messages."""
        logger.info("Closing event producer...")
        
        try:
            # Flush pending messages
            remaining = self.flush(timeout or 10.0)
            if remaining > 0:
                logger.warning(f"{remaining} messages still pending after flush")
            
            # Close producer
            self.producer.close(timeout or 10.0)
            
            # Log final metrics
            metrics = self.get_metrics()
            logger.info(f"Producer closed. Final metrics: {metrics}")
            
        except Exception as e:
            logger.error(f"Error closing producer: {e}")


class BatchEventProducer(EventProducer):
    """Event producer with batching capabilities for high-throughput scenarios."""
    
    def __init__(self, config: Dict[str, Any], batch_size: int = 100, 
                 batch_timeout: float = 5.0):
        """
        Initialize batch producer.
        
        Args:
            config: Kafka configuration
            batch_size: Number of messages to batch before sending
            batch_timeout: Maximum time to wait before sending partial batch
        """
        super().__init__(config)
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.message_batch = []
        self.last_batch_time = time.time()
    
    def add_to_batch(self, topic: str, key: Optional[str], value: Dict[str, Any],
                     headers: Optional[Dict[str, str]] = None):
        """Add message to batch."""
        message = {
            'topic': topic,
            'key': key,
            'value': value,
            'headers': headers,
            'timestamp': time.time()
        }
        
        self.message_batch.append(message)
        
        # Send batch if size or timeout reached
        if (len(self.message_batch) >= self.batch_size or 
            time.time() - self.last_batch_time >= self.batch_timeout):
            self.send_batch()
    
    def send_batch(self) -> int:
        """Send current batch of messages."""
        if not self.message_batch:
            return 0
        
        sent_count = 0
        batch = self.message_batch.copy()
        self.message_batch.clear()
        self.last_batch_time = time.time()
        
        for message in batch:
            success = self.produce_event(
                message['topic'],
                message['key'],
                message['value'],
                message['headers']
            )
            if success:
                sent_count += 1
        
        logger.debug(f"Sent batch of {sent_count}/{len(batch)} messages")
        return sent_count
    
    def close(self, timeout: Optional[float] = None):
        """Close batch producer and send remaining messages."""
        # Send any remaining messages in batch
        self.send_batch()
        
        # Call parent close
        super().close(timeout)


class ReliableEventProducer(EventProducer):
    """Event producer with enhanced reliability features."""
    
    def __init__(self, config: Dict[str, Any], max_retries: int = 3, 
                 retry_delay: float = 1.0, circuit_breaker_threshold: int = 10):
        """
        Initialize reliable producer.
        
        Args:
            config: Kafka configuration
            max_retries: Maximum number of retries for failed messages
            retry_delay: Delay between retries (seconds)
            circuit_breaker_threshold: Number of consecutive failures to trigger circuit breaker
        """
        super().__init__(config)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.circuit_breaker_threshold = circuit_breaker_threshold
        
        # Circuit breaker state
        self.consecutive_failures = 0
        self.circuit_open = False
        self.last_failure_time = 0
        self.circuit_recovery_timeout = 60.0  # seconds
    
    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker allows requests."""
        if not self.circuit_open:
            return True
        
        # Check if recovery timeout has passed
        if time.time() - self.last_failure_time > self.circuit_recovery_timeout:
            logger.info("Circuit breaker recovery timeout passed, attempting to close")
            self.circuit_open = False
            self.consecutive_failures = 0
            return True
        
        return False
    
    def _handle_success(self):
        """Handle successful message send."""
        if self.consecutive_failures > 0:
            logger.info(f"Producer recovered after {self.consecutive_failures} failures")
        self.consecutive_failures = 0
        self.circuit_open = False
    
    def _handle_failure(self):
        """Handle failed message send."""
        self.consecutive_failures += 1
        self.last_failure_time = time.time()
        
        if self.consecutive_failures >= self.circuit_breaker_threshold:
            logger.error(f"Circuit breaker opened after {self.consecutive_failures} failures")
            self.circuit_open = True
    
    def produce_event_reliable(self, topic: str, key: Optional[str], 
                              value: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Produce event with retry logic and circuit breaker.
        
        Returns:
            bool: True if message was successfully sent
        """
        if not self._check_circuit_breaker():
            logger.warning("Circuit breaker is open, rejecting message")
            return False
        
        for attempt in range(self.max_retries + 1):
            try:
                success = self.produce_event(topic, key, value, headers)
                
                if success:
                    self._handle_success()
                    return True
                else:
                    raise Exception("Producer returned False")
                    
            except Exception as e:
                if attempt < self.max_retries:
                    logger.warning(f"Send attempt {attempt + 1} failed, retrying: {e}")
                    time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"All {self.max_retries + 1} send attempts failed: {e}")
                    self._handle_failure()
                    return False
        
        return False