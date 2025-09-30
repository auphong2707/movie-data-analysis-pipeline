"""
Airflow operators for movie data pipeline.
Custom operators that integrate with existing pipeline components.
"""
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context
from airflow.exceptions import AirflowException

import sys
import os

# Add project root to Python path
sys.path.append('/opt/airflow')

class TMDBExtractOperator(BaseOperator):
    """
    Custom operator for TMDB data extraction.
    """
    
    template_fields = ['api_key', 'extraction_type', 'max_pages']
    
    @apply_defaults
    def __init__(
        self,
        api_key: str,
        extraction_type: str = 'trending_movies',
        max_pages: int = 1,
        time_window: str = 'day',
        kafka_servers: str = 'kafka:29092',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_key = api_key
        self.extraction_type = extraction_type
        self.max_pages = max_pages
        self.time_window = time_window
        self.kafka_servers = kafka_servers
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute TMDB data extraction."""
        from src.ingestion.tmdb_client import TMDBClient
        from src.ingestion.kafka_producer import MovieDataProducer
        
        self.log.info(f"Starting TMDB extraction: {self.extraction_type}")
        
        # Initialize clients
        tmdb_client = TMDBClient(api_key=self.api_key)
        kafka_producer = MovieDataProducer(bootstrap_servers=self.kafka_servers)
        
        try:
            results = {'status': 'success', 'records_extracted': 0}
            
            if self.extraction_type == 'trending_movies':
                data = tmdb_client.get_trending_movies(self.time_window)
                movies = data.get('results', [])
                kafka_producer.produce_movies(movies)
                results['records_extracted'] = len(movies)
                
            elif self.extraction_type == 'popular_movies':
                all_movies = []
                for page in range(1, self.max_pages + 1):
                    data = tmdb_client.get_popular_movies(page)
                    movies = data.get('results', [])
                    all_movies.extend(movies)
                
                kafka_producer.produce_movies(all_movies)
                results['records_extracted'] = len(all_movies)
                
            elif self.extraction_type == 'trending_people':
                data = tmdb_client.get_trending_people(self.time_window)
                people = data.get('results', [])
                kafka_producer.produce_people(people)
                results['records_extracted'] = len(people)
                
            else:
                raise AirflowException(f"Unknown extraction type: {self.extraction_type}")
            
            kafka_producer.flush()
            
            self.log.info(f"Extraction completed: {results['records_extracted']} records")
            return results
            
        except Exception as e:
            self.log.error(f"Extraction failed: {e}")
            raise AirflowException(f"TMDB extraction failed: {e}")
        finally:
            kafka_producer.close()

class SparkProcessOperator(BaseOperator):
    """
    Custom operator for Spark data processing.
    """
    
    template_fields = ['spark_master', 'app_name', 'processing_type']
    
    @apply_defaults
    def __init__(
        self,
        spark_master: str = 'spark://spark-master:7077',
        app_name: str = 'MovieDataProcessing',
        processing_type: str = 'clean_movies',
        kafka_servers: str = 'kafka:29092',
        mongodb_uri: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.spark_master = spark_master
        self.app_name = app_name
        self.processing_type = processing_type
        self.kafka_servers = kafka_servers
        self.mongodb_uri = mongodb_uri
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute Spark data processing."""
        from src.streaming.spark_config import get_spark_session
        from src.streaming.data_cleaner import DataCleaner
        
        self.log.info(f"Starting Spark processing: {self.processing_type}")
        
        # Get Spark session
        spark = get_spark_session(self.app_name, master=self.spark_master)
        
        try:
            results = {'status': 'success', 'records_processed': 0}
            
            if self.processing_type == 'clean_movies':
                # Initialize data cleaner
                cleaner = DataCleaner(spark)
                
                # Read from Kafka (batch mode)
                df = spark.read \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", self.kafka_servers) \
                    .option("subscribe", "movies") \
                    .option("startingOffsets", "earliest") \
                    .option("endingOffsets", "latest") \
                    .load()
                
                if df.count() > 0:
                    # Clean data
                    cleaned_df = cleaner.clean_movie_data(df)
                    
                    # Write to MongoDB
                    if self.mongodb_uri:
                        cleaned_df.write \
                            .format("mongo") \
                            .option("uri", self.mongodb_uri) \
                            .option("database", "moviedb") \
                            .option("collection", "processed_movies") \
                            .mode("append") \
                            .save()
                    
                    results['records_processed'] = cleaned_df.count()
                
            elif self.processing_type == 'sentiment_analysis':
                from src.streaming.sentiment_analyzer import SentimentAnalyzer
                
                # Initialize sentiment analyzer
                analyzer = SentimentAnalyzer(spark)
                
                # Read reviews from Kafka
                df = spark.read \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", self.kafka_servers) \
                    .option("subscribe", "reviews") \
                    .option("startingOffsets", "earliest") \
                    .option("endingOffsets", "latest") \
                    .load()
                
                if df.count() > 0:
                    # Analyze sentiment
                    analyzed_df = analyzer.analyze_review_sentiment(df)
                    
                    # Write to MongoDB
                    if self.mongodb_uri:
                        analyzed_df.write \
                            .format("mongo") \
                            .option("uri", self.mongodb_uri) \
                            .option("database", "moviedb") \
                            .option("collection", "sentiment_analysis") \
                            .mode("append") \
                            .save()
                    
                    results['records_processed'] = analyzed_df.count()
            
            else:
                raise AirflowException(f"Unknown processing type: {self.processing_type}")
            
            self.log.info(f"Processing completed: {results['records_processed']} records")
            return results
            
        except Exception as e:
            self.log.error(f"Processing failed: {e}")
            raise AirflowException(f"Spark processing failed: {e}")
        finally:
            spark.stop()

class MongoDBValidationOperator(BaseOperator):
    """
    Custom operator for MongoDB data validation.
    """
    
    template_fields = ['mongodb_uri', 'collection_name']
    
    @apply_defaults
    def __init__(
        self,
        mongodb_uri: str,
        collection_name: str,
        validation_rules: Dict[str, Any] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.mongodb_uri = mongodb_uri
        self.collection_name = collection_name
        self.validation_rules = validation_rules or {}
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute MongoDB data validation."""
        from pymongo import MongoClient
        
        self.log.info(f"Starting MongoDB validation for collection: {self.collection_name}")
        
        client = MongoClient(self.mongodb_uri)
        
        try:
            db = client.moviedb
            collection = db[self.collection_name]
            
            # Basic validation
            total_docs = collection.count_documents({})
            
            if total_docs == 0:
                self.log.warning(f"No documents found in collection {self.collection_name}")
                return {'status': 'warning', 'total_documents': 0, 'valid_documents': 0}
            
            # Sample documents for validation
            sample_size = min(100, total_docs)
            sample_docs = list(collection.aggregate([{'$sample': {'size': sample_size}}]))
            
            valid_docs = 0
            validation_errors = []
            
            # Apply validation rules
            required_fields = self.validation_rules.get('required_fields', [])
            
            for doc in sample_docs:
                doc_valid = True
                for field in required_fields:
                    if field not in doc or doc[field] is None:
                        doc_valid = False
                        validation_errors.append(f"Missing field '{field}' in document {doc.get('_id')}")
                        break
                
                if doc_valid:
                    valid_docs += 1
            
            validation_score = valid_docs / len(sample_docs) if sample_docs else 0
            
            result = {
                'status': 'success' if validation_score > 0.9 else 'warning',
                'total_documents': total_docs,
                'sampled_documents': len(sample_docs),
                'valid_documents': valid_docs,
                'validation_score': validation_score,
                'errors': validation_errors[:10]  # Limit errors
            }
            
            self.log.info(f"Validation completed: {validation_score:.2f} score")
            return result
            
        except Exception as e:
            self.log.error(f"Validation failed: {e}")
            raise AirflowException(f"MongoDB validation failed: {e}")
        finally:
            client.close()

class DataQualityBranchOperator(BaseOperator):
    """
    Custom operator that branches based on data quality results.
    """
    
    template_fields = ['quality_threshold']
    
    @apply_defaults
    def __init__(
        self,
        quality_threshold: float = 0.8,
        success_task_id: str = 'quality_success',
        failure_task_id: str = 'quality_failure',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.quality_threshold = quality_threshold
        self.success_task_id = success_task_id
        self.failure_task_id = failure_task_id
    
    def execute(self, context: Context) -> str:
        """Branch based on data quality results."""
        # Get quality results from previous tasks
        validation_results = []
        
        # Check for validation task results
        for task_id in ['validate_movies', 'validate_sentiment', 'validate_analytics']:
            try:
                result = context['task_instance'].xcom_pull(task_ids=task_id)
                if result and 'validation_score' in result:
                    validation_results.append(result['validation_score'])
            except:
                continue
        
        if not validation_results:
            self.log.warning("No validation results found, defaulting to failure branch")
            return self.failure_task_id
        
        avg_quality = sum(validation_results) / len(validation_results)
        
        if avg_quality >= self.quality_threshold:
            self.log.info(f"Data quality passed: {avg_quality:.2f} >= {self.quality_threshold}")
            return self.success_task_id
        else:
            self.log.warning(f"Data quality failed: {avg_quality:.2f} < {self.quality_threshold}")
            return self.failure_task_id

class KafkaHealthCheckOperator(BaseOperator):
    """
    Custom operator for Kafka health checking.
    """
    
    template_fields = ['kafka_servers']
    
    @apply_defaults
    def __init__(
        self,
        kafka_servers: str = 'kafka:29092',
        timeout: int = 30,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kafka_servers = kafka_servers
        self.timeout = timeout
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute Kafka health check."""
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import NoBrokersAvailable
        
        self.log.info(f"Checking Kafka health at {self.kafka_servers}")
        
        try:
            # Test producer connection
            producer = KafkaProducer(
                bootstrap_servers=[self.kafka_servers],
                request_timeout_ms=self.timeout * 1000,
                api_version_auto_timeout_ms=self.timeout * 1000
            )
            
            # Test basic functionality
            producer.send('health_check', b'test_message')
            producer.flush(timeout=self.timeout)
            producer.close()
            
            # Test consumer connection
            consumer = KafkaConsumer(
                bootstrap_servers=[self.kafka_servers],
                consumer_timeout_ms=5000
            )
            
            # Get available topics
            topics = consumer.topics()
            consumer.close()
            
            result = {
                'status': 'healthy',
                'kafka_servers': self.kafka_servers,
                'available_topics': list(topics),
                'topics_count': len(topics)
            }
            
            self.log.info(f"Kafka health check passed. Topics: {len(topics)}")
            return result
            
        except NoBrokersAvailable:
            self.log.error("No Kafka brokers available")
            raise AirflowException("Kafka health check failed: No brokers available")
        except Exception as e:
            self.log.error(f"Kafka health check failed: {e}")
            raise AirflowException(f"Kafka health check failed: {e}")