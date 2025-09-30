"""
Airflow hooks for movie data pipeline.
Custom hooks that provide connection interfaces to external systems.
"""
from typing import Dict, Any, List, Optional
import logging

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

import sys
import os

# Add project root to Python path
sys.path.append('/opt/airflow')

class TMDBHook(BaseHook):
    """
    Hook for TMDB API interactions.
    """
    
    conn_name_attr = 'tmdb_conn_id'
    default_conn_name = 'tmdb_default'
    conn_type = 'tmdb'
    hook_name = 'TMDB'
    
    def __init__(self, tmdb_conn_id: str = default_conn_name):
        super().__init__()
        self.tmdb_conn_id = tmdb_conn_id
        self._client = None
    
    def get_connection(self):
        """Get TMDB connection."""
        return self.get_connection(self.tmdb_conn_id)
    
    def get_client(self):
        """Get TMDB client instance."""
        if self._client is None:
            from src.ingestion.tmdb_client import TMDBClient
            
            # Get API key from connection or environment
            try:
                conn = self.get_connection()
                api_key = conn.password or conn.extra_dejson.get('api_key')
            except:
                api_key = os.getenv('TMDB_API_KEY')
            
            if not api_key:
                raise AirflowException("TMDB API key not found in connection or environment")
            
            self._client = TMDBClient(api_key=api_key)
        
        return self._client
    
    def test_connection(self) -> Dict[str, Any]:
        """Test TMDB connection."""
        try:
            client = self.get_client()
            # Test with a simple API call
            result = client.get_trending_movies('day')
            
            return {
                'status': 'success',
                'message': 'TMDB connection successful',
                'test_results': len(result.get('results', []))
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'TMDB connection failed: {e}'
            }
    
    def extract_trending_movies(self, time_window: str = 'day') -> List[Dict[str, Any]]:
        """Extract trending movies."""
        client = self.get_client()
        data = client.get_trending_movies(time_window)
        return data.get('results', [])
    
    def extract_popular_movies(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        """Extract popular movies."""
        client = self.get_client()
        all_movies = []
        
        for page in range(1, max_pages + 1):
            data = client.get_popular_movies(page)
            movies = data.get('results', [])
            all_movies.extend(movies)
        
        return all_movies
    
    def extract_trending_people(self, time_window: str = 'day') -> List[Dict[str, Any]]:
        """Extract trending people."""
        client = self.get_client()
        data = client.get_trending_people(time_window)
        return data.get('results', [])

class KafkaHook(BaseHook):
    """
    Hook for Kafka interactions.
    """
    
    conn_name_attr = 'kafka_conn_id'
    default_conn_name = 'kafka_default'
    conn_type = 'kafka'
    hook_name = 'Kafka'
    
    def __init__(self, kafka_conn_id: str = default_conn_name):
        super().__init__()
        self.kafka_conn_id = kafka_conn_id
        self._producer = None
        self._consumer = None
    
    def get_connection(self):
        """Get Kafka connection."""
        try:
            return self.get_connection(self.kafka_conn_id)
        except:
            # Return default connection info
            class DefaultConn:
                host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
                extra_dejson = {}
            return DefaultConn()
    
    def get_producer(self):
        """Get Kafka producer instance."""
        if self._producer is None:
            from src.ingestion.kafka_producer import MovieDataProducer
            
            conn = self.get_connection()
            bootstrap_servers = conn.host
            
            self._producer = MovieDataProducer(bootstrap_servers=bootstrap_servers)
        
        return self._producer
    
    def get_consumer(self, topics: List[str] = None, group_id: str = 'airflow_consumer'):
        """Get Kafka consumer instance."""
        from kafka import KafkaConsumer
        
        conn = self.get_connection()
        bootstrap_servers = conn.host
        
        consumer = KafkaConsumer(
            *topics if topics else [],
            bootstrap_servers=[bootstrap_servers],
            group_id=group_id,
            enable_auto_commit=False
        )
        
        return consumer
    
    def test_connection(self) -> Dict[str, Any]:
        """Test Kafka connection."""
        try:
            from kafka import KafkaProducer
            
            conn = self.get_connection()
            bootstrap_servers = conn.host
            
            # Test producer connection
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                request_timeout_ms=10000
            )
            producer.close()
            
            # Get available topics
            consumer = self.get_consumer()
            topics = consumer.topics()
            consumer.close()
            
            return {
                'status': 'success',
                'message': 'Kafka connection successful',
                'bootstrap_servers': bootstrap_servers,
                'available_topics': list(topics)
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Kafka connection failed: {e}'
            }
    
    def produce_data(self, topic: str, data: List[Dict[str, Any]], key_field: str = 'id'):
        """Produce data to Kafka topic."""
        producer = self.get_producer()
        
        for record in data:
            if topic == 'movies':
                producer.produce_movie_data('movies', record, str(record.get(key_field, '')))
            elif topic == 'people':
                producer.produce_movie_data('people', record, str(record.get(key_field, '')))
            elif topic == 'credits':
                producer.produce_credits(record, record.get('movie_id'))
            elif topic == 'reviews':
                producer.produce_reviews([record], record.get('movie_id'))
        
        producer.flush()
    
    def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """Get information about a Kafka topic."""
        consumer = self.get_consumer()
        
        try:
            partitions = consumer.partitions_for_topic(topic)
            
            if partitions is None:
                return {'status': 'not_found', 'topic': topic}
            
            # Get partition info
            from kafka.structs import TopicPartition
            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            
            # Get latest offsets
            latest_offsets = consumer.end_offsets(topic_partitions)
            earliest_offsets = consumer.beginning_offsets(topic_partitions)
            
            total_messages = sum(latest_offsets.values()) - sum(earliest_offsets.values())
            
            return {
                'status': 'found',
                'topic': topic,
                'partitions': len(partitions),
                'total_messages': total_messages,
                'latest_offsets': latest_offsets,
                'earliest_offsets': earliest_offsets
            }
        finally:
            consumer.close()

class MongoDBHook(BaseHook):
    """
    Hook for MongoDB interactions.
    """
    
    conn_name_attr = 'mongodb_conn_id'
    default_conn_name = 'mongodb_default'
    conn_type = 'mongodb'
    hook_name = 'MongoDB'
    
    def __init__(self, mongodb_conn_id: str = default_conn_name):
        super().__init__()
        self.mongodb_conn_id = mongodb_conn_id
        self._client = None
    
    def get_connection(self):
        """Get MongoDB connection."""
        try:
            return self.get_connection(self.mongodb_conn_id)
        except:
            # Return default connection info
            class DefaultConn:
                host = os.getenv('MONGODB_URI', 'mongodb://admin:password@mongodb:27017/moviedb?authSource=admin')
                extra_dejson = {'database': 'moviedb'}
            return DefaultConn()
    
    def get_client(self):
        """Get MongoDB client instance."""
        if self._client is None:
            from pymongo import MongoClient
            
            conn = self.get_connection()
            uri = conn.host
            
            self._client = MongoClient(uri)
        
        return self._client
    
    def get_database(self, database_name: str = None):
        """Get MongoDB database."""
        client = self.get_client()
        
        if database_name is None:
            conn = self.get_connection()
            database_name = conn.extra_dejson.get('database', 'moviedb')
        
        return client[database_name]
    
    def test_connection(self) -> Dict[str, Any]:
        """Test MongoDB connection."""
        try:
            client = self.get_client()
            
            # Test connection
            client.admin.command('ping')
            
            # Get database info
            db = self.get_database()
            collections = db.list_collection_names()
            
            return {
                'status': 'success',
                'message': 'MongoDB connection successful',
                'database': db.name,
                'collections': collections
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'MongoDB connection failed: {e}'
            }
    
    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Get statistics for a MongoDB collection."""
        try:
            db = self.get_database()
            collection = db[collection_name]
            
            # Get basic stats
            stats = db.command('collStats', collection_name)
            
            # Get document count
            doc_count = collection.count_documents({})
            
            # Get latest document
            latest_doc = collection.find().sort([('_id', -1)]).limit(1)
            latest_doc = list(latest_doc)
            
            latest_timestamp = None
            if latest_doc:
                latest_timestamp = latest_doc[0]['_id'].generation_time.isoformat()
            
            return {
                'status': 'success',
                'collection': collection_name,
                'document_count': doc_count,
                'size_bytes': stats.get('size', 0),
                'storage_size_bytes': stats.get('storageSize', 0),
                'index_count': stats.get('nindexes', 0),
                'latest_document_time': latest_timestamp
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Failed to get collection stats: {e}'
            }
    
    def validate_collection_schema(self, collection_name: str, required_fields: List[str]) -> Dict[str, Any]:
        """Validate collection schema."""
        try:
            db = self.get_database()
            collection = db[collection_name]
            
            # Sample documents
            sample_size = min(100, collection.count_documents({}))
            if sample_size == 0:
                return {
                    'status': 'warning',
                    'message': 'No documents found',
                    'validation_score': 0.0
                }
            
            sample_docs = list(collection.aggregate([{'$sample': {'size': sample_size}}]))
            
            valid_docs = 0
            validation_errors = []
            
            for doc in sample_docs:
                doc_valid = True
                for field in required_fields:
                    if field not in doc or doc[field] is None:
                        doc_valid = False
                        validation_errors.append(f"Missing field '{field}' in document {doc.get('_id')}")
                        break
                
                if doc_valid:
                    valid_docs += 1
            
            validation_score = valid_docs / len(sample_docs)
            
            return {
                'status': 'success',
                'collection': collection_name,
                'sampled_documents': len(sample_docs),
                'valid_documents': valid_docs,
                'validation_score': validation_score,
                'errors': validation_errors[:10]  # Limit errors
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Schema validation failed: {e}'
            }
    
    def cleanup_old_documents(self, collection_name: str, field_name: str, days_old: int = 30) -> Dict[str, Any]:
        """Clean up old documents from a collection."""
        try:
            from datetime import datetime, timedelta
            
            db = self.get_database()
            collection = db[collection_name]
            
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            # Delete old documents
            if field_name == '_id':
                # Use ObjectId generation time
                result = collection.delete_many({
                    '_id': {'$lt': cutoff_date}
                })
            else:
                # Use specific date field
                result = collection.delete_many({
                    field_name: {'$lt': cutoff_date.isoformat()}
                })
            
            return {
                'status': 'success',
                'collection': collection_name,
                'deleted_documents': result.deleted_count,
                'cutoff_date': cutoff_date.isoformat()
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Cleanup failed: {e}'
            }

class SparkHook(BaseHook):
    """
    Hook for Spark interactions.
    """
    
    conn_name_attr = 'spark_conn_id'
    default_conn_name = 'spark_default'
    conn_type = 'spark'
    hook_name = 'Spark'
    
    def __init__(self, spark_conn_id: str = default_conn_name):
        super().__init__()
        self.spark_conn_id = spark_conn_id
        self._session = None
    
    def get_connection(self):
        """Get Spark connection."""
        try:
            return self.get_connection(self.spark_conn_id)
        except:
            # Return default connection info
            class DefaultConn:
                host = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
                extra_dejson = {}\n            return DefaultConn()\n    \n    def get_spark_session(self, app_name: str = 'AirflowSparkApp'):\n        \"\"\"Get Spark session.\"\"\"\n        if self._session is None:\n            from src.streaming.spark_config import get_spark_session\n            \n            conn = self.get_connection()\n            master_url = conn.host\n            \n            self._session = get_spark_session(app_name, master=master_url)\n        \n        return self._session\n    \n    def test_connection(self) -> Dict[str, Any]:\n        \"\"\"Test Spark connection.\"\"\"\n        try:\n            spark = self.get_spark_session('HealthCheck')\n            \n            # Simple test\n            test_df = spark.range(10)\n            count = test_df.count()\n            \n            return {\n                'status': 'success',\n                'message': 'Spark connection successful',\n                'master_url': spark.sparkContext.master,\n                'test_count': count\n            }\n        except Exception as e:\n            return {\n                'status': 'error',\n                'message': f'Spark connection failed: {e}'\n            }\n    \n    def stop_session(self):\n        \"\"\"Stop Spark session.\"\"\"\n        if self._session is not None:\n            self._session.stop()\n            self._session = None