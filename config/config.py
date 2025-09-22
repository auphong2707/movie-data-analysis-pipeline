"""
Configuration management for the movie analytics pipeline.
"""
import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for the movie analytics pipeline."""
    
    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT', 'development')
        
        # API Configuration
        self.tmdb_api_key = os.getenv('TMDB_API_KEY')
        self.tmdb_base_url = os.getenv('TMDB_BASE_URL', 'https://api.themoviedb.org/3')
        
        # Kafka Configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_topics = {
            'movies': os.getenv('KAFKA_TOPICS_MOVIES', 'movies'),
            'people': os.getenv('KAFKA_TOPICS_PEOPLE', 'people'),
            'credits': os.getenv('KAFKA_TOPICS_CREDITS', 'credits'),
            'reviews': os.getenv('KAFKA_TOPICS_REVIEWS', 'reviews'),
            'ratings': os.getenv('KAFKA_TOPICS_RATINGS', 'ratings')
        }
        self.schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
        
        # Storage Configuration
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.minio_buckets = {
            'bronze': os.getenv('MINIO_BUCKET_BRONZE', 'bronze-data'),
            'silver': os.getenv('MINIO_BUCKET_SILVER', 'silver-data'),
            'gold': os.getenv('MINIO_BUCKET_GOLD', 'gold-data')
        }
        
        # MongoDB Configuration
        self.mongodb_connection_string = os.getenv(
            'MONGODB_CONNECTION_STRING', 
            'mongodb://admin:password@localhost:27017/moviedb?authSource=admin'
        )
        self.mongodb_database = os.getenv('MONGODB_DATABASE', 'moviedb')
        
        # Spark Configuration
        self.spark_master_url = os.getenv('SPARK_MASTER_URL', 'spark://localhost:7077')
        self.spark_app_name = os.getenv('SPARK_APP_NAME', 'movie-analytics-pipeline')
        
        # Monitoring Configuration
        self.grafana_url = os.getenv('GRAFANA_URL', 'http://localhost:3000')
        self.grafana_username = os.getenv('GRAFANA_USERNAME', 'admin')
        self.grafana_password = os.getenv('GRAFANA_PASSWORD', 'admin')
        
        # Superset Configuration
        self.superset_url = os.getenv('SUPERSET_URL', 'http://localhost:8088')
        
        # Logging Configuration
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.log_format = os.getenv(
            'LOG_FORMAT', 
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def validate(self) -> bool:
        """Validate that all required configuration is present."""
        required_configs = [
            ('TMDB_API_KEY', self.tmdb_api_key),
        ]
        
        missing_configs = []
        for name, value in required_configs:
            if not value:
                missing_configs.append(name)
        
        if missing_configs:
            raise ValueError(f"Missing required configuration: {', '.join(missing_configs)}")
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'environment': self.environment,
            'tmdb_base_url': self.tmdb_base_url,
            'kafka_bootstrap_servers': self.kafka_bootstrap_servers,
            'kafka_topics': self.kafka_topics,
            'schema_registry_url': self.schema_registry_url,
            'minio_endpoint': self.minio_endpoint,
            'minio_buckets': self.minio_buckets,
            'mongodb_database': self.mongodb_database,
            'spark_master_url': self.spark_master_url,
            'spark_app_name': self.spark_app_name,
            'grafana_url': self.grafana_url,
            'superset_url': self.superset_url,
            'log_level': self.log_level
        }

# Global configuration instance
config = Config()