"""
DataHub configuration for data governance and lineage tracking.
"""
import os
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DataHubConfig:
    """Configuration class for DataHub integration."""
    
    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT', 'development')
        
        # DataHub Server Configuration
        self.datahub_gms_host = os.getenv('DATAHUB_GMS_HOST', 'localhost')
        self.datahub_gms_port = int(os.getenv('DATAHUB_GMS_PORT', '8080'))
        self.datahub_gms_url = f"http://{self.datahub_gms_host}:{self.datahub_gms_port}"
        
        # DataHub Frontend Configuration
        self.datahub_frontend_host = os.getenv('DATAHUB_FRONTEND_HOST', 'localhost')
        self.datahub_frontend_port = int(os.getenv('DATAHUB_FRONTEND_PORT', '9002'))
        self.datahub_frontend_url = f"http://{self.datahub_frontend_host}:{self.datahub_frontend_port}"
        
        # DataHub Authentication
        self.datahub_token = os.getenv('DATAHUB_TOKEN')
        self.datahub_username = os.getenv('DATAHUB_USERNAME', 'datahub')
        self.datahub_password = os.getenv('DATAHUB_PASSWORD', 'datahub')
        
        # Platform Configuration
        self.platform_instance = os.getenv('DATAHUB_PLATFORM_INSTANCE', 'movie-pipeline')
        
        # Data Platform URNs
        self.platforms = {
            'kafka': 'urn:li:dataPlatform:kafka',
            'spark': 'urn:li:dataPlatform:spark',
            'mongodb': 'urn:li:dataPlatform:mongodb',
            'minio': 'urn:li:dataPlatform:s3',
            'tmdb': 'urn:li:dataPlatform:rest'
        }
        
        # Kafka Topics Configuration
        self.kafka_topics = {
            'movies': 'movies',
            'people': 'people', 
            'credits': 'credits',
            'reviews': 'reviews',
            'ratings': 'ratings'
        }
        
        # Storage Layers Configuration
        self.storage_layers = {
            'bronze': 'bronze-data',
            'silver': 'silver-data', 
            'gold': 'gold-data'
        }
        
        # MongoDB Collections Configuration
        self.mongodb_collections = {
            'movies': 'movies',
            'people': 'people',
            'analytics': 'analytics',
            'trends': 'trends'
        }
        
        # Schema Registry Configuration
        self.schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
        
        # Lineage Configuration
        self.enable_lineage_tracking = os.getenv('DATAHUB_ENABLE_LINEAGE', 'true').lower() == 'true'
        self.lineage_batch_size = int(os.getenv('DATAHUB_LINEAGE_BATCH_SIZE', '100'))
        
        # Tags and Glossary Configuration
        self.default_tags = [
            'movie-data',
            'real-time',
            'analytics'
        ]
        
        # Data Quality Configuration
        self.enable_data_quality = os.getenv('DATAHUB_ENABLE_DQ', 'true').lower() == 'true'
        
        # Airflow Integration
        self.airflow_dag_prefix = os.getenv('AIRFLOW_DAG_PREFIX', 'movie')
        
    def get_dataset_urn(self, platform: str, dataset_name: str, env: str = None) -> str:
        """Generate DataHub dataset URN."""
        env = env or self.environment
        platform_urn = self.platforms.get(platform, f'urn:li:dataPlatform:{platform}')
        return f"urn:li:dataset:({platform_urn},{self.platform_instance}.{dataset_name},{env.upper()})"
    
    def get_job_urn(self, platform: str, job_name: str, env: str = None) -> str:
        """Generate DataHub job URN."""
        env = env or self.environment
        return f"urn:li:dataJob:(urn:li:dataFlow:({platform},{self.platform_instance},{env.upper()}),{job_name})"
    
    def get_data_flow_urn(self, platform: str, flow_name: str, env: str = None) -> str:
        """Generate DataHub data flow URN."""
        env = env or self.environment
        return f"urn:li:dataFlow:({platform},{self.platform_instance}.{flow_name},{env.upper()})"
    
    def get_kafka_topic_urns(self) -> Dict[str, str]:
        """Get all Kafka topic URNs."""
        return {
            topic: self.get_dataset_urn('kafka', topic)
            for topic in self.kafka_topics.values()
        }
    
    def get_storage_layer_urns(self) -> Dict[str, str]:
        """Get all storage layer URNs."""
        return {
            layer: self.get_dataset_urn('minio', f"{bucket}/{layer}")
            for layer, bucket in self.storage_layers.items()
        }
    
    def get_mongodb_collection_urns(self) -> Dict[str, str]:
        """Get all MongoDB collection URNs."""
        return {
            collection: self.get_dataset_urn('mongodb', f"moviedb.{collection}")
            for collection in self.mongodb_collections.values()
        }
    
    def get_tmdb_source_urn(self) -> str:
        """Get TMDB API source URN."""
        return self.get_dataset_urn('tmdb', 'api.themoviedb.org')
    
    def get_lineage_config(self) -> Dict[str, Any]:
        """Get lineage tracking configuration."""
        return {
            'enabled': self.enable_lineage_tracking,
            'batch_size': self.lineage_batch_size,
            'platforms': self.platforms,
            'default_tags': self.default_tags
        }
    
    def get_schema_registry_config(self) -> Dict[str, str]:
        """Get schema registry configuration for DataHub."""
        return {
            'url': self.schema_registry_url,
            'enable_schema_metadata': 'true'
        }
    
    def validate(self) -> bool:
        """Validate DataHub configuration."""
        required_configs = [
            ('DATAHUB_GMS_HOST', self.datahub_gms_host),
        ]
        
        missing_configs = []
        for name, value in required_configs:
            if not value:
                missing_configs.append(name)
        
        if missing_configs:
            raise ValueError(f"Missing required DataHub configuration: {', '.join(missing_configs)}")
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'environment': self.environment,
            'datahub_gms_url': self.datahub_gms_url,
            'datahub_frontend_url': self.datahub_frontend_url,
            'platform_instance': self.platform_instance,
            'platforms': self.platforms,
            'kafka_topics': self.kafka_topics,
            'storage_layers': self.storage_layers,
            'mongodb_collections': self.mongodb_collections,
            'schema_registry_url': self.schema_registry_url,
            'enable_lineage_tracking': self.enable_lineage_tracking,
            'enable_data_quality': self.enable_data_quality,
            'default_tags': self.default_tags
        }

# Global DataHub configuration instance
datahub_config = DataHubConfig()