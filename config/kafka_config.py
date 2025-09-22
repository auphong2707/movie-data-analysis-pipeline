"""
Kafka topic management and configuration.
"""
import json
import logging
from typing import Dict, List, Optional, Any
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from confluent_kafka import KafkaException

logger = logging.getLogger(__name__)

class KafkaTopicManager:
    """Manages Kafka topics for the movie data pipeline."""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({
            'bootstrap.servers': bootstrap_servers
        })
        
        # Topic configurations for different data types
        self.topic_configs = {
            'movies': {
                'num_partitions': 6,
                'replication_factor': 1,
                'config': {
                    'cleanup.policy': 'compact',
                    'compression.type': 'gzip',
                    'retention.ms': '604800000',  # 7 days
                    'segment.ms': '86400000',      # 1 day
                    'max.message.bytes': '1048576' # 1MB
                }
            },
            'people': {
                'num_partitions': 4,
                'replication_factor': 1,
                'config': {
                    'cleanup.policy': 'compact',
                    'compression.type': 'gzip',
                    'retention.ms': '604800000',
                    'segment.ms': '86400000',
                    'max.message.bytes': '1048576'
                }
            },
            'credits': {
                'num_partitions': 8,
                'replication_factor': 1,
                'config': {
                    'cleanup.policy': 'delete',
                    'compression.type': 'gzip',
                    'retention.ms': '2592000000',  # 30 days
                    'segment.ms': '86400000',
                    'max.message.bytes': '512000'  # 512KB
                }
            },
            'reviews': {
                'num_partitions': 6,
                'replication_factor': 1,
                'config': {
                    'cleanup.policy': 'delete',
                    'compression.type': 'gzip',
                    'retention.ms': '2592000000',  # 30 days
                    'segment.ms': '86400000',
                    'max.message.bytes': '2097152'  # 2MB (reviews can be long)
                }
            },
            'ratings': {
                'num_partitions': 4,
                'replication_factor': 1,
                'config': {
                    'cleanup.policy': 'delete',
                    'compression.type': 'gzip',
                    'retention.ms': '2592000000',
                    'segment.ms': '86400000',
                    'max.message.bytes': '256000'  # 256KB
                }
            }
        }
    
    def create_topics(self, topics: Optional[List[str]] = None) -> Dict[str, bool]:
        """Create Kafka topics for the pipeline."""
        if topics is None:
            topics = list(self.topic_configs.keys())
        
        logger.info(f"Creating Kafka topics: {topics}")
        
        # Create NewTopic objects
        new_topics = []
        for topic_name in topics:
            if topic_name not in self.topic_configs:
                logger.warning(f"Unknown topic configuration: {topic_name}")
                continue
            
            config = self.topic_configs[topic_name]
            new_topic = NewTopic(
                topic=topic_name,
                num_partitions=config['num_partitions'],
                replication_factor=config['replication_factor'],
                config=config.get('config', {})
            )
            new_topics.append(new_topic)
        
        # Create topics
        results = {}
        try:
            futures = self.admin_client.create_topics(new_topics)
            
            for topic_name, future in futures.items():
                try:
                    future.result(timeout=30)
                    results[topic_name] = True
                    logger.info(f"Topic '{topic_name}' created successfully")
                except KafkaException as e:
                    if e.args[0].code() == 36:  # Topic already exists
                        results[topic_name] = True
                        logger.info(f"Topic '{topic_name}' already exists")
                    else:
                        results[topic_name] = False
                        logger.error(f"Failed to create topic '{topic_name}': {e}")
                except Exception as e:
                    results[topic_name] = False
                    logger.error(f"Failed to create topic '{topic_name}': {e}")
        
        except Exception as e:
            logger.error(f"Error creating topics: {e}")
            for topic_name in topics:
                results[topic_name] = False
        
        return results
    
    def delete_topics(self, topics: List[str]) -> Dict[str, bool]:
        """Delete Kafka topics."""
        logger.info(f"Deleting Kafka topics: {topics}")
        
        results = {}
        try:
            futures = self.admin_client.delete_topics(topics)
            
            for topic_name, future in futures.items():
                try:
                    future.result(timeout=30)
                    results[topic_name] = True
                    logger.info(f"Topic '{topic_name}' deleted successfully")
                except Exception as e:
                    results[topic_name] = False
                    logger.error(f"Failed to delete topic '{topic_name}': {e}")
        
        except Exception as e:
            logger.error(f"Error deleting topics: {e}")
            for topic_name in topics:
                results[topic_name] = False
        
        return results
    
    def list_topics(self) -> Dict[str, Any]:
        """List all topics in the cluster."""
        try:
            metadata = self.admin_client.list_topics(timeout=30)
            topics_info = {}
            
            for topic_name, topic_metadata in metadata.topics.items():
                topics_info[topic_name] = {
                    'partitions': len(topic_metadata.partitions),
                    'error': topic_metadata.error
                }
            
            return topics_info
        
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return {}
    
    def get_topic_config(self, topic_name: str) -> Optional[Dict[str, str]]:
        """Get configuration for a specific topic."""
        try:
            resource = ConfigResource(ResourceType.TOPIC, topic_name)
            futures = self.admin_client.describe_configs([resource])
            
            configs = futures[resource].result(timeout=30)
            return {config.name: config.value for config in configs.values()}
        
        except Exception as e:
            logger.error(f"Error getting config for topic '{topic_name}': {e}")
            return None
    
    def update_topic_config(self, topic_name: str, config_updates: Dict[str, str]) -> bool:
        """Update configuration for a specific topic."""
        try:
            resource = ConfigResource(ResourceType.TOPIC, topic_name)
            futures = self.admin_client.alter_configs([resource], config_updates)
            
            futures[resource].result(timeout=30)
            logger.info(f"Updated config for topic '{topic_name}': {config_updates}")
            return True
        
        except Exception as e:
            logger.error(f"Error updating config for topic '{topic_name}': {e}")
            return False
    
    def check_topic_health(self, topic_name: str) -> Dict[str, Any]:
        """Check health status of a topic."""
        try:
            metadata = self.admin_client.list_topics(timeout=30)
            
            if topic_name not in metadata.topics:
                return {'exists': False, 'error': 'Topic not found'}
            
            topic_metadata = metadata.topics[topic_name]
            
            health_info = {
                'exists': True,
                'partitions': len(topic_metadata.partitions),
                'partition_health': {}
            }
            
            for partition_id, partition_metadata in topic_metadata.partitions.items():
                health_info['partition_health'][partition_id] = {
                    'leader': partition_metadata.leader,
                    'replicas': partition_metadata.replicas,
                    'isrs': partition_metadata.isrs,
                    'error': partition_metadata.error
                }
            
            return health_info
        
        except Exception as e:
            logger.error(f"Error checking health for topic '{topic_name}': {e}")
            return {'exists': False, 'error': str(e)}
    
    def setup_pipeline_topics(self) -> bool:
        """Setup all topics required for the movie data pipeline."""
        logger.info("Setting up all pipeline topics...")
        
        # Create all topics
        results = self.create_topics()
        
        # Check if all topics were created successfully
        success = all(results.values())
        
        if success:
            logger.info("All pipeline topics setup successfully")
        else:
            failed_topics = [topic for topic, success in results.items() if not success]
            logger.error(f"Failed to setup topics: {failed_topics}")
        
        return success