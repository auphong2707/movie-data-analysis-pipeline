"""
Kafka Topics Setup Script
Creates and configures Kafka topics with proper partitions, replication, and retention.
"""

import logging
import sys
import os
from typing import Dict, List
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from confluent_kafka import KafkaException

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaTopicSetup:
    """Setup and configure Kafka topics for Speed Layer."""
    
    def __init__(self, bootstrap_servers: str = None):
        """Initialize Kafka admin client."""
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        logger.info(f"Connecting to Kafka: {bootstrap_servers}")
        self.admin_client = AdminClient({
            'bootstrap.servers': bootstrap_servers
        })
        
        # Topic configurations as per requirements
        self.topic_configs = {
            'movie.reviews': {
                'num_partitions': 6,
                'replication_factor': 3,
                'config': {
                    'cleanup.policy': 'delete',
                    'retention.ms': '604800000',  # 7 days
                    'compression.type': 'gzip',
                    'max.message.bytes': '2097152',  # 2MB (reviews can be long)
                    'segment.ms': '86400000'  # 1 day
                }
            },
            'movie.ratings': {
                'num_partitions': 6,
                'replication_factor': 3,
                'config': {
                    'cleanup.policy': 'delete',
                    'retention.ms': '604800000',  # 7 days
                    'compression.type': 'gzip',
                    'max.message.bytes': '1048576',  # 1MB
                    'segment.ms': '86400000'
                }
            },
            'movie.metadata': {
                'num_partitions': 6,
                'replication_factor': 3,
                'config': {
                    'cleanup.policy': 'compact,delete',  # Compact + delete for latest state
                    'retention.ms': '604800000',  # 7 days
                    'compression.type': 'gzip',
                    'max.message.bytes': '1048576',  # 1MB
                    'segment.ms': '86400000',
                    'min.compaction.lag.ms': '3600000'  # 1 hour
                }
            }
        }
    
    def create_topics(self, topics: List[str] = None) -> Dict[str, bool]:
        """
        Create Kafka topics with configurations.
        
        Args:
            topics: List of topic names to create. If None, creates all topics.
            
        Returns:
            Dictionary mapping topic names to success status
        """
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
        fs = self.admin_client.create_topics(new_topics, validate_only=False)
        
        for topic, future in fs.items():
            try:
                future.result()
                logger.info(f"✓ Topic '{topic}' created successfully")
                results[topic] = True
            except KafkaException as e:
                if e.args[0].code() == 36:  # Topic already exists
                    logger.info(f"→ Topic '{topic}' already exists")
                    results[topic] = True
                else:
                    logger.error(f"✗ Failed to create topic '{topic}': {e}")
                    results[topic] = False
            except Exception as e:
                logger.error(f"✗ Failed to create topic '{topic}': {e}")
                results[topic] = False
        
        return results
    
    def list_topics(self) -> Dict[str, any]:
        """List all topics in the cluster."""
        metadata = self.admin_client.list_topics(timeout=10)
        return {topic: metadata.topics[topic] for topic in metadata.topics}
    
    def describe_topic(self, topic_name: str) -> Dict[str, any]:
        """Get detailed information about a topic."""
        metadata = self.admin_client.list_topics(topic=topic_name, timeout=10)
        
        if topic_name not in metadata.topics:
            logger.error(f"Topic '{topic_name}' not found")
            return {}
        
        topic = metadata.topics[topic_name]
        
        info = {
            'name': topic_name,
            'partitions': len(topic.partitions),
            'partition_details': []
        }
        
        for partition_id, partition in topic.partitions.items():
            info['partition_details'].append({
                'id': partition_id,
                'leader': partition.leader,
                'replicas': partition.replicas,
                'isrs': partition.isrs
            })
        
        return info
    
    def verify_topics(self) -> bool:
        """Verify that all required topics exist with correct configuration."""
        logger.info("Verifying Kafka topics...")
        
        all_valid = True
        for topic_name, expected_config in self.topic_configs.items():
            info = self.describe_topic(topic_name)
            
            if not info:
                logger.error(f"✗ Topic '{topic_name}' does not exist")
                all_valid = False
                continue
            
            # Check partitions
            if info['partitions'] != expected_config['num_partitions']:
                logger.warning(
                    f"⚠ Topic '{topic_name}' has {info['partitions']} partitions, "
                    f"expected {expected_config['num_partitions']}"
                )
                all_valid = False
            else:
                logger.info(f"✓ Topic '{topic_name}' has correct partition count")
        
        return all_valid
    
    def delete_topics(self, topics: List[str]) -> Dict[str, bool]:
        """
        Delete Kafka topics (use with caution!).
        
        Args:
            topics: List of topic names to delete
            
        Returns:
            Dictionary mapping topic names to success status
        """
        logger.warning(f"Deleting Kafka topics: {topics}")
        
        results = {}
        fs = self.admin_client.delete_topics(topics, operation_timeout=30)
        
        for topic, future in fs.items():
            try:
                future.result()
                logger.info(f"✓ Topic '{topic}' deleted successfully")
                results[topic] = True
            except Exception as e:
                logger.error(f"✗ Failed to delete topic '{topic}': {e}")
                results[topic] = False
        
        return results


def main():
    """Main entry point for topic setup."""
    import os
    
    # Get Kafka bootstrap servers from environment
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Create topic setup instance
    setup = KafkaTopicSetup(bootstrap_servers)
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == 'create':
            # Create all topics
            results = setup.create_topics()
            success_count = sum(1 for success in results.values() if success)
            logger.info(f"\nCreated {success_count}/{len(results)} topics successfully")
            
        elif command == 'verify':
            # Verify topics
            if setup.verify_topics():
                logger.info("\n✓ All topics are correctly configured")
            else:
                logger.error("\n✗ Some topics have configuration issues")
                sys.exit(1)
                
        elif command == 'list':
            # List all topics
            topics = setup.list_topics()
            logger.info(f"\nFound {len(topics)} topics:")
            for topic_name in sorted(topics.keys()):
                logger.info(f"  - {topic_name}")
                
        elif command == 'describe':
            # Describe specific topics
            topics_to_describe = sys.argv[2:] if len(sys.argv) > 2 else list(setup.topic_configs.keys())
            for topic_name in topics_to_describe:
                info = setup.describe_topic(topic_name)
                if info:
                    logger.info(f"\n{topic_name}:")
                    logger.info(f"  Partitions: {info['partitions']}")
                    for partition in info['partition_details']:
                        logger.info(f"    Partition {partition['id']}: leader={partition['leader']}, "
                                  f"replicas={partition['replicas']}, ISRs={partition['isrs']}")
                        
        elif command == 'delete':
            # Delete topics (requires explicit confirmation)
            topics_to_delete = sys.argv[2:] if len(sys.argv) > 2 else []
            if not topics_to_delete:
                logger.error("Please specify topics to delete")
                sys.exit(1)
            
            confirmation = input(f"Are you sure you want to delete topics {topics_to_delete}? (yes/no): ")
            if confirmation.lower() == 'yes':
                setup.delete_topics(topics_to_delete)
            else:
                logger.info("Deletion cancelled")
                
        else:
            logger.error(f"Unknown command: {command}")
            print_usage()
            sys.exit(1)
    else:
        print_usage()


def print_usage():
    """Print usage information."""
    print("""
Kafka Topics Setup Script

Usage:
    python setup_topics.py <command> [arguments]

Commands:
    create          Create all required Kafka topics
    verify          Verify topics exist with correct configuration
    list            List all topics in the cluster
    describe [topics]  Describe specific topics (or all if none specified)
    delete <topics>    Delete specified topics (requires confirmation)

Examples:
    python setup_topics.py create
    python setup_topics.py verify
    python setup_topics.py describe movie.reviews movie.ratings
    python setup_topics.py delete movie.reviews

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS   Kafka bootstrap servers (default: localhost:9092)
""")


if __name__ == '__main__':
    main()
