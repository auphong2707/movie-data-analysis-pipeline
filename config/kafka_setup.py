"""
Kafka setup and initialization script.
"""
import logging
import sys
import time
from typing import Dict, Any

from config.config import config
from config.kafka_config import KafkaTopicManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_kafka(topic_manager: KafkaTopicManager, max_retries: int = 30, delay: int = 2) -> bool:
    """Wait for Kafka to be available."""
    logger.info("Waiting for Kafka to be available...")
    
    for attempt in range(max_retries):
        try:
            # Try to list topics to check if Kafka is available
            topics = topic_manager.list_topics()
            logger.info("Kafka is available")
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Kafka not ready - {e}")
            time.sleep(delay)
    
    logger.error("Kafka is not available after maximum retries")
    return False

def setup_kafka_topics() -> bool:
    """Setup all Kafka topics for the movie data pipeline."""
    logger.info("Setting up Kafka topics...")
    
    try:
        # Initialize topic manager
        topic_manager = KafkaTopicManager(config.kafka_bootstrap_servers)
        
        # Wait for Kafka to be available
        if not wait_for_kafka(topic_manager):
            return False
        
        # Setup all pipeline topics
        success = topic_manager.setup_pipeline_topics()
        
        if success:
            logger.info("All Kafka topics setup successfully")
            
            # Verify topics
            topics = topic_manager.list_topics()
            pipeline_topics = list(config.kafka_topics.values())
            
            logger.info("Topic verification:")
            for topic_name in pipeline_topics:
                if topic_name in topics:
                    topic_info = topic_manager.check_topic_health(topic_name)
                    logger.info(f"  ✓ {topic_name}: {topic_info['partitions']} partitions")
                else:
                    logger.error(f"  ✗ {topic_name}: not found")
                    success = False
        
        return success
    
    except Exception as e:
        logger.error(f"Error setting up Kafka topics: {e}")
        return False

def cleanup_kafka_topics() -> bool:
    """Clean up all Kafka topics (for testing/reset)."""
    logger.info("Cleaning up Kafka topics...")
    
    try:
        topic_manager = KafkaTopicManager(config.kafka_bootstrap_servers)
        
        # Wait for Kafka to be available
        if not wait_for_kafka(topic_manager):
            return False
        
        # Delete all pipeline topics
        pipeline_topics = list(config.kafka_topics.values())
        results = topic_manager.delete_topics(pipeline_topics)
        
        success = all(results.values())
        
        if success:
            logger.info("All Kafka topics deleted successfully")
        else:
            failed_topics = [topic for topic, success in results.items() if not success]
            logger.error(f"Failed to delete topics: {failed_topics}")
        
        return success
    
    except Exception as e:
        logger.error(f"Error cleaning up Kafka topics: {e}")
        return False

def check_kafka_health() -> Dict[str, Any]:
    """Check health of Kafka cluster and topics."""
    logger.info("Checking Kafka health...")
    
    health_status = {
        'kafka_available': False,
        'topics': {},
        'overall_health': False
    }
    
    try:
        topic_manager = KafkaTopicManager(config.kafka_bootstrap_servers)
        
        # Check if Kafka is available
        topics = topic_manager.list_topics()
        health_status['kafka_available'] = True
        
        # Check each pipeline topic
        pipeline_topics = list(config.kafka_topics.values())
        all_healthy = True
        
        for topic_name in pipeline_topics:
            topic_health = topic_manager.check_topic_health(topic_name)
            health_status['topics'][topic_name] = topic_health
            
            if not topic_health.get('exists', False):
                all_healthy = False
        
        health_status['overall_health'] = all_healthy
        
    except Exception as e:
        logger.error(f"Error checking Kafka health: {e}")
        health_status['error'] = str(e)
    
    return health_status

def main():
    """Main entry point for Kafka setup."""
    if len(sys.argv) < 2:
        print("Usage: python kafka_setup.py [setup|cleanup|health]")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == 'setup':
        success = setup_kafka_topics()
        sys.exit(0 if success else 1)
    
    elif command == 'cleanup':
        success = cleanup_kafka_topics()
        sys.exit(0 if success else 1)
    
    elif command == 'health':
        health = check_kafka_health()
        print(f"Kafka Health Status: {health}")
        sys.exit(0 if health['overall_health'] else 1)
    
    else:
        print(f"Unknown command: {command}")
        print("Available commands: setup, cleanup, health")
        sys.exit(1)

if __name__ == "__main__":
    main()