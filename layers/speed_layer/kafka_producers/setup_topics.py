#!/usr/bin/env python3
"""
Kafka Topic Setup Script for Reddit Stream
Creates and configures Kafka topics for the speed layer
"""

import sys
import os
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:29092,kafka-2:29092,kafka-3:29092')
REPLICATION_FACTOR = 3
PARTITIONS = 3
RETENTION_MS = 172800000  # 48 hours

# Topic definitions
TOPICS = [
    {
        'name': 'reddit.posts',
        'partitions': PARTITIONS,
        'replication_factor': REPLICATION_FACTOR,
        'config': {
            'retention.ms': str(RETENTION_MS),
            'compression.type': 'gzip',
            'cleanup.policy': 'delete'
        }
    },
    {
        'name': 'reddit.comments',
        'partitions': PARTITIONS,
        'replication_factor': REPLICATION_FACTOR,
        'config': {
            'retention.ms': str(RETENTION_MS),
            'compression.type': 'gzip',
            'cleanup.policy': 'delete'
        }
    },
    {
        'name': 'reddit.sentiment',
        'partitions': PARTITIONS,
        'replication_factor': REPLICATION_FACTOR,
        'config': {
            'retention.ms': str(RETENTION_MS),
            'compression.type': 'gzip',
            'cleanup.policy': 'delete'
        }
    }
]


def wait_for_kafka(bootstrap_servers, max_retries=30, retry_delay=2):
    """Wait for Kafka to be available"""
    print(f"⏳ Waiting for Kafka at {bootstrap_servers}...")
    
    for attempt in range(max_retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers.split(','),
                request_timeout_ms=5000
            )
            admin_client.close()
            print("✅ Kafka is ready!")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"   Attempt {attempt + 1}/{max_retries}: Not ready yet, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"❌ Failed to connect to Kafka after {max_retries} attempts: {e}")
                return False
    
    return False


def create_topics(bootstrap_servers):
    """Create Kafka topics"""
    print(f"\n📋 Creating Kafka topics...")
    print(f"   Bootstrap servers: {bootstrap_servers}")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers.split(','),
            request_timeout_ms=10000
        )
        
        # Create NewTopic objects
        new_topics = []
        for topic_def in TOPICS:
            new_topic = NewTopic(
                name=topic_def['name'],
                num_partitions=topic_def['partitions'],
                replication_factor=topic_def['replication_factor'],
                topic_configs=topic_def['config']
            )
            new_topics.append(new_topic)
            print(f"   • {topic_def['name']} (partitions={topic_def['partitions']}, replication={topic_def['replication_factor']})")
        
        # Create topics
        try:
            result = admin_client.create_topics(new_topics=new_topics, validate_only=False)
            
            # Check results
            for topic_name, future in result.items():
                try:
                    future.result()  # Block until topic is created
                    print(f"   ✅ Topic '{topic_name}' created successfully")
                except TopicAlreadyExistsError:
                    print(f"   ℹ️  Topic '{topic_name}' already exists")
                except Exception as e:
                    print(f"   ❌ Failed to create topic '{topic_name}': {e}")
        
        except TopicAlreadyExistsError as e:
            # Handle case where all topics already exist
            print(f"   ℹ️  Topics already exist (this is OK)")
        
        admin_client.close()
        print("\n✅ Topic setup completed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error creating topics: {e}")
        return False


def list_topics(bootstrap_servers):
    """List existing Kafka topics"""
    print(f"\n📋 Listing Kafka topics...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers.split(','),
            request_timeout_ms=10000
        )
        
        topics = admin_client.list_topics()
        print(f"\n   Total topics: {len(topics)}")
        for topic in sorted(topics):
            if not topic.startswith('__'):  # Skip internal topics
                print(f"   • {topic}")
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Error listing topics: {e}")
        return False


def delete_topics(bootstrap_servers):
    """Delete all Reddit-related topics"""
    print(f"\n🗑️  Deleting Reddit topics...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers.split(','),
            request_timeout_ms=10000
        )
        
        topics_to_delete = [topic['name'] for topic in TOPICS]
        
        result = admin_client.delete_topics(topics=topics_to_delete, timeout_ms=10000)
        
        for topic_name, future in result.items():
            try:
                future.result()
                print(f"   ✅ Topic '{topic_name}' deleted successfully")
            except Exception as e:
                print(f"   ℹ️  Topic '{topic_name}': {e}")
        
        admin_client.close()
        print("\n✅ Topic deletion completed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error deleting topics: {e}")
        return False


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python setup_topics.py [create|list|delete]")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
    
    print("=" * 70)
    print("🚀 Kafka Topic Management - Reddit Stream")
    print("=" * 70)
    
    # Wait for Kafka to be ready
    if not wait_for_kafka(bootstrap_servers):
        print("\n❌ Kafka is not available. Exiting.")
        sys.exit(1)
    
    # Execute command
    if command == 'create':
        success = create_topics(bootstrap_servers)
    elif command == 'list':
        success = list_topics(bootstrap_servers)
    elif command == 'delete':
        success = delete_topics(bootstrap_servers)
    else:
        print(f"\n❌ Unknown command: {command}")
        print("   Valid commands: create, list, delete")
        sys.exit(1)
    
    print("=" * 70)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
