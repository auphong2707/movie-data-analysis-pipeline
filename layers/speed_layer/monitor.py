"""
Speed Layer Monitoring Script

Monitors health and performance of Reddit streaming pipeline:
- Kafka topic lag
- Cassandra data freshness
- Message throughput
- Error rates

Usage:
    python monitor.py --interval 60
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition
from cassandra.cluster import Cluster

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SpeedLayerMonitor:
    """Monitor speed layer health and metrics."""
    
    def __init__(self, kafka_bootstrap: str, cassandra_host: str):
        """Initialize monitoring connections."""
        self.kafka_bootstrap = kafka_bootstrap
        self.cassandra_host = cassandra_host
        
        # Kafka admin client
        self.kafka_admin = KafkaAdminClient(
            bootstrap_servers=kafka_bootstrap.split(',')
        )
        
        # Cassandra connection
        self.cassandra_cluster = Cluster([cassandra_host])
        self.cassandra_session = self.cassandra_cluster.connect('speed_layer')
        
        logger.info("Monitor initialized")
    
    def check_kafka_lag(self) -> Dict[str, int]:
        """
        Check consumer lag for Reddit topics.
        
        Returns:
            Dict mapping topic to lag count
        """
        lag_info = {}
        
        for topic in ['reddit.posts', 'reddit.comments']:
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=self.kafka_bootstrap.split(','),
                    group_id='monitor_group'
                )
                
                partitions = [TopicPartition(topic, p) for p in range(3)]
                consumer.assign(partitions)
                
                # Get end offsets
                end_offsets = consumer.end_offsets(partitions)
                
                # Get current offsets
                current_offsets = {}
                for partition in partitions:
                    current_offsets[partition] = consumer.position(partition)
                
                # Calculate total lag
                total_lag = sum(
                    end_offsets[p] - current_offsets.get(p, 0)
                    for p in partitions
                )
                
                lag_info[topic] = total_lag
                consumer.close()
                
            except Exception as e:
                logger.error(f"Error checking lag for {topic}: {e}")
                lag_info[topic] = -1
        
        return lag_info
    
    def check_cassandra_freshness(self) -> Dict[str, datetime]:
        """
        Check when Cassandra tables were last updated.
        
        Returns:
            Dict mapping table to last update time
        """
        freshness = {}
        
        tables = ['reddit_post_metrics', 'reddit_comment_metrics', 'speed_views']
        
        for table in tables:
            try:
                query = f"SELECT MAX(processed_at) as last_update FROM {table}"
                result = self.cassandra_session.execute(query)
                row = result.one()
                
                if row and row.last_update:
                    freshness[table] = row.last_update
                else:
                    freshness[table] = None
                    
            except Exception as e:
                logger.error(f"Error checking {table}: {e}")
                freshness[table] = None
        
        return freshness
    
    def check_data_counts(self) -> Dict[str, int]:
        """
        Get record counts from Cassandra tables.
        
        Returns:
            Dict mapping table to count
        """
        counts = {}
        
        tables = ['reddit_post_metrics', 'reddit_comment_metrics', 'speed_views']
        
        for table in tables:
            try:
                query = f"SELECT COUNT(*) as count FROM {table}"
                result = self.cassandra_session.execute(query)
                row = result.one()
                counts[table] = row.count if row else 0
                
            except Exception as e:
                logger.error(f"Error counting {table}: {e}")
                counts[table] = -1
        
        return counts
    
    def print_status(self):
        """Print current monitoring status."""
        print("\n" + "="*60)
        print(f"Speed Layer Health Check - {datetime.now()}")
        print("="*60)
        
        # Kafka lag
        print("\n📊 Kafka Topic Lag:")
        lag = self.check_kafka_lag()
        for topic, lag_count in lag.items():
            status = "✓" if lag_count >= 0 and lag_count < 1000 else "✗"
            print(f"  {status} {topic}: {lag_count} messages")
        
        # Cassandra freshness
        print("\n🕒 Data Freshness:")
        freshness = self.check_cassandra_freshness()
        now = datetime.now()
        for table, last_update in freshness.items():
            if last_update:
                age = now - last_update
                status = "✓" if age < timedelta(minutes=10) else "⚠"
                print(f"  {status} {table}: {age.seconds//60}m ago")
            else:
                print(f"  ✗ {table}: No data")
        
        # Data counts
        print("\n📈 Record Counts:")
        counts = self.check_data_counts()
        for table, count in counts.items():
            status = "✓" if count > 0 else "⚠"
            print(f"  {status} {table}: {count:,} records")
        
        print("\n" + "="*60)
    
    def run(self, interval: int = 60):
        """
        Run continuous monitoring.
        
        Args:
            interval: Check interval in seconds
        """
        try:
            while True:
                self.print_status()
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Monitoring stopped")
        finally:
            self.cassandra_cluster.shutdown()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Speed Layer Monitor')
    parser.add_argument('--interval', type=int, default=60,
                        help='Check interval in seconds')
    args = parser.parse_args()
    
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-1:29092')
    cassandra_host = os.getenv('CASSANDRA_HOST', 'cassandra')
    
    monitor = SpeedLayerMonitor(kafka_bootstrap, cassandra_host)
    monitor.run(interval=args.interval)
