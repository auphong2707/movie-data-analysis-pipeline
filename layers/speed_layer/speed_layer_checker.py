#!/usr/bin/env python3
"""
Speed Layer Runtime Checker
===========================

This script performs comprehensive checks to ensure the Speed Layer can run smoothly.
It validates dependencies, configurations, connectivity, and data flow paths.

Usage:
    python speed_layer_checker.py [--verbose]

Author: Speed Layer Team
"""

import sys
import os
import importlib
import subprocess
import yaml
import logging
from typing import Dict, List, Tuple, Any
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SpeedLayerChecker:
    """Comprehensive checker for Speed Layer runtime requirements."""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.issues = []
        self.warnings = []
        self.base_path = Path(__file__).parent
        
        if verbose:
            logger.setLevel(logging.DEBUG)
            
    def check_all(self) -> bool:
        """Run all checks and return True if system is ready."""
        logger.info("🚀 Starting Speed Layer Runtime Checks...")
        
        checks = [
            ("Python Dependencies", self.check_dependencies),
            ("Configuration Files", self.check_configurations),
            ("Code Structure", self.check_code_structure),
            ("Import Statements", self.check_imports),
            ("Schema Definitions", self.check_schemas),
            ("Service Connectivity", self.check_connectivity),
            ("Performance Settings", self.check_performance),
        ]
        
        for check_name, check_func in checks:
            logger.info(f"🔍 Checking {check_name}...")
            try:
                check_func()
                logger.info(f"✅ {check_name} - PASSED")
            except Exception as e:
                logger.error(f"❌ {check_name} - FAILED: {e}")
                self.issues.append(f"{check_name}: {e}")
                
        self._print_summary()
        return len(self.issues) == 0
        
    def check_dependencies(self):
        """Check if all required Python packages are available."""
        required_packages = [
            # Kafka dependencies
            ('confluent_kafka', 'confluent-kafka>=2.11.1'),
            ('kafka', 'kafka-python>=2.2.15'),
            ('avro', 'avro-python3>=1.10.2'),
            
            # Spark dependencies  
            ('pyspark', 'pyspark>=4.0.1'),
            
            # Cassandra dependencies
            ('cassandra', 'cassandra-driver>=3.29.2'),
            
            # Analysis dependencies
            ('vaderSentiment', 'vaderSentiment>=3.3.2'),
            
            # Utility dependencies
            ('yaml', 'PyYAML>=6.0.2'),
            ('requests', 'requests>=2.32.5'),
        ]
        
        missing_packages = []
        for package_name, pip_name in required_packages:
            try:
                importlib.import_module(package_name)
                logger.debug(f"✅ {package_name} - Available")
            except ImportError:
                missing_packages.append(pip_name)
                logger.warning(f"❌ {package_name} - Missing")
                
        if missing_packages:
            install_cmd = f"pip install {' '.join(missing_packages)}"
            raise Exception(f"Missing packages: {missing_packages}. Install with: {install_cmd}")
            
    def check_configurations(self):
        """Check if all configuration files exist and are valid."""
        config_files = [
            'config/spark_streaming_config.yaml',
            'config/cassandra_config.yaml',
            'config/kafka_config.py',
        ]
        
        for config_file in config_files:
            config_path = self.base_path.parent.parent / config_file
            if not config_path.exists():
                raise Exception(f"Missing configuration file: {config_file}")
                
            if config_file.endswith('.yaml'):
                try:
                    with open(config_path, 'r') as f:
                        config = yaml.safe_load(f)
                        if not config:
                            raise Exception(f"Empty configuration file: {config_file}")
                        logger.debug(f"✅ {config_file} - Valid YAML")
                except yaml.YAMLError as e:
                    raise Exception(f"Invalid YAML in {config_file}: {e}")
                    
    def check_code_structure(self):
        """Check if all required code files exist."""
        required_files = [
            # Kafka Producers
            'kafka_producers/tmdb_stream_producer.py',
            'kafka_producers/event_producer.py', 
            'kafka_producers/schema_registry.py',
            
            # Streaming Jobs
            'streaming_jobs/review_sentiment_stream.py',
            'streaming_jobs/movie_aggregation_stream.py',
            'streaming_jobs/trending_detection_stream.py',
            'streaming_jobs/windowing_utils.py',
            
            # Cassandra Views
            'cassandra_views/schema.cql',
            'cassandra_views/speed_view_manager.py',
            'cassandra_views/ttl_manager.py',
        ]
        
        for file_path in required_files:
            full_path = self.base_path / file_path
            if not full_path.exists():
                raise Exception(f"Missing required file: {file_path}")
            logger.debug(f"✅ {file_path} - Exists")
            
    def check_imports(self):
        """Check if all Python files have correct imports."""
        python_files = [
            'kafka_producers/tmdb_stream_producer.py',
            'kafka_producers/event_producer.py',
            'kafka_producers/schema_registry.py',
            'streaming_jobs/review_sentiment_stream.py',
            'streaming_jobs/movie_aggregation_stream.py', 
            'streaming_jobs/trending_detection_stream.py',
            'streaming_jobs/windowing_utils.py',
            'cassandra_views/speed_view_manager.py',
            'cassandra_views/ttl_manager.py',
        ]
        
        import_issues = []
        for file_path in python_files:
            full_path = self.base_path / file_path
            try:
                # Read file and check for common import issues
                with open(full_path, 'r') as f:
                    content = f.read()
                    
                # Check for specific import patterns that might fail
                if 'from pyspark.sql.types import' in content:
                    if 'BooleanType' in content and 'BooleanType' not in content.split('from pyspark.sql.types import')[1].split(')')[0]:
                        import_issues.append(f"{file_path}: Missing BooleanType in imports")
                        
                logger.debug(f"✅ {file_path} - Import syntax OK")
            except Exception as e:
                import_issues.append(f"{file_path}: {e}")
                
        if import_issues:
            raise Exception(f"Import issues found: {import_issues}")
            
    def check_schemas(self):
        """Check if schema definitions are complete."""
        schema_file = self.base_path / 'cassandra_views/schema.cql'
        
        required_tables = [
            'review_sentiments',
            'movie_stats', 
            'trending_movies',
            'movie_ratings_by_window',
            'breakout_movies',
            'declining_movies'
        ]
        
        with open(schema_file, 'r') as f:
            schema_content = f.read()
            
        for table in required_tables:
            if f"CREATE TABLE IF NOT EXISTS {table}" not in schema_content:
                raise Exception(f"Missing table definition: {table}")
            logger.debug(f"✅ Table {table} - Defined")
            
    def check_connectivity(self):
        """Check if external services are accessible (optional - warns if not available)."""
        services = [
            ('Kafka', 'localhost', 9092),
            ('Schema Registry', 'localhost', 8081),
            ('Cassandra', 'localhost', 9042),
            ('MongoDB', 'localhost', 27017),
        ]
        
        for service_name, host, port in services:
            try:
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    logger.debug(f"✅ {service_name} ({host}:{port}) - Accessible")
                else:
                    self.warnings.append(f"{service_name} ({host}:{port}) - Not accessible")
                    logger.warning(f"⚠️  {service_name} ({host}:{port}) - Not accessible")
            except Exception as e:
                self.warnings.append(f"{service_name} connectivity check failed: {e}")
                
    def check_performance(self):
        """Check performance and resource settings."""
        spark_config_path = self.base_path.parent.parent / 'config/spark_streaming_config.yaml'
        
        try:
            with open(spark_config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            # Check critical settings
            spark_config = config.get('spark', {})
            
            # Check executor memory
            executor_memory = spark_config.get('executor', {}).get('memory', '512m')
            if not executor_memory.endswith('g') or int(executor_memory[:-1]) < 1:
                self.warnings.append("Spark executor memory < 1GB - may cause performance issues")
                
            # Check streaming batch duration
            streaming_config = spark_config.get('streaming', {})
            batch_duration = streaming_config.get('batch_duration', '10 minutes')
            if 'minute' in batch_duration and int(batch_duration.split()[0]) > 5:
                self.warnings.append("Streaming batch duration > 5 minutes - may increase latency")
                
            logger.debug("✅ Performance settings - Reviewed")
            
        except Exception as e:
            self.warnings.append(f"Could not check performance settings: {e}")
            
    def _print_summary(self):
        """Print a summary of all checks."""
        print("\n" + "="*60)
        print("🏁 SPEED LAYER RUNTIME CHECK SUMMARY")
        print("="*60)
        
        if self.issues:
            print(f"\n❌ CRITICAL ISSUES ({len(self.issues)}):")
            for i, issue in enumerate(self.issues, 1):
                print(f"  {i}. {issue}")
                
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for i, warning in enumerate(self.warnings, 1):
                print(f"  {i}. {warning}")
                
        if not self.issues:
            print("\n✅ ALL CRITICAL CHECKS PASSED!")
            print("The Speed Layer is ready to run.")
        else:
            print(f"\n❌ {len(self.issues)} CRITICAL ISSUES FOUND")
            print("Please fix these issues before running the Speed Layer.")
            
        if self.warnings:
            print(f"\n💡 {len(self.warnings)} warnings found - these won't prevent startup but may affect performance.")
            
        print("\n" + "="*60)

def main():
    """Main entry point."""
    verbose = '--verbose' in sys.argv or '-v' in sys.argv
    
    checker = SpeedLayerChecker(verbose=verbose)
    success = checker.check_all()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()