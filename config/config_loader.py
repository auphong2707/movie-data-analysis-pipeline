"""
Configuration Loader for Speed Layer
Handles environment variable substitution in YAML configs
"""

import os
import re
import yaml
from typing import Dict, Any


def substitute_env_vars(config: Any) -> Any:
    """
    Recursively substitute environment variables in config values.
    Format: ${VAR_NAME:default_value} or ${VAR_NAME}
    """
    if isinstance(config, dict):
        return {key: substitute_env_vars(value) for key, value in config.items()}
    elif isinstance(config, list):
        return [substitute_env_vars(item) for item in config]
    elif isinstance(config, str):
        # Pattern: ${VAR_NAME:default} or ${VAR_NAME}
        pattern = r'\$\{([^:}]+)(?::([^}]+))?\}'
        
        def replace_var(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) else ""
            return os.environ.get(var_name, default_value)
        
        return re.sub(pattern, replace_var, config)
    else:
        return config


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load YAML configuration with environment variable substitution.
    
    Args:
        config_path: Path to YAML config file
        
    Returns:
        Configuration dictionary with env vars substituted
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return substitute_env_vars(config)


def get_kafka_config() -> Dict[str, str]:
    """Get Kafka configuration from environment or defaults."""
    return {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'schema.registry.url': os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
    }


def get_cassandra_config() -> Dict[str, str]:
    """Get Cassandra configuration from environment or defaults."""
    return {
        'hosts': os.environ.get('CASSANDRA_HOSTS', 'localhost').split(','),
        'port': int(os.environ.get('CASSANDRA_PORT', '9042')),
        'keyspace': os.environ.get('CASSANDRA_KEYSPACE', 'speed_layer'),
    }


def get_spark_config() -> Dict[str, str]:
    """Get Spark configuration from environment."""
    return {
        'spark.cassandra.connection.host': os.environ.get('CASSANDRA_HOSTS', 'localhost'),
        'spark.cassandra.connection.port': os.environ.get('CASSANDRA_PORT', '9042'),
    }
