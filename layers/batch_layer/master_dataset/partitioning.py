"""
Partitioning utilities for the Batch Layer data pipeline.

This module provides utilities for managing HDFS partitions across
Bronze, Silver, and Gold layers with efficient data organization.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os
from pathlib import Path

# Setup logging
logger = logging.getLogger(__name__)


class PartitionManager:
    """Manages partition strategies and operations for different data layers."""
    
    def __init__(self, hdfs_config: Dict):
        """
        Initialize partition manager with HDFS configuration.
        
        Args:
            hdfs_config: Dictionary containing HDFS configuration
        """
        self.hdfs_config = hdfs_config
        self.paths = hdfs_config.get("paths", {})
        
    def generate_bronze_partition_path(
        self, 
        base_path: str, 
        timestamp: datetime
    ) -> str:
        """
        Generate partition path for Bronze layer (time-based).
        
        Format: /data/bronze/year=YYYY/month=MM/day=DD/hour=HH
        
        Args:
            base_path: Base HDFS path for Bronze layer
            timestamp: Extraction timestamp for partitioning
            
        Returns:
            str: Complete partition path
        """
        return os.path.join(
            base_path,
            f"year={timestamp.year}",
            f"month={timestamp.month:02d}",
            f"day={timestamp.day:02d}",
            f"hour={timestamp.hour:02d}"
        )
    
    def generate_silver_partition_path(
        self, 
        base_path: str, 
        timestamp: datetime,
        genre: str
    ) -> str:
        """
        Generate partition path for Silver layer (hybrid: time + genre).
        
        Format: /data/silver/year=YYYY/month=MM/genre=GENRE
        
        Args:
            base_path: Base HDFS path for Silver layer
            timestamp: Processing timestamp
            genre: Primary genre for partitioning
            
        Returns:
            str: Complete partition path
        """
        # Clean genre name for filesystem compatibility
        clean_genre = self._clean_partition_value(genre)
        
        return os.path.join(
            base_path,
            f"year={timestamp.year}",
            f"month={timestamp.month:02d}",
            f"genre={clean_genre}"
        )
    
    def generate_gold_partition_path(
        self, 
        base_path: str, 
        metric_type: str,
        timestamp: datetime
    ) -> str:
        """
        Generate partition path for Gold layer (metric-based).
        
        Format: /data/gold/metric_type=TYPE/year=YYYY/month=MM
        
        Args:
            base_path: Base HDFS path for Gold layer
            metric_type: Type of metric/aggregation
            timestamp: Processing timestamp
            
        Returns:
            str: Complete partition path
        """
        clean_metric = self._clean_partition_value(metric_type)
        
        return os.path.join(
            base_path,
            f"metric_type={clean_metric}",
            f"year={timestamp.year}",
            f"month={timestamp.month:02d}"
        )
    
    def generate_trending_partition_path(
        self, 
        base_path: str, 
        window: str,
        timestamp: datetime
    ) -> str:
        """
        Generate partition path for trending scores in Gold layer.
        
        Format: /data/gold/trending/window=7d/year=YYYY/month=MM
        
        Args:
            base_path: Base HDFS path for Gold layer
            window: Time window (7d, 30d, 90d)
            timestamp: Processing timestamp
            
        Returns:
            str: Complete partition path
        """
        return os.path.join(
            base_path,
            "trending",
            f"window={window}",
            f"year={timestamp.year}",
            f"month={timestamp.month:02d}"
        )
    
    def get_bronze_partitions_to_process(
        self, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[str]:
        """
        Get list of Bronze partition paths to process for given time range.
        
        Args:
            start_time: Start of processing window
            end_time: End of processing window
            
        Returns:
            List[str]: List of partition paths to process
        """
        partitions = []
        current_time = start_time
        base_path = self.paths.get("bronze", "/data/bronze")
        
        while current_time <= end_time:
            partition_path = self.generate_bronze_partition_path(base_path, current_time)
            partitions.append(partition_path)
            current_time += timedelta(hours=1)
        
        return partitions
    
    def get_partitions_for_cleanup(
        self, 
        layer: str, 
        retention_days: int
    ) -> List[str]:
        """
        Get partitions that are eligible for cleanup based on retention policy.
        
        Args:
            layer: Data layer (bronze, silver, gold)
            retention_days: Number of days to retain data
            
        Returns:
            List[str]: List of partition paths to clean up
        """
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        base_path = self.paths.get(layer, f"/data/{layer}")
        
        # For now, return theoretical paths - in production this would
        # scan HDFS to find actual partitions older than cutoff_date
        partitions_to_cleanup = []
        
        # Generate paths for partitions older than retention period
        current_date = cutoff_date
        start_date = cutoff_date - timedelta(days=30)  # Look back 30 days for cleanup
        
        while start_date <= current_date:
            if layer == "bronze":
                for hour in range(24):
                    timestamp = start_date.replace(hour=hour)
                    partition_path = self.generate_bronze_partition_path(base_path, timestamp)
                    partitions_to_cleanup.append(partition_path)
            elif layer in ["silver", "gold"]:
                partition_path = os.path.join(
                    base_path,
                    f"year={start_date.year}",
                    f"month={start_date.month:02d}"
                )
                partitions_to_cleanup.append(partition_path)
            
            start_date += timedelta(days=1)
        
        return partitions_to_cleanup
    
    def create_partition_filter_condition(
        self, 
        layer: str,
        start_time: datetime,
        end_time: Optional[datetime] = None
    ) -> str:
        """
        Create Spark SQL filter condition for partition pruning.
        
        Args:
            layer: Data layer (bronze, silver, gold)
            start_time: Start time for filtering
            end_time: End time for filtering (optional)
            
        Returns:
            str: SQL WHERE condition for partition filtering
        """
        if layer == "bronze":
            conditions = [
                f"partition_year >= {start_time.year}",
                f"partition_month >= {start_time.month}",
                f"partition_day >= {start_time.day}",
                f"partition_hour >= {start_time.hour}"
            ]
            
            if end_time:
                conditions.extend([
                    f"partition_year <= {end_time.year}",
                    f"partition_month <= {end_time.month}",
                    f"partition_day <= {end_time.day}",
                    f"partition_hour <= {end_time.hour}"
                ])
        
        elif layer in ["silver", "gold"]:
            conditions = [
                f"partition_year >= {start_time.year}",
                f"partition_month >= {start_time.month}"
            ]
            
            if end_time:
                conditions.extend([
                    f"partition_year <= {end_time.year}",
                    f"partition_month <= {end_time.month}"
                ])
        
        return " AND ".join(conditions)
    
    def optimize_partition_sizes(
        self, 
        spark_session,
        target_size_mb: int = 128
    ) -> Dict[str, str]:
        """
        Calculate optimal partition configuration based on data size.
        
        Args:
            spark_session: Active Spark session
            target_size_mb: Target partition size in MB
            
        Returns:
            Dict[str, str]: Optimization recommendations
        """
        recommendations = {}
        
        # Analyze current partition sizes (simplified)
        recommendations["coalesce_partitions"] = "true"
        recommendations["target_partition_size"] = f"{target_size_mb}MB"
        recommendations["max_records_per_file"] = str(target_size_mb * 1024 * 8)  # ~8 records per KB
        
        # Calculate shuffle partitions based on data volume
        estimated_data_gb = 10  # This would be calculated from actual data
        optimal_shuffle_partitions = max(200, estimated_data_gb * 20)
        recommendations["shuffle_partitions"] = str(optimal_shuffle_partitions)
        
        return recommendations
    
    def validate_partition_structure(self, partition_path: str, layer: str) -> bool:
        """
        Validate that partition path follows the expected structure for the layer.
        
        Args:
            partition_path: HDFS path to validate
            layer: Expected layer type
            
        Returns:
            bool: True if structure is valid
        """
        try:
            path_parts = partition_path.strip('/').split('/')
            
            if layer == "bronze":
                # Expected: .../year=YYYY/month=MM/day=DD/hour=HH
                expected_patterns = ["year=", "month=", "day=", "hour="]
                return self._validate_path_patterns(path_parts[-4:], expected_patterns)
            
            elif layer == "silver":
                # Expected: .../year=YYYY/month=MM/genre=GENRE
                expected_patterns = ["year=", "month=", "genre="]
                return self._validate_path_patterns(path_parts[-3:], expected_patterns)
            
            elif layer == "gold":
                # Expected: .../metric_type=TYPE/year=YYYY/month=MM
                expected_patterns = ["metric_type=", "year=", "month="]
                return self._validate_path_patterns(path_parts[-3:], expected_patterns)
            
            return False
            
        except Exception as e:
            logger.error(f"Error validating partition structure: {e}")
            return False
    
    def _clean_partition_value(self, value: str) -> str:
        """
        Clean partition value for filesystem compatibility.
        
        Args:
            value: Raw partition value
            
        Returns:
            str: Cleaned value safe for filesystem use
        """
        if not value:
            return "unknown"
        
        # Replace problematic characters
        cleaned = value.lower()
        cleaned = cleaned.replace(" ", "_")
        cleaned = cleaned.replace("-", "_")
        cleaned = cleaned.replace("&", "and")
        cleaned = "".join(c for c in cleaned if c.isalnum() or c == "_")
        
        # Ensure it's not empty and doesn't start with number
        if not cleaned or cleaned[0].isdigit():
            cleaned = "genre_" + cleaned
            
        return cleaned[:50]  # Limit length
    
    def _validate_path_patterns(self, path_parts: List[str], patterns: List[str]) -> bool:
        """
        Validate that path parts match expected patterns.
        
        Args:
            path_parts: List of path components to validate
            patterns: List of expected patterns (e.g., "year=", "month=")
            
        Returns:
            bool: True if all patterns match
        """
        if len(path_parts) != len(patterns):
            return False
        
        for part, pattern in zip(path_parts, patterns):
            if not part.startswith(pattern):
                return False
        
        return True


class RetentionManager:
    """Manages data retention policies across all layers."""
    
    def __init__(self, hdfs_config: Dict):
        """
        Initialize retention manager.
        
        Args:
            hdfs_config: HDFS configuration including retention policies
        """
        self.hdfs_config = hdfs_config
        self.retention_policies = hdfs_config.get("retention", {})
        
    def get_cleanup_candidates(self) -> Dict[str, List[str]]:
        """
        Get all partitions that should be cleaned up based on retention policies.
        
        Returns:
            Dict[str, List[str]]: Dictionary mapping layer names to cleanup paths
        """
        partition_manager = PartitionManager(self.hdfs_config)
        cleanup_candidates = {}
        
        for layer, retention_days in self.retention_policies.items():
            if layer.endswith("_days"):
                layer_name = layer.replace("_days", "")
                candidates = partition_manager.get_partitions_for_cleanup(
                    layer_name, 
                    retention_days
                )
                cleanup_candidates[layer_name] = candidates
        
        return cleanup_candidates
    
    def calculate_storage_savings(self, cleanup_candidates: Dict[str, List[str]]) -> Dict[str, Dict]:
        """
        Calculate estimated storage savings from cleanup operations.
        
        Args:
            cleanup_candidates: Dictionary of partitions to clean up
            
        Returns:
            Dict[str, Dict]: Storage savings estimates by layer
        """
        # This would integrate with HDFS APIs to get actual sizes
        # For now, returning estimated values
        savings = {}
        
        for layer, partitions in cleanup_candidates.items():
            estimated_size_gb = len(partitions) * 0.5  # Assume 500MB per partition
            savings[layer] = {
                "partitions_count": len(partitions),
                "estimated_size_gb": estimated_size_gb,
                "estimated_cost_savings": estimated_size_gb * 0.023  # $0.023 per GB-month
            }
        
        return savings