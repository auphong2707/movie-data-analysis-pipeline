"""
HDFS utilities for batch layer.
"""
import os
import sys
from typing import List, Optional
from datetime import datetime
import subprocess

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))
from config.config import config


class HDFSManager:
    """
    Manager for HDFS operations in the batch layer.
    
    Provides utilities for:
    - Creating partitioned paths
    - Checking file existence
    - Listing files
    - Managing retention policies
    """
    
    def __init__(self, namenode: Optional[str] = None):
        """
        Initialize HDFS manager.
        
        Args:
            namenode: HDFS namenode URL (defaults to config)
        """
        self.namenode = namenode or config.hdfs_namenode
        self.bronze_path = config.hdfs_paths['bronze']
        self.silver_path = config.hdfs_paths['silver']
        self.gold_path = config.hdfs_paths['gold']
    
    def get_bronze_path(self, data_type: str, dt: datetime) -> str:
        """
        Get bronze layer partition path.
        
        Args:
            data_type: Type of data (movies, reviews, etc.)
            dt: Datetime for partitioning
            
        Returns:
            Full HDFS path with partitions
            
        Example:
            >>> manager = HDFSManager()
            >>> path = manager.get_bronze_path("movies", datetime.now())
            >>> # Returns: hdfs://.../bronze/movies/year=2025/month=01/day=15/hour=10
        """
        return (
            f"{self.namenode}{self.bronze_path}/{data_type}/"
            f"year={dt.year}/month={dt.month:02d}/"
            f"day={dt.day:02d}/hour={dt.hour:02d}/"
        )
    
    def get_silver_path(self, data_type: str, dt: datetime, genre: Optional[str] = None) -> str:
        """
        Get silver layer partition path.
        
        Args:
            data_type: Type of data (movies, reviews, etc.)
            dt: Datetime for partitioning
            genre: Optional genre for additional partitioning
            
        Returns:
            Full HDFS path with partitions
            
        Example:
            >>> path = manager.get_silver_path("movies", datetime.now(), "Action")
            >>> # Returns: hdfs://.../silver/movies/year=2025/month=01/genre=Action
        """
        base_path = (
            f"{self.namenode}{self.silver_path}/{data_type}/"
            f"year={dt.year}/month={dt.month:02d}/"
        )
        
        if genre:
            base_path += f"genre={genre}/"
        
        return base_path
    
    def get_gold_path(self, metric_type: str, dt: datetime) -> str:
        """
        Get gold layer partition path.
        
        Args:
            metric_type: Type of metric (genre_analytics, trending, etc.)
            dt: Datetime for partitioning
            
        Returns:
            Full HDFS path with partitions
            
        Example:
            >>> path = manager.get_gold_path("trending", datetime.now())
            >>> # Returns: hdfs://.../gold/trending/year=2025/month=01
        """
        return (
            f"{self.namenode}{self.gold_path}/{metric_type}/"
            f"year={dt.year}/month={dt.month:02d}/"
        )
    
    def path_exists(self, path: str) -> bool:
        """
        Check if HDFS path exists.
        
        Args:
            path: HDFS path to check
            
        Returns:
            True if path exists, False otherwise
        """
        try:
            result = subprocess.run(
                ["hdfs", "dfs", "-test", "-e", path],
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def list_files(self, path: str) -> List[str]:
        """
        List files in HDFS directory.
        
        Args:
            path: HDFS directory path
            
        Returns:
            List of file paths
        """
        try:
            result = subprocess.run(
                ["hdfs", "dfs", "-ls", path],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                return []
            
            files = []
            for line in result.stdout.split('\n'):
                if line.startswith('drwxr') or line.startswith('-rw'):
                    parts = line.split()
                    if len(parts) >= 8:
                        files.append(parts[7])
            
            return files
        except Exception:
            return []
    
    def create_directory(self, path: str) -> bool:
        """
        Create HDFS directory if it doesn't exist.
        
        Args:
            path: Directory path to create
            
        Returns:
            True if successful, False otherwise
        """
        try:
            subprocess.run(
                ["hdfs", "dfs", "-mkdir", "-p", path],
                capture_output=True,
                timeout=10
            )
            return True
        except Exception:
            return False
    
    def delete_path(self, path: str, recursive: bool = False) -> bool:
        """
        Delete HDFS path.
        
        Args:
            path: Path to delete
            recursive: Whether to delete recursively
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cmd = ["hdfs", "dfs", "-rm"]
            if recursive:
                cmd.append("-r")
            cmd.append(path)
            
            subprocess.run(cmd, capture_output=True, timeout=30)
            return True
        except Exception:
            return False
