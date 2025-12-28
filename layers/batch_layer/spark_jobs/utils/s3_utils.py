"""
S3/MinIO utility functions for Batch Layer.

Provides helper functions for working with S3A paths and MinIO storage.
"""

import os
from typing import List, Optional
from datetime import datetime
from urllib.parse import urlparse


class S3PathBuilder:
    """
    Builder for S3A paths with partitioning support.
    
    Example:
        >>> builder = S3PathBuilder("bronze-data", "movies")
        >>> path = builder.with_date_partition(datetime.now()).build()
        >>> # Returns: s3a://bronze-data/movies/year=2025/month=11/day=10/hour=14/
    """
    
    def __init__(self, bucket: str, prefix: str = ""):
        self.bucket = bucket
        self.prefix = prefix
        self.partitions = []
    
    def with_partition(self, key: str, value: any) -> 'S3PathBuilder':
        """Add a partition key-value pair."""
        self.partitions.append(f"{key}={value}")
        return self
    
    def with_date_partition(
        self,
        dt: datetime,
        include_hour: bool = True
    ) -> 'S3PathBuilder':
        """Add date-based partitions (year/month/day/hour)."""
        self.partitions.append(f"year={dt.year}")
        self.partitions.append(f"month={dt.month:02d}")
        self.partitions.append(f"day={dt.day:02d}")
        if include_hour:
            self.partitions.append(f"hour={dt.hour:02d}")
        return self
    
    def build(self) -> str:
        """Build the complete S3A path."""
        parts = [f"s3a://{self.bucket}"]
        if self.prefix:
            parts.append(self.prefix)
        if self.partitions:
            parts.extend(self.partitions)
        return "/".join(parts) + "/"


def get_s3_path(
    layer: str,
    data_type: str,
    partition_date: Optional[datetime] = None,
    include_hour: bool = True
) -> str:
    """
    Get S3A path for a specific layer and data type.
    
    Args:
        layer: Data layer (bronze, silver, gold)
        data_type: Type of data (movies, reviews, people, etc.)
        partition_date: Date for partitioning (default: now)
        include_hour: Include hour in partition
    
    Returns:
        Complete S3A path
    
    Example:
        >>> path = get_s3_path("bronze", "movies", datetime.now())
        >>> # Returns: s3a://bronze-data/movies/year=2025/month=11/day=10/hour=14/
    """
    # Get bucket from environment
    bucket_map = {
        "bronze": os.getenv('MINIO_BUCKET_BRONZE', 'bronze-data'),
        "silver": os.getenv('MINIO_BUCKET_SILVER', 'silver-data'),
        "gold": os.getenv('MINIO_BUCKET_GOLD', 'gold-data'),
    }
    
    bucket = bucket_map.get(layer, f"{layer}-data")
    
    builder = S3PathBuilder(bucket, data_type)
    
    if partition_date:
        builder.with_date_partition(partition_date, include_hour)
    
    return builder.build()


def get_bronze_path(data_type: str, partition_date: Optional[datetime] = None) -> str:
    """Get Bronze layer S3A path."""
    return get_s3_path("bronze", data_type, partition_date, include_hour=True)


def get_silver_path(
    data_type: str,
    partition_date: Optional[datetime] = None,
    genre: Optional[str] = None
) -> str:
    """Get Silver layer S3A path with optional genre partition."""
    path = get_s3_path("silver", data_type, partition_date, include_hour=False)
    
    if genre:
        # Remove trailing slash and add genre partition
        path = path.rstrip('/') + f"/genre={genre}/"
    
    return path


def get_gold_path(
    metric_type: str,
    partition_date: Optional[datetime] = None
) -> str:
    """Get Gold layer S3A path."""
    return get_s3_path("gold", metric_type, partition_date, include_hour=False)


def parse_s3_path(s3_path: str) -> dict:
    """
    Parse S3A path into components.
    
    Args:
        s3_path: S3A path (e.g., s3a://bucket/prefix/year=2025/month=11/)
    
    Returns:
        Dictionary with bucket, prefix, and partitions
    
    Example:
        >>> result = parse_s3_path("s3a://bronze-data/movies/year=2025/month=11/")
        >>> print(result)
        >>> # {'bucket': 'bronze-data', 'prefix': 'movies', 'partitions': {'year': '2025', 'month': '11'}}
    """
    parsed = urlparse(s3_path)
    
    if parsed.scheme != 's3a':
        raise ValueError(f"Invalid S3A path: {s3_path}")
    
    bucket = parsed.netloc
    path_parts = [p for p in parsed.path.split('/') if p]
    
    # Separate prefix from partitions
    prefix_parts = []
    partitions = {}
    
    for part in path_parts:
        if '=' in part:
            key, value = part.split('=', 1)
            partitions[key] = value
        else:
            prefix_parts.append(part)
    
    return {
        'bucket': bucket,
        'prefix': '/'.join(prefix_parts) if prefix_parts else '',
        'partitions': partitions
    }


def list_partitions(base_path: str, partition_keys: List[str]) -> List[str]:
    """
    Generate list of partition paths (placeholder - requires Spark context).
    
    Note: This is a helper for generating partition filter patterns.
    Actual partition listing should be done via Spark DataFrame operations.
    
    Args:
        base_path: Base S3A path
        partition_keys: List of partition keys to filter
    
    Returns:
        List of partition filter strings
    """
    # This would typically be implemented with Spark's catalog
    # or boto3 for direct S3 access
    # For now, return base path
    return [base_path]


def get_latest_partition(
    spark,
    base_path: str,
    partition_keys: List[str] = None
) -> Optional[str]:
    """
    Get the latest partition from a dataset.
    
    Args:
        spark: SparkSession instance
        base_path: Base S3A path
        partition_keys: Partition keys to consider (default: ["year", "month", "day", "hour"])
    
    Returns:
        Path to latest partition or None
    """
    if partition_keys is None:
        partition_keys = ["year", "month", "day", "hour"]
    
    try:
        # Read partition metadata
        df = spark.read.parquet(base_path)
        
        # Get max values for each partition key
        max_partition = df.select(partition_keys).orderBy(
            *[spark.sql.functions.col(k).desc() for k in partition_keys]
        ).first()
        
        if max_partition:
            builder = S3PathBuilder(parse_s3_path(base_path)['bucket'], 
                                    parse_s3_path(base_path)['prefix'])
            for i, key in enumerate(partition_keys):
                builder.with_partition(key, max_partition[i])
            return builder.build()
        
    except Exception:
        pass
    
    return None


def ensure_trailing_slash(path: str) -> str:
    """Ensure S3 path ends with trailing slash."""
    return path if path.endswith('/') else path + '/'


def remove_trailing_slash(path: str) -> str:
    """Remove trailing slash from S3 path."""
    return path.rstrip('/')
