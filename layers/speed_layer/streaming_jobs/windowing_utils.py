"""
Windowing Utilities for Spark Streaming Jobs
Common windowing and aggregation utilities shared across streaming processors.
"""

import logging
from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    window, col, avg, count, sum as spark_sum, min as spark_min, 
    max as spark_max, stddev, lag, lead, first, last, 
    coalesce, lit, when, expr, unix_timestamp, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

logger = logging.getLogger(__name__)


class WindowingUtils:
    """Utility functions for windowing operations in streaming jobs."""
    
    @staticmethod
    def create_time_windows(df: DataFrame, timestamp_col: str, 
                          window_duration: str, slide_duration: str = None,
                          watermark_delay: str = "2 minutes") -> DataFrame:
        """
        Create time windows with watermarking for late data handling.
        
        Args:
            df: Input DataFrame
            timestamp_col: Name of timestamp column
            window_duration: Window duration (e.g., "5 minutes")
            slide_duration: Sliding window duration (optional)
            watermark_delay: Watermark delay for late data
            
        Returns:
            DataFrame with window column added
        """
        # Add watermark
        watermarked_df = df.withWatermark(timestamp_col, watermark_delay)
        
        # Create windows
        if slide_duration:
            # Sliding window
            windowed_df = watermarked_df.groupBy(
                window(col(timestamp_col), window_duration, slide_duration)
            )
        else:
            # Tumbling window
            windowed_df = watermarked_df.groupBy(
                window(col(timestamp_col), window_duration)
            )
        
        return windowed_df
    
    @staticmethod
    def add_velocity_calculations(df: DataFrame, metric_col: str, 
                                partition_cols: List[str],
                                order_col: str = "window") -> DataFrame:
        """
        Add velocity (rate of change) calculations to windowed data.
        
        Args:
            df: Input DataFrame with windowed data
            metric_col: Column to calculate velocity for
            partition_cols: Columns to partition by (e.g., movie_id)
            order_col: Column to order by for lag calculations
            
        Returns:
            DataFrame with velocity columns added
        """
        # Create window specification
        window_spec = Window.partitionBy(*partition_cols).orderBy(order_col)
        
        # Add velocity calculations
        velocity_df = df \
            .withColumn(f"prev_{metric_col}", 
                       lag(metric_col, 1).over(window_spec)) \
            .withColumn(f"{metric_col}_velocity",
                       when(col(f"prev_{metric_col}").isNotNull(),
                            (col(metric_col) - col(f"prev_{metric_col}")) / 
                            (col(f"prev_{metric_col}") + 1))  # Avoid division by zero
                       .otherwise(lit(0.0))) \
            .withColumn(f"prev_{metric_col}_velocity",
                       lag(f"{metric_col}_velocity", 1).over(window_spec)) \
            .withColumn(f"{metric_col}_acceleration",
                       when(col(f"prev_{metric_col}_velocity").isNotNull(),
                            col(f"{metric_col}_velocity") - col(f"prev_{metric_col}_velocity"))
                       .otherwise(lit(0.0)))
        
        return velocity_df
    
    @staticmethod
    def calculate_moving_averages(df: DataFrame, metric_col: str,
                                partition_cols: List[str],
                                window_sizes: List[int] = [3, 5, 10],
                                order_col: str = "window") -> DataFrame:
        """
        Calculate moving averages for specified window sizes.
        
        Args:
            df: Input DataFrame
            metric_col: Column to calculate moving average for
            partition_cols: Columns to partition by
            window_sizes: List of window sizes for moving averages
            order_col: Column to order by
            
        Returns:
            DataFrame with moving average columns added
        """
        result_df = df
        
        for size in window_sizes:
            # Create window specification for moving average
            window_spec = Window.partitionBy(*partition_cols) \
                              .orderBy(order_col) \
                              .rowsBetween(-(size-1), 0)
            
            result_df = result_df.withColumn(
                f"{metric_col}_ma_{size}",
                avg(metric_col).over(window_spec)
            )
        
        return result_df
    
    @staticmethod
    def add_percentile_ranks(df: DataFrame, metric_col: str,
                           partition_cols: List[str] = None,
                           order_col: str = None) -> DataFrame:
        """
        Add percentile ranks for a metric within partitions.
        
        Args:
            df: Input DataFrame
            metric_col: Column to rank
            partition_cols: Columns to partition by (optional)
            order_col: Column to order by (defaults to metric_col)
            
        Returns:
            DataFrame with percentile rank column added
        """
        from pyspark.sql.functions import percent_rank
        
        order_col = order_col or metric_col
        
        if partition_cols:
            window_spec = Window.partitionBy(*partition_cols).orderBy(order_col)
        else:
            window_spec = Window.orderBy(order_col)
        
        return df.withColumn(
            f"{metric_col}_percentile_rank",
            percent_rank().over(window_spec)
        )
    
    @staticmethod
    def detect_anomalies_zscore(df: DataFrame, metric_col: str,
                              partition_cols: List[str],
                              threshold: float = 2.0,
                              window_size: int = 10) -> DataFrame:
        """
        Detect anomalies using Z-score method within a rolling window.
        
        Args:
            df: Input DataFrame
            metric_col: Column to analyze for anomalies
            partition_cols: Columns to partition by
            threshold: Z-score threshold for anomaly detection
            window_size: Size of rolling window for statistics
            
        Returns:
            DataFrame with anomaly detection columns added
        """
        # Create rolling window for statistics
        window_spec = Window.partitionBy(*partition_cols) \
                          .orderBy("window") \
                          .rowsBetween(-(window_size-1), 0)
        
        # Calculate rolling statistics
        stats_df = df \
            .withColumn(f"{metric_col}_rolling_mean",
                       avg(metric_col).over(window_spec)) \
            .withColumn(f"{metric_col}_rolling_std",
                       stddev(metric_col).over(window_spec))
        
        # Calculate Z-score and detect anomalies
        anomaly_df = stats_df \
            .withColumn(f"{metric_col}_zscore",
                       when(col(f"{metric_col}_rolling_std") > 0,
                            (col(metric_col) - col(f"{metric_col}_rolling_mean")) / 
                            col(f"{metric_col}_rolling_std"))
                       .otherwise(lit(0.0))) \
            .withColumn(f"{metric_col}_is_anomaly",
                       (abs(col(f"{metric_col}_zscore")) > threshold))
        
        return anomaly_df
    
    @staticmethod
    def create_session_windows(df: DataFrame, timestamp_col: str,
                             session_gap: str = "30 minutes",
                             partition_cols: List[str] = None) -> DataFrame:
        """
        Create session windows based on inactivity gaps.
        
        Args:
            df: Input DataFrame
            timestamp_col: Name of timestamp column
            session_gap: Gap duration to define session boundaries
            partition_cols: Columns to partition sessions by
            
        Returns:
            DataFrame with session information added
        """
        from pyspark.sql.functions import session_window
        
        if partition_cols:
            return df.groupBy(
                session_window(col(timestamp_col), session_gap),
                *partition_cols
            )
        else:
            return df.groupBy(
                session_window(col(timestamp_col), session_gap)
            )
    
    @staticmethod
    def add_time_based_features(df: DataFrame, timestamp_col: str) -> DataFrame:
        """
        Add time-based features like hour of day, day of week, etc.
        
        Args:
            df: Input DataFrame
            timestamp_col: Name of timestamp column
            
        Returns:
            DataFrame with time-based features added
        """
        from pyspark.sql.functions import hour, dayofweek, dayofyear, weekofyear
        
        return df \
            .withColumn("hour_of_day", hour(col(timestamp_col))) \
            .withColumn("day_of_week", dayofweek(col(timestamp_col))) \
            .withColumn("day_of_year", dayofyear(col(timestamp_col))) \
            .withColumn("week_of_year", weekofyear(col(timestamp_col)))


class AggregationUtils:
    """Utility functions for common aggregation patterns."""
    
    @staticmethod
    def basic_numeric_aggregations(df: DataFrame, 
                                 numeric_cols: List[str]) -> Dict[str, Any]:
        """
        Generate basic numeric aggregations for specified columns.
        
        Args:
            df: Input DataFrame
            numeric_cols: List of numeric columns to aggregate
            
        Returns:
            Dictionary of aggregation expressions
        """
        agg_exprs = {}
        
        for col_name in numeric_cols:
            agg_exprs.update({
                f"{col_name}_count": count(col_name),
                f"{col_name}_avg": avg(col_name),
                f"{col_name}_min": spark_min(col_name),
                f"{col_name}_max": spark_max(col_name),
                f"{col_name}_sum": spark_sum(col_name),
                f"{col_name}_stddev": stddev(col_name)
            })
        
        return agg_exprs
    
    @staticmethod
    def distribution_aggregations(df: DataFrame, col_name: str,
                                bins: List[Tuple[float, float]]) -> DataFrame:
        """
        Create distribution aggregations by binning values.
        
        Args:
            df: Input DataFrame
            col_name: Column to create distribution for
            bins: List of (min, max) tuples defining bins
            
        Returns:
            DataFrame with distribution counts
        """
        result_df = df
        
        for i, (min_val, max_val) in enumerate(bins):
            bin_name = f"{col_name}_bin_{i}"
            result_df = result_df.withColumn(
                bin_name,
                when((col(col_name) >= min_val) & (col(col_name) < max_val), 1)
                .otherwise(0)
            )
        
        return result_df
    
    @staticmethod
    def top_k_aggregation(df: DataFrame, group_cols: List[str],
                         metric_col: str, k: int = 10) -> DataFrame:
        """
        Get top K records by metric within groups.
        
        Args:
            df: Input DataFrame
            group_cols: Columns to group by
            metric_col: Column to rank by
            k: Number of top records to return
            
        Returns:
            DataFrame with top K records per group
        """
        from pyspark.sql.functions import rank, desc
        
        window_spec = Window.partitionBy(*group_cols).orderBy(desc(metric_col))
        
        return df \
            .withColumn("rank", rank().over(window_spec)) \
            .filter(col("rank") <= k)


class StreamingMetrics:
    """Utilities for calculating streaming-specific metrics."""
    
    @staticmethod
    def calculate_throughput_metrics(df: DataFrame, 
                                   window_col: str = "window") -> DataFrame:
        """
        Calculate throughput metrics for streaming data.
        
        Args:
            df: Input DataFrame with windowed data
            window_col: Name of window column
            
        Returns:
            DataFrame with throughput metrics
        """
        return df \
            .withColumn("window_duration_seconds",
                       (col(f"{window_col}.end").cast("long") - 
                        col(f"{window_col}.start").cast("long"))) \
            .withColumn("records_per_second",
                       col("record_count") / col("window_duration_seconds"))
    
    @staticmethod
    def calculate_latency_metrics(df: DataFrame,
                                event_time_col: str,
                                processing_time_col: str) -> DataFrame:
        """
        Calculate end-to-end latency metrics.
        
        Args:
            df: Input DataFrame
            event_time_col: Column with event timestamp
            processing_time_col: Column with processing timestamp
            
        Returns:
            DataFrame with latency metrics
        """
        return df \
            .withColumn("latency_seconds",
                       (col(processing_time_col).cast("long") - 
                        col(event_time_col).cast("long"))) \
            .withColumn("latency_minutes",
                       col("latency_seconds") / 60)


# Convenience functions for common patterns
def create_standard_movie_window(df: DataFrame, timestamp_col: str = "event_time",
                               window_duration: str = "5 minutes") -> DataFrame:
    """Create standard 5-minute tumbling window for movie data."""
    return WindowingUtils.create_time_windows(
        df, timestamp_col, window_duration, watermark_delay="2 minutes"
    )


def add_movie_velocity_metrics(df: DataFrame, metric_col: str) -> DataFrame:
    """Add velocity calculations for movie metrics."""
    return WindowingUtils.add_velocity_calculations(
        df, metric_col, ["movie_id"], "window"
    )


def detect_movie_anomalies(df: DataFrame, metric_col: str) -> DataFrame:
    """Detect anomalies in movie metrics using Z-score method."""
    return WindowingUtils.detect_anomalies_zscore(
        df, metric_col, ["movie_id"], threshold=2.5, window_size=10
    )