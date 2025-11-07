"""
Common transformation utilities for batch layer.
"""
from typing import List, Optional
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, row_number, count, when, isnull, lit, 
    sum as spark_sum, expr, coalesce
)


def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_column: str = "extraction_timestamp",
    ascending: bool = False
) -> DataFrame:
    """
    Deduplicate DataFrame by key columns, keeping the latest/earliest record.
    
    Args:
        df: Input DataFrame
        key_columns: Columns to use as deduplication key
        order_column: Column to use for ordering (e.g., timestamp)
        ascending: Whether to sort ascending (False = keep latest)
        
    Returns:
        Deduplicated DataFrame
        
    Example:
        >>> deduped_df = deduplicate_by_key(df, ["movie_id"], "extraction_timestamp")
    """
    window_spec = Window.partitionBy(*key_columns).orderBy(
        col(order_column).asc() if ascending else col(order_column).desc()
    )
    
    df_with_rank = df.withColumn("_rank", row_number().over(window_spec))
    deduplicated_df = df_with_rank.filter(col("_rank") == 1).drop("_rank")
    
    return deduplicated_df


def validate_completeness(
    df: DataFrame,
    required_columns: List[str],
    threshold: float = 0.95
) -> tuple[DataFrame, dict]:
    """
    Validate data completeness and return stats.
    
    Args:
        df: Input DataFrame
        required_columns: Columns that should not be null
        threshold: Minimum acceptable completeness ratio (0-1)
        
    Returns:
        Tuple of (DataFrame, validation_stats)
        
    Example:
        >>> df, stats = validate_completeness(df, ["movie_id", "title"], 0.95)
        >>> print(stats["completeness_ratio"])
    """
    total_rows = df.count()
    
    if total_rows == 0:
        return df, {
            "total_rows": 0,
            "completeness_ratio": 1.0,
            "passes_threshold": True,
            "column_stats": {}
        }
    
    # Calculate null counts for required columns
    null_counts = {}
    for col_name in required_columns:
        if col_name in df.columns:
            null_count = df.filter(isnull(col(col_name))).count()
            null_counts[col_name] = {
                "null_count": null_count,
                "null_ratio": null_count / total_rows,
                "completeness": 1 - (null_count / total_rows)
            }
    
    # Calculate overall completeness
    avg_completeness = sum(
        stats["completeness"] for stats in null_counts.values()
    ) / len(required_columns)
    
    passes_threshold = avg_completeness >= threshold
    
    validation_stats = {
        "total_rows": total_rows,
        "completeness_ratio": avg_completeness,
        "passes_threshold": passes_threshold,
        "threshold": threshold,
        "column_stats": null_counts
    }
    
    return df, validation_stats


def add_quality_flags(
    df: DataFrame,
    required_columns: Optional[List[str]] = None,
    numeric_columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Add quality flags to DataFrame based on data completeness and validity.
    
    Args:
        df: Input DataFrame
        required_columns: Columns that should not be null
        numeric_columns: Numeric columns to check for validity
        
    Returns:
        DataFrame with 'quality_flag' column added
        
    Quality flags:
        - OK: All checks pass
        - WARNING: Some optional fields are null
        - ERROR: Required fields are null or invalid
        
    Example:
        >>> df_with_flags = add_quality_flags(df, ["movie_id"], ["vote_average"])
    """
    required_columns = required_columns or []
    numeric_columns = numeric_columns or []
    
    # Check for null required columns
    required_null_condition = None
    for col_name in required_columns:
        if col_name in df.columns:
            if required_null_condition is None:
                required_null_condition = isnull(col(col_name))
            else:
                required_null_condition = required_null_condition | isnull(col(col_name))
    
    # Check for invalid numeric values
    numeric_invalid_condition = None
    for col_name in numeric_columns:
        if col_name in df.columns:
            invalid = (isnull(col(col_name))) | (col(col_name) < 0)
            if numeric_invalid_condition is None:
                numeric_invalid_condition = invalid
            else:
                numeric_invalid_condition = numeric_invalid_condition | invalid
    
    # Add quality flag
    df_with_flag = df.withColumn(
        "quality_flag",
        when(
            required_null_condition if required_null_condition is not None else lit(False),
            "ERROR"
        ).when(
            numeric_invalid_condition if numeric_invalid_condition is not None else lit(False),
            "WARNING"
        ).otherwise("OK")
    )
    
    return df_with_flag


def calculate_window_aggregates(
    df: DataFrame,
    partition_cols: List[str],
    order_col: str,
    agg_col: str,
    windows: List[int]
) -> DataFrame:
    """
    Calculate rolling window aggregates.
    
    Args:
        df: Input DataFrame
        partition_cols: Columns to partition by
        order_col: Column to order by
        agg_col: Column to aggregate
        windows: List of window sizes (in rows)
        
    Returns:
        DataFrame with rolling aggregates
        
    Example:
        >>> df = calculate_window_aggregates(
        ...     df, ["movie_id"], "date", "popularity", [7, 30, 90]
        ... )
    """
    for window_size in windows:
        window_spec = Window.partitionBy(*partition_cols).orderBy(col(order_col)).rowsBetween(
            -window_size + 1, 0
        )
        
        df = df.withColumn(
            f"{agg_col}_{window_size}d_avg",
            expr(f"avg({agg_col})").over(window_spec)
        )
    
    return df


def explode_genres(df: DataFrame, genre_column: str = "genres") -> DataFrame:
    """
    Explode genres array into separate rows.
    
    Args:
        df: Input DataFrame with genres array
        genre_column: Name of the genre column
        
    Returns:
        DataFrame with one row per genre
        
    Example:
        >>> exploded_df = explode_genres(df, "genres")
    """
    from pyspark.sql.functions import explode_outer
    
    return df.withColumn("genre", explode_outer(col(genre_column)))


def clean_text_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Clean text column (trim, handle nulls).
    
    Args:
        df: Input DataFrame
        column_name: Column to clean
        
    Returns:
        DataFrame with cleaned column
    """
    from pyspark.sql.functions import trim, regexp_replace
    
    return df.withColumn(
        column_name,
        when(
            isnull(col(column_name)) | (trim(col(column_name)) == ""),
            None
        ).otherwise(
            regexp_replace(trim(col(column_name)), r'\s+', ' ')
        )
    )


def calculate_trend_metrics(
    df: DataFrame,
    metric_col: str,
    partition_cols: List[str],
    order_col: str
) -> DataFrame:
    """
    Calculate trend metrics (velocity, acceleration).
    
    Args:
        df: Input DataFrame
        metric_col: Column to calculate trends for
        partition_cols: Columns to partition by
        order_col: Column to order by (e.g., date)
        
    Returns:
        DataFrame with velocity and acceleration columns
        
    Example:
        >>> df = calculate_trend_metrics(df, "popularity", ["movie_id"], "date")
    """
    from pyspark.sql.functions import lag
    
    window_spec = Window.partitionBy(*partition_cols).orderBy(col(order_col))
    
    # Calculate velocity (first derivative)
    df = df.withColumn(
        f"{metric_col}_prev",
        lag(col(metric_col), 1).over(window_spec)
    )
    
    df = df.withColumn(
        f"{metric_col}_velocity",
        when(
            col(f"{metric_col}_prev").isNotNull(),
            col(metric_col) - col(f"{metric_col}_prev")
        ).otherwise(0)
    )
    
    # Calculate acceleration (second derivative)
    df = df.withColumn(
        f"{metric_col}_velocity_prev",
        lag(col(f"{metric_col}_velocity"), 1).over(window_spec)
    )
    
    df = df.withColumn(
        f"{metric_col}_acceleration",
        when(
            col(f"{metric_col}_velocity_prev").isNotNull(),
            col(f"{metric_col}_velocity") - col(f"{metric_col}_velocity_prev")
        ).otherwise(0)
    )
    
    # Drop temporary columns
    df = df.drop(f"{metric_col}_prev", f"{metric_col}_velocity_prev")
    
    return df
