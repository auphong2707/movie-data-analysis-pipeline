"""
Gold Layer - Unified Batch Views Export

Reads three datasets from Silver layer and combines into unified batch_views:
1. sentiment_baseline: Genre/franchise/yearly sentiment patterns
2. viral_threshold: Genre/budget-tier/seasonal viral cutoffs
3. movie_intelligence: Individual movie competitive data

All three types are unioned into a single batch_views dataset with view_type discriminator.

Usage:
    spark-submit gold_aggregate.py
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
import pyspark.sql.functions as F

# Add utils to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import get_spark_session, stop_spark_session
from utils.logger import get_logger, log_execution, JobMetrics
from utils.s3_utils import get_silver_path, get_gold_path

logger = get_logger(__name__)


class GoldAggregationJob:
    """
    Gold Layer unified batch_views export job.
    
    Reads three datasets from Silver and combines into unified batch_views:
    1. sentiment_baseline (view_type discriminator)
    2. viral_threshold (view_type discriminator)
    3. movie_intelligence (view_type discriminator)
    
    All types are unioned into single batch_views dataset for MongoDB.
    """
    
    DATASETS = [
        "sentiment_baselines",
        "viral_thresholds",
        "movie_intelligence"
    ]
    
    # Map dataset names to view_type values
    VIEW_TYPE_MAP = {
        "sentiment_baselines": "sentiment_baseline",
        "viral_thresholds": "viral_threshold",
        "movie_intelligence": "movie_intelligence"
    }
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = JobMetrics("gold_unified_batch_views")
        self.batch_timestamp = datetime.utcnow()
    
    @log_execution(logger, "gold_unified_batch_views")
    def run(self):
        """
        Load all three datasets from Silver, add view_type discriminator,
        and union into single batch_views dataset.
        """
        logger.info("Starting Gold layer unified batch_views export")
        
        unified_dfs = []
        
        # Load and prepare each dataset
        for dataset_name in self.DATASETS:
            try:
                logger.info(f"Processing dataset: {dataset_name}")
                
                # Load from Silver
                df = self._load_from_silver(dataset_name)
                
                if df is None:
                    logger.warning(f"No data found in Silver for {dataset_name}")
                    continue
                
                # Add view_type discriminator and metadata
                df = self._add_view_type_and_metadata(df, dataset_name)
                
                unified_dfs.append(df)
                
            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {str(e)}", exc_info=True)
                # Continue with next dataset
        
        # Union all datasets
        if unified_dfs:
            unified_df = self._union_all(unified_dfs)
            
            # Write unified batch_views to Gold
            self._write_unified_to_gold(unified_df)
        else:
            logger.warning("No datasets to export to batch_views")
        
        # Log metrics
        self.metrics.log(logger)
        logger.info("Gold layer unified batch_views export completed")
    
    def _load_from_silver(self, dataset_name: str) -> Optional[DataFrame]:
        """Load dataset from Silver layer."""
        try:
            silver_path = get_silver_path(dataset_name, None).rstrip('/')
            logger.info(f"Loading {dataset_name} from Silver: {silver_path}")
            
            df = self.spark.read.parquet(silver_path)
            
            count = df.count()
            logger.info(f"Loaded {count} records from Silver/{dataset_name}")
            self.metrics.add_metric(f"{dataset_name}_loaded", count)
            
            return df if count > 0 else None
            
        except Exception as e:
            logger.error(f"Failed to load Silver/{dataset_name}: {str(e)}", exc_info=True)
            return None
    
    def _add_view_type_and_metadata(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Add view_type discriminator and temporal metadata to support README requirements.
        
        Metadata fields:
        - view_type: Discriminator ('sentiment_baseline', 'viral_threshold', 'movie_intelligence')
        - batch_run_timestamp: When this batch was calculated (ISO 8601 UTC)
        - aggregation_granularity: 'all_time' (current), or 'daily'/'weekly'/'monthly' (future)
        - data_period_start: Start of data period (for all_time: earliest movie date)
        - data_period_end: End of data period (for all_time: latest movie date)
        
        These fields enable future daily/weekly aggregations without schema changes.
        """
        view_type = self.VIEW_TYPE_MAP.get(dataset_name, dataset_name)
        
        # Handle genre field type inconsistency
        # movie_intelligence has genre as ARRAY<STRING>, others have STRING
        # Convert array to comma-separated string for union compatibility
        if "genre" in df.columns:
            from pyspark.sql.types import ArrayType
            genre_type = [f.dataType for f in df.schema.fields if f.name == "genre"][0]
            if isinstance(genre_type, ArrayType):
                logger.info(f"Converting genre from ARRAY<STRING> to STRING in {dataset_name}")
                df = df.withColumn("genre", F.concat_ws(", ", F.col("genre")))
        
        # Add view_type discriminator
        df = df.withColumn("view_type", F.lit(view_type))
        
        # Add batch processing metadata
        df = df.withColumn("batch_run_timestamp", F.lit(self.batch_timestamp.isoformat() + "Z"))
        df = df.withColumn("aggregation_granularity", F.lit("all_time"))
        
        # For all_time aggregations, use placeholder dates (can be refined later with actual data ranges)
        df = df.withColumn("data_period_start", F.lit("1900-01-01").cast(StringType()))
        df = df.withColumn("data_period_end", F.lit(datetime.utcnow().strftime("%Y-%m-%d")).cast(StringType()))
        
        logger.info(f"Added view_type='{view_type}' and metadata to {dataset_name}")
        
        return df
    
    def _union_all(self, dfs: list) -> DataFrame:
        """
        Union all dataframes into single unified batch_views dataset.
        
        Handles schema differences between the three view types by:
        1. Building a unified schema that merges all column types
        2. Adding missing columns with correct types (not just StringType)
        """
        logger.info(f"Unioning {len(dfs)} datasets into unified batch_views")
        
        if len(dfs) == 1:
            return dfs[0]
        
        # Build unified schema from all dataframes
        # Map column name -> data type (use first non-null occurrence)
        unified_schema = {}
        for df in dfs:
            for field in df.schema.fields:
                if field.name not in unified_schema:
                    unified_schema[field.name] = field.dataType
        
        logger.info(f"Unified schema has {len(unified_schema)} columns")
        
        # Normalize each dataframe to have all columns with correct types
        normalized_dfs = []
        for idx, df in enumerate(dfs):
            logger.info(f"Normalizing dataframe {idx+1}/{len(dfs)} with {len(df.columns)} columns")
            
            # Add missing columns with correct type
            for col_name, col_type in unified_schema.items():
                if col_name not in df.columns:
                    df = df.withColumn(col_name, F.lit(None).cast(col_type))
            
            # Select columns in consistent order
            df = df.select(sorted(unified_schema.keys()))
            normalized_dfs.append(df)
        
        # Union all
        unified_df = normalized_dfs[0]
        for df in normalized_dfs[1:]:
            unified_df = unified_df.union(df)
        
        total_count = unified_df.count()
        logger.info(f"Unified batch_views contains {total_count} total documents")
        self.metrics.add_metric("batch_views_total", total_count)
        
        return unified_df
    
    def _write_unified_to_gold(self, df: DataFrame):
        """Write unified batch_views to Gold layer."""
        try:
            output_path = get_gold_path("batch_views", None).rstrip('/')
            
            logger.info(f"Writing unified batch_views to Gold layer: {output_path}")
            
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            count = df.count()
            logger.info(f"Successfully wrote {count} records to Gold/batch_views")
            self.metrics.add_metric("batch_views_written", count)
            
        except Exception as e:
            logger.error(f"Failed to write Gold/batch_views: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for Gold unified batch_views export job."""
    parser = argparse.ArgumentParser(description="Gold Layer Unified Batch Views Export")
    
    args = parser.parse_args()
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("gold_unified_batch_views")
        
        # Run export
        job = GoldAggregationJob(spark)
        job.run()
        
    except Exception as e:
        logger.error(f"Gold unified batch_views export failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
