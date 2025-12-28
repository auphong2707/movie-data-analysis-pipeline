"""
Gold Layer - Separate Views Export for 3 Business Goals

Reads three datasets from Silver layer and exports them as SEPARATE collections:
1. sentiment_baselines: Genre/franchise/yearly sentiment patterns (Business Goal #1: PR Crisis Detection)
2. viral_thresholds: Genre/budget-tier/seasonal viral cutoffs (Business Goal #2: Viral Content Detection)
3. movie_intelligence: Individual movie competitive data (Business Goal #3: Content Recommendation)

Each dataset is kept SEPARATE to avoid schema pollution and maintain clear business logic separation.

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
    Gold Layer separate views export job.
    
    Reads three datasets from Silver and exports them as SEPARATE Gold datasets:
    1. sentiment_baselines: For PR Crisis Detection (Business Goal #1)
    2. viral_thresholds: For Viral Content Detection (Business Goal #2)
    3. movie_intelligence: For Content Recommendation (Business Goal #3)
    
    Each dataset is kept separate to avoid schema pollution with null fields.
    """
    
    DATASETS = [
        "sentiment_baselines",
        "viral_thresholds",
        "movie_intelligence"
    ]
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = JobMetrics("gold_separate_views")
        self.batch_timestamp = datetime.utcnow()
    
    @log_execution(logger, "gold_separate_views")
    def run(self):
        """
        Load all three datasets from Silver, add metadata,
        and write them as SEPARATE Gold datasets (no merging).
        """
        logger.info("Starting Gold layer separate views export")
        
        # Process each dataset separately
        for dataset_name in self.DATASETS:
            try:
                logger.info(f"Processing dataset: {dataset_name}")
                
                # Load from Silver
                df = self._load_from_silver(dataset_name)
                
                if df is None:
                    logger.warning(f"No data found in Silver for {dataset_name}")
                    continue
                
                # Add temporal metadata (but NOT view_type - we keep them separate)
                df = self._add_metadata(df, dataset_name)
                
                # Write to separate Gold path
                self._write_to_gold(df, dataset_name)
                
            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {str(e)}", exc_info=True)
                # Continue with next dataset
        
        # Log metrics
        self.metrics.log(logger)
        logger.info("Gold layer separate views export completed")
    
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
    
    def _add_metadata(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Add temporal metadata only (NO view_type discriminator - keep datasets separate).
        
        Metadata fields:
        - batch_run_timestamp: When this batch was calculated (ISO 8601 UTC)
        - aggregation_granularity: 'all_time' (current), or 'daily'/'weekly'/'monthly' (future)
        - data_period_start: Start of data period (for all_time: earliest movie date)
        - data_period_end: End of data period (for all_time: latest movie date)
        
        These fields enable future daily/weekly aggregations without schema changes.
        """
        # Handle genre field type if it's an array
        if "genre" in df.columns:
            from pyspark.sql.types import ArrayType
            genre_type = [f.dataType for f in df.schema.fields if f.name == "genre"][0]
            if isinstance(genre_type, ArrayType):
                logger.info(f"Converting genre from ARRAY<STRING> to STRING in {dataset_name}")
                df = df.withColumn("genre", F.concat_ws(", ", F.col("genre")))
        
        # Add batch processing metadata
        df = df.withColumn("batch_run_timestamp", F.lit(self.batch_timestamp.isoformat() + "Z"))
        df = df.withColumn("aggregation_granularity", F.lit("all_time"))
        
        # For all_time aggregations, use placeholder dates
        df = df.withColumn("data_period_start", F.lit("1900-01-01").cast(StringType()))
        df = df.withColumn("data_period_end", F.lit(datetime.utcnow().strftime("%Y-%m-%d")).cast(StringType()))
        
        logger.info(f"Added metadata to {dataset_name}")
        
        return df
    
    def _write_to_gold(self, df: DataFrame, dataset_name: str):
        """Write dataset to SEPARATE Gold layer path (no merging)."""
        try:
            output_path = get_gold_path(dataset_name, None).rstrip('/')
            
            logger.info(f"Writing {dataset_name} to Gold layer: {output_path}")
            
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            count = df.count()
            logger.info(f"Successfully wrote {count} records to Gold/{dataset_name}")
            self.metrics.add_metric(f"{dataset_name}_written", count)
            
        except Exception as e:
            logger.error(f"Failed to write Gold/{dataset_name}: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for Gold separate views export job."""
    parser = argparse.ArgumentParser(description="Gold Layer Separate Views Export (3 Collections)")
    
    args = parser.parse_args()
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("gold_separate_views")
        
        # Run export
        job = GoldAggregationJob(spark)
        job.run()
        
    except Exception as e:
        logger.error(f"Gold separate views export failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
