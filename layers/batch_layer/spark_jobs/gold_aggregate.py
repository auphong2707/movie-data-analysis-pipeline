"""
Gold Layer - Baseline Export

Reads baselines from Silver layer, adds metadata, and prepares for MongoDB export.
Simplified for baseline-only architecture.

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
    Gold Layer baseline export job.
    
    Simply reads baselines from Silver, adds final metadata,
    and writes to Gold for MongoDB export.
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = JobMetrics("gold_baseline_export")
    
    @log_execution(logger, "gold_baseline_export")
    def run(self):
        """
        Export baselines from Silver to Gold.
        
        Adds final metadata before MongoDB export.
        """
        logger.info("Starting Gold layer baseline export")
        
        # Load baselines from Silver
        baselines_df = self._load_silver_baselines()
        
        if baselines_df is None:
            logger.warning("No baselines found in Silver layer")
            return
        
        # Add final metadata
        final_baselines = self._add_metadata(baselines_df)
        
        # Write to Gold
        self._write_to_gold(final_baselines)
        
        # Log metrics
        self.metrics.log(logger)
        logger.info("Gold layer baseline export completed successfully")
    
    def _load_silver_baselines(self) -> Optional[DataFrame]:
        """Load baselines from Silver layer."""
        try:
            silver_path = get_silver_path("baselines", None).rstrip('/')
            logger.info(f"Loading baselines from Silver: {silver_path}")
            
            df = self.spark.read.parquet(silver_path)
            
            count = df.count()
            logger.info(f"Loaded {count} genre baselines from Silver layer")
            self.metrics.add_metric("silver_baselines_loaded", count)
            
            return df if count > 0 else None
            
        except Exception as e:
            logger.error(f"Failed to load Silver baselines: {str(e)}", exc_info=True)
            return None
    
    def _add_metadata(self, baselines_df: DataFrame) -> DataFrame:
        """
        Add final metadata to baselines before MongoDB export.
        
        Metadata already includes:
        - genre, avg_sentiment, sentiment_stddev, viral_threshold
        - avg_rating, avg_popularity, movie_count, review_count
        - type='baseline', source='tmdb_batch', updated_at
        
        No additional processing needed.
        """
        logger.info("Baselines ready for export (metadata already complete)")
        
        return baselines_df
    
    def _write_to_gold(self, baselines_df: DataFrame):
        """Write baselines to Gold layer."""
        try:
            output_path = get_gold_path("baselines", None).rstrip('/')
            
            logger.info(f"Writing baselines to Gold layer: {output_path}")
            
            baselines_df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            count = baselines_df.count()
            logger.info(f"Successfully wrote {count} baselines to Gold layer")
            self.metrics.add_metric("baselines_written", count)
            
        except Exception as e:
            logger.error(f"Failed to write baselines to Gold: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for Gold baseline export job."""
    parser = argparse.ArgumentParser(description="Gold Layer Baseline Export")
    
    args = parser.parse_args()
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("gold_baseline_export")
        
        # Run export
        job = GoldAggregationJob(spark)
        job.run()
        
    except Exception as e:
        logger.error(f"Gold baseline export failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
