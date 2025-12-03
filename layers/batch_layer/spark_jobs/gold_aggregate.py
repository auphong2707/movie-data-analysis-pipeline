"""
Gold Layer - Multi-Goal Dataset Export

Reads three datasets from Silver layer and prepares for MongoDB export:
1. sentiment_baselines: Genre/franchise/director/temporal sentiment patterns
2. viral_thresholds: Genre/budget-tier/seasonal viral cutoffs
3. movie_intelligence: Individual movie competitive data

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
    Gold Layer multi-goal dataset export job.
    
    Reads three datasets from Silver and writes to Gold for MongoDB export:
    1. sentiment_baselines
    2. viral_thresholds
    3. movie_intelligence
    """
    
    DATASETS = [
        "sentiment_baselines",
        "viral_thresholds",
        "movie_intelligence"
    ]
    
    def __init__(self, spark):
        self.spark = spark
        self.metrics = JobMetrics("gold_multi_goal_export")
    
    @log_execution(logger, "gold_multi_goal_export")
    def run(self):
        """
        Export all three datasets from Silver to Gold.
        """
        logger.info("Starting Gold layer multi-goal export")
        
        # Process each dataset
        for dataset_name in self.DATASETS:
            try:
                logger.info(f"Processing dataset: {dataset_name}")
                
                # Load from Silver
                df = self._load_from_silver(dataset_name)
                
                if df is None:
                    logger.warning(f"No data found in Silver for {dataset_name}")
                    continue
                
                # Add final metadata
                df = self._add_metadata(df)
                
                # Write to Gold
                self._write_to_gold(df, dataset_name)
                
            except Exception as e:
                logger.error(f"Failed to process {dataset_name}: {str(e)}", exc_info=True)
                # Continue with next dataset
        
        # Log metrics
        self.metrics.log(logger)
        logger.info("Gold layer multi-goal export completed")
    
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
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add final metadata before MongoDB export.
        
        Metadata already complete from Silver layer:
        - type, updated_at fields already present
        
        No additional processing needed.
        """
        return df
    
    def _write_to_gold(self, df: DataFrame, dataset_name: str):
        """Write dataset to Gold layer."""
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
    """Main entry point for Gold multi-goal export job."""
    parser = argparse.ArgumentParser(description="Gold Layer Multi-Goal Dataset Export")
    
    args = parser.parse_args()
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("gold_multi_goal_export")
        
        # Run export
        job = GoldAggregationJob(spark)
        job.run()
        
    except Exception as e:
        logger.error(f"Gold multi-goal export failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
