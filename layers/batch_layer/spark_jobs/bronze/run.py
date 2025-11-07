"""
Bronze Layer - Spark Job

Alternative Bronze layer implementation using pure Spark.
Can be used if direct Python ingestion is not preferred.

This reads from a staging area and writes to partitioned Bronze HDFS.
"""

import argparse
import os
import sys
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))
from config.config import config
from layers.batch_layer.spark_jobs.utils.spark_session import get_spark_session, stop_spark_session
from layers.batch_layer.spark_jobs.utils.logging_utils import get_logger, JobMetrics
from layers.batch_layer.spark_jobs.utils.hdfs_utils import HDFSManager

from pyspark.sql.functions import col, current_timestamp, year, month, dayofmonth, hour as spark_hour

logger = get_logger(__name__)


class BronzeSparkJob:
    """
    Spark job for Bronze layer processing.
    Reads raw data and writes to partitioned Bronze layer.
    """
    
    def __init__(self, execution_date: datetime):
        self.execution_date = execution_date
        self.spark = get_spark_session("bronze_spark_job")
        self.hdfs_manager = HDFSManager()
        self.metrics = JobMetrics("bronze_spark_job", logger)
    
    def process_movies(self, input_path: str) -> bool:
        """
        Process movies data to Bronze layer.
        
        Args:
            input_path: Path to input data (JSON or Parquet)
            
        Returns:
            True if successful
        """
        logger.info(f"Processing movies from {input_path}")
        
        try:
            # Read input data
            df = self.spark.read.json(input_path)
            
            # Add extraction metadata
            df = df.withColumn("extraction_timestamp", current_timestamp()) \
                .withColumn("partition_year", year(col("extraction_timestamp"))) \
                .withColumn("partition_month", month(col("extraction_timestamp"))) \
                .withColumn("partition_day", dayofmonth(col("extraction_timestamp"))) \
                .withColumn("partition_hour", spark_hour(col("extraction_timestamp")))
            
            # Write to Bronze
            output_path = f"{config.hdfs_namenode}{config.hdfs_paths['bronze']}/movies"
            
            df.write \
                .mode("append") \
                .partitionBy("partition_year", "partition_month", "partition_day", "partition_hour") \
                .parquet(output_path)
            
            count = df.count()
            self.metrics.add_metric("movies_processed", count)
            logger.info(f"Processed {count} movies to Bronze layer")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process movies: {e}", exc_info=True)
            return False
    
    def run(self, input_path: str, data_type: str = "movies") -> bool:
        """
        Run Bronze Spark job.
        
        Args:
            input_path: Path to input data
            data_type: Type of data (movies, reviews, etc.)
            
        Returns:
            True if successful
        """
        logger.info(f"Starting Bronze Spark job for {data_type}")
        
        try:
            if data_type == "movies":
                success = self.process_movies(input_path)
            else:
                logger.error(f"Unsupported data type: {data_type}")
                success = False
            
            self.metrics.finish(success=success)
            return success
            
        except Exception as e:
            logger.error(f"Bronze Spark job failed: {e}", exc_info=True)
            self.metrics.finish(success=False)
            return False
        finally:
            stop_spark_session(self.spark)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Bronze Layer Spark Job")
    parser.add_argument(
        "--input-path",
        type=str,
        required=True,
        help="Input data path (HDFS or local)"
    )
    parser.add_argument(
        "--data-type",
        type=str,
        default="movies",
        choices=["movies", "reviews"],
        help="Type of data to process"
    )
    parser.add_argument(
        "--execution-date",
        type=str,
        help="Execution date (YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    
    execution_date = datetime.now()
    if args.execution_date:
        execution_date = datetime.strptime(args.execution_date, "%Y-%m-%d")
    
    logger.info("Starting Bronze Spark job", extra={
        "input_path": args.input_path,
        "data_type": args.data_type,
        "execution_date": execution_date.isoformat()
    })
    
    job = BronzeSparkJob(execution_date)
    success = job.run(args.input_path, args.data_type)
    
    if success:
        print(f"\n✅ Bronze Spark job completed successfully!")
        sys.exit(0)
    else:
        print(f"\n❌ Bronze Spark job failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
