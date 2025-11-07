"""
Gold Layer Aggregations - Silver to Gold

Creates aggregated analytics views from Silver layer data.

Features:
- Genre analytics (by genre, year, month)
- Trending scores (7d, 30d, 90d rolling windows)
- Temporal analytics (year-over-year comparisons)
- Popularity metrics

Usage:
    python run.py --execution-date 2025-01-15
    python run.py --execution-date 2025-01-15 --metric-type genre_analytics
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))
from config.config import config
from layers.batch_layer.spark_jobs.utils.spark_session import get_spark_session, stop_spark_session
from layers.batch_layer.spark_jobs.utils.logging_utils import get_logger, log_dataframe_info, JobMetrics
from layers.batch_layer.spark_jobs.utils.transformations import calculate_window_aggregates, explode_genres

from pyspark.sql.functions import (
    col, avg, sum as spark_sum, count, max as spark_max, min as spark_min,
    collect_list, explode, year, month, lag, when, lit, current_timestamp,
    expr, datediff, current_date, row_number, round as spark_round
)
from pyspark.sql import Window

logger = get_logger(__name__)


class GoldAggregation:
    """Gold layer aggregation processor."""
    
    def __init__(self, execution_date: datetime):
        self.execution_date = execution_date
        self.spark = get_spark_session("gold_aggregation")
        self.metrics = JobMetrics("gold_aggregation", logger)
    
    def aggregate_genre_analytics(self) -> bool:
        """Create genre analytics aggregations."""
        logger.info("Creating genre analytics")
        
        try:
            # Read Silver movies
            silver_path = f"{config.hdfs_namenode}{config.hdfs_paths['silver']}/movies"
            movies_df = self.spark.read.parquet(silver_path)
            
            # Explode genres
            movies_exploded = movies_df.select(
                col("movie_id"),
                col("title"),
                col("release_date"),
                col("vote_average"),
                col("vote_count"),
                col("popularity"),
                col("revenue"),
                col("budget"),
                explode(col("genres")).alias("genre")
            ).filter(col("genre").isNotNull())
            
            # Aggregate by genre, year, month
            genre_agg = movies_exploded.withColumn(
                "year", year(col("release_date"))
            ).withColumn(
                "month", month(col("release_date"))
            ).groupBy("genre", "year", "month").agg(
                count("movie_id").alias("total_movies"),
                spark_round(avg("vote_average"), 2).alias("avg_rating"),
                spark_sum("revenue").alias("total_revenue"),
                spark_round(avg("budget"), 2).alias("avg_budget"),
                spark_round(avg("popularity"), 2).alias("avg_popularity"),
                collect_list("title").alias("movie_titles")
            ).withColumn(
                "top_movies",
                expr("slice(movie_titles, 1, 10)")
            ).drop("movie_titles").withColumn(
                "computed_timestamp", current_timestamp()
            ).withColumn(
                "partition_year", col("year")
            ).withColumn(
                "partition_month", col("month")
            )
            
            # Add avg_sentiment placeholder
            genre_agg = genre_agg.withColumn("avg_sentiment", lit(None).cast("double"))
            genre_agg = genre_agg.withColumn("total_reviews", lit(0))
            
            # Write to Gold
            gold_path = f"{config.hdfs_namenode}{config.hdfs_paths['gold']}/genre_analytics"
            genre_agg.write.mode("append").partitionBy("partition_year", "partition_month").parquet(gold_path)
            
            count = genre_agg.count()
            self.metrics.add_metric("genre_analytics_written", count)
            logger.info(f"Wrote {count} genre analytics records")
            return True
            
        except Exception as e:
            logger.error(f"Genre analytics failed: {e}", exc_info=True)
            return False
    
    def aggregate_trending_scores(self) -> bool:
        """Calculate trending scores with rolling windows."""
        logger.info("Calculating trending scores")
        
        try:
            silver_path = f"{config.hdfs_namenode}{config.hdfs_paths['silver']}/movies"
            movies_df = self.spark.read.parquet(silver_path)
            
            # Calculate trend metrics for different windows
            for window in ["7d", "30d", "90d"]:
                days = int(window[:-1])
                
                # Filter recent movies
                recent_movies = movies_df.filter(
                    datediff(current_date(), col("release_date")) <= days
                ).select(
                    col("movie_id"),
                    col("title"),
                    col("popularity"),
                    col("release_date")
                )
                
                # Calculate velocity and trend score
                window_spec = Window.partitionBy("movie_id").orderBy("release_date")
                
                trending = recent_movies.withColumn(
                    "popularity_prev",
                    lag("popularity", 1).over(window_spec)
                ).withColumn(
                    "velocity",
                    when(col("popularity_prev").isNotNull(),
                         col("popularity") - col("popularity_prev")
                    ).otherwise(0)
                ).withColumn(
                    "trend_score",
                    when(col("popularity_prev").isNotNull(),
                         (col("popularity") - col("popularity_prev")) / col("popularity_prev") * 100
                    ).otherwise(0)
                ).withColumn(
                    "window", lit(window)
                ).withColumn(
                    "popularity_start", col("popularity_prev")
                ).withColumn(
                    "popularity_end", col("popularity")
                ).withColumn(
                    "popularity_change_pct", col("trend_score")
                ).withColumn(
                    "acceleration", lit(0.0)
                ).withColumn(
                    "computed_date", current_date()
                ).withColumn(
                    "computed_timestamp", current_timestamp()
                ).withColumn(
                    "partition_window", lit(window)
                ).withColumn(
                    "partition_year", year(current_date())
                ).withColumn(
                    "partition_month", month(current_date())
                ).drop("popularity_prev")
                
                # Write to Gold
                gold_path = f"{config.hdfs_namenode}{config.hdfs_paths['gold']}/trending"
                trending.write.mode("append").partitionBy("partition_window", "partition_year", "partition_month").parquet(gold_path)
                
                count = trending.count()
                self.metrics.add_metric(f"trending_{window}_written", count)
                logger.info(f"Wrote {count} trending records for {window} window")
            
            return True
            
        except Exception as e:
            logger.error(f"Trending scores failed: {e}", exc_info=True)
            return False
    
    def aggregate_temporal_analytics(self) -> bool:
        """Create temporal analytics (year-over-year)."""
        logger.info("Creating temporal analytics")
        
        try:
            silver_path = f"{config.hdfs_namenode}{config.hdfs_paths['silver']}/movies"
            movies_df = self.spark.read.parquet(silver_path)
            
            # Aggregate by year and month
            temporal = movies_df.withColumn(
                "year", year(col("release_date"))
            ).withColumn(
                "month", month(col("release_date"))
            ).groupBy("year", "month").agg(
                count("movie_id").alias("total_movies"),
                spark_sum("revenue").alias("total_revenue"),
                spark_round(avg("vote_average"), 2).alias("avg_rating"),
                spark_round(avg("budget"), 2).alias("avg_budget")
            ).withColumn(
                "computed_timestamp", current_timestamp()
            ).withColumn(
                "partition_year", col("year")
            ).withColumn(
                "partition_month", col("month")
            )
            
            # Calculate YoY growth (placeholder)
            temporal = temporal.withColumn("yoy_movie_growth_pct", lit(None).cast("double"))
            temporal = temporal.withColumn("yoy_revenue_growth_pct", lit(None).cast("double"))
            temporal = temporal.withColumn("top_genre", lit(None).cast("string"))
            temporal = temporal.withColumn("avg_sentiment", lit(None).cast("double"))
            
            # Write to Gold
            gold_path = f"{config.hdfs_namenode}{config.hdfs_paths['gold']}/temporal_analytics"
            temporal.write.mode("append").partitionBy("partition_year", "partition_month").parquet(gold_path)
            
            count = temporal.count()
            self.metrics.add_metric("temporal_analytics_written", count)
            logger.info(f"Wrote {count} temporal analytics records")
            return True
            
        except Exception as e:
            logger.error(f"Temporal analytics failed: {e}", exc_info=True)
            return False
    
    def run(self, metric_type: Optional[str] = None) -> bool:
        """Run Gold aggregations."""
        logger.info(f"Starting Gold aggregations for {metric_type or 'all metrics'}")
        
        success = True
        
        try:
            if metric_type is None or metric_type == "genre_analytics":
                if not self.aggregate_genre_analytics():
                    success = False
            
            if metric_type is None or metric_type == "trending":
                if not self.aggregate_trending_scores():
                    success = False
            
            if metric_type is None or metric_type == "temporal_analytics":
                if not self.aggregate_temporal_analytics():
                    success = False
            
            self.metrics.finish(success=success)
            return success
            
        except Exception as e:
            logger.error(f"Gold aggregation failed: {e}", exc_info=True)
            self.metrics.finish(success=False)
            return False
        finally:
            stop_spark_session(self.spark)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Gold Layer Aggregations")
    parser.add_argument(
        "--execution-date",
        type=str,
        required=True,
        help="Execution date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--metric-type",
        type=str,
        choices=["genre_analytics", "trending", "temporal_analytics"],
        help="Specific metric type to aggregate"
    )
    
    args = parser.parse_args()
    
    execution_date = datetime.strptime(args.execution_date, "%Y-%m-%d")
    
    logger.info("Starting Gold aggregations", extra={
        "execution_date": execution_date.isoformat(),
        "metric_type": args.metric_type or "all"
    })
    
    aggregation = GoldAggregation(execution_date)
    success = aggregation.run(args.metric_type)
    
    if success:
        print(f"\n✅ Gold aggregations completed successfully!")
        sys.exit(0)
    else:
        print(f"\n❌ Gold aggregations failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
