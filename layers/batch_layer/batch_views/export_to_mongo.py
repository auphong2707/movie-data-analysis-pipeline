"""
MongoDB Export - Gold Layer to Serving Layer

Exports Gold layer aggregations to MongoDB batch_views collection.

Features:
- Bulk upsert to MongoDB
- Index creation and maintenance
- Support for all Gold metrics
- Idempotent operations

Usage:
    python export_to_mongo.py --execution-date 2025-01-15
    python export_to_mongo.py --execution-date 2025-01-15 --metric-type genre_analytics
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
from config.config import config
from layers.batch_layer.spark_jobs.utils.spark_session import get_spark_session, stop_spark_session
from layers.batch_layer.spark_jobs.utils.logging_utils import get_logger, JobMetrics

logger = get_logger(__name__)


class MongoDBExporter:
    """
    Export Gold layer data to MongoDB batch_views collection.
    """
    
    def __init__(self, execution_date: datetime):
        """
        Initialize MongoDB exporter.
        
        Args:
            execution_date: Execution date for batch run ID
        """
        self.execution_date = execution_date
        self.batch_run_id = f"batch_{execution_date.strftime('%Y_%m_%d_%H')}"
        
        # MongoDB connection
        self.mongo_client = MongoClient(config.mongodb_connection_string)
        self.db = self.mongo_client[config.mongodb_database]
        self.collection = self.db['batch_views']
        
        # Spark session
        self.spark = get_spark_session("mongodb_export")
        
        # Metrics
        self.metrics = JobMetrics("mongodb_export", logger)
        
        # Ensure indexes exist
        self._create_indexes()
    
    def _create_indexes(self):
        """Create MongoDB indexes for batch_views collection."""
        logger.info("Creating MongoDB indexes")
        
        try:
            # Compound indexes for common query patterns
            self.collection.create_index([
                ("view_type", ASCENDING),
                ("data.genre", ASCENDING),
                ("data.year", ASCENDING)
            ], name="view_type_genre_year_idx")
            
            self.collection.create_index([
                ("view_type", ASCENDING),
                ("data.movie_id", ASCENDING)
            ], name="view_type_movie_id_idx")
            
            self.collection.create_index([
                ("view_type", ASCENDING),
                ("computed_at", DESCENDING)
            ], name="view_type_computed_at_idx")
            
            self.collection.create_index([
                ("batch_run_id", ASCENDING)
            ], name="batch_run_id_idx")
            
            # Index for temporal queries
            self.collection.create_index([
                ("data.year", ASCENDING),
                ("data.month", ASCENDING)
            ], name="year_month_idx")
            
            logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")
    
    def export_genre_analytics(self) -> int:
        """
        Export genre analytics to MongoDB.
        
        Returns:
            Number of documents exported
        """
        logger.info("Exporting genre analytics to MongoDB")
        
        try:
            # Read Gold genre analytics
            gold_path = f"{config.hdfs_namenode}{config.hdfs_paths['gold']}/genre_analytics"
            df = self.spark.read.parquet(gold_path)
            
            # Convert to list of dicts
            records = df.toPandas().to_dict('records')
            
            if not records:
                logger.warning("No genre analytics records to export")
                return 0
            
            # Prepare bulk operations
            operations = []
            
            for record in records:
                doc = {
                    "view_type": "genre_analytics",
                    "data": {
                        "genre": record.get("genre"),
                        "year": record.get("year"),
                        "month": record.get("month"),
                        "total_movies": record.get("total_movies"),
                        "avg_rating": record.get("avg_rating"),
                        "total_revenue": record.get("total_revenue"),
                        "avg_budget": record.get("avg_budget"),
                        "avg_popularity": record.get("avg_popularity"),
                        "avg_sentiment": record.get("avg_sentiment"),
                        "total_reviews": record.get("total_reviews"),
                        "top_movies": record.get("top_movies", [])
                    },
                    "computed_at": record.get("computed_timestamp", datetime.now()),
                    "batch_run_id": self.batch_run_id
                }
                
                # Upsert based on view_type, genre, year, month
                operations.append(UpdateOne(
                    {
                        "view_type": "genre_analytics",
                        "data.genre": record.get("genre"),
                        "data.year": record.get("year"),
                        "data.month": record.get("month")
                    },
                    {"$set": doc},
                    upsert=True
                ))
            
            # Execute bulk write
            if operations:
                result = self.collection.bulk_write(operations, ordered=False)
                logger.info(f"Genre analytics export: {result.upserted_count} inserted, "
                          f"{result.modified_count} modified")
                return result.upserted_count + result.modified_count
            
            return 0
            
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            return 0
        except Exception as e:
            logger.error(f"Failed to export genre analytics: {e}", exc_info=True)
            return 0
    
    def export_trending_scores(self) -> int:
        """Export trending scores to MongoDB."""
        logger.info("Exporting trending scores to MongoDB")
        
        try:
            gold_path = f"{config.hdfs_namenode}{config.hdfs_paths['gold']}/trending"
            df = self.spark.read.parquet(gold_path)
            
            records = df.toPandas().to_dict('records')
            
            if not records:
                logger.warning("No trending records to export")
                return 0
            
            operations = []
            
            for record in records:
                doc = {
                    "view_type": "trending",
                    "data": {
                        "movie_id": record.get("movie_id"),
                        "title": record.get("title"),
                        "window": record.get("window"),
                        "trend_score": record.get("trend_score"),
                        "velocity": record.get("velocity"),
                        "acceleration": record.get("acceleration"),
                        "popularity_start": record.get("popularity_start"),
                        "popularity_end": record.get("popularity_end"),
                        "popularity_change_pct": record.get("popularity_change_pct"),
                        "computed_date": record.get("computed_date")
                    },
                    "computed_at": record.get("computed_timestamp", datetime.now()),
                    "batch_run_id": self.batch_run_id
                }
                
                operations.append(UpdateOne(
                    {
                        "view_type": "trending",
                        "data.movie_id": record.get("movie_id"),
                        "data.window": record.get("window")
                    },
                    {"$set": doc},
                    upsert=True
                ))
            
            if operations:
                result = self.collection.bulk_write(operations, ordered=False)
                logger.info(f"Trending export: {result.upserted_count} inserted, "
                          f"{result.modified_count} modified")
                return result.upserted_count + result.modified_count
            
            return 0
            
        except Exception as e:
            logger.error(f"Failed to export trending scores: {e}", exc_info=True)
            return 0
    
    def export_temporal_analytics(self) -> int:
        """Export temporal analytics to MongoDB."""
        logger.info("Exporting temporal analytics to MongoDB")
        
        try:
            gold_path = f"{config.hdfs_namenode}{config.hdfs_paths['gold']}/temporal_analytics"
            df = self.spark.read.parquet(gold_path)
            
            records = df.toPandas().to_dict('records')
            
            if not records:
                logger.warning("No temporal analytics records to export")
                return 0
            
            operations = []
            
            for record in records:
                doc = {
                    "view_type": "temporal_analytics",
                    "data": {
                        "year": record.get("year"),
                        "month": record.get("month"),
                        "total_movies": record.get("total_movies"),
                        "total_revenue": record.get("total_revenue"),
                        "avg_rating": record.get("avg_rating"),
                        "avg_budget": record.get("avg_budget"),
                        "top_genre": record.get("top_genre"),
                        "avg_sentiment": record.get("avg_sentiment"),
                        "yoy_movie_growth_pct": record.get("yoy_movie_growth_pct"),
                        "yoy_revenue_growth_pct": record.get("yoy_revenue_growth_pct")
                    },
                    "computed_at": record.get("computed_timestamp", datetime.now()),
                    "batch_run_id": self.batch_run_id
                }
                
                operations.append(UpdateOne(
                    {
                        "view_type": "temporal_analytics",
                        "data.year": record.get("year"),
                        "data.month": record.get("month")
                    },
                    {"$set": doc},
                    upsert=True
                ))
            
            if operations:
                result = self.collection.bulk_write(operations, ordered=False)
                logger.info(f"Temporal analytics export: {result.upserted_count} inserted, "
                          f"{result.modified_count} modified")
                return result.upserted_count + result.modified_count
            
            return 0
            
        except Exception as e:
            logger.error(f"Failed to export temporal analytics: {e}", exc_info=True)
            return 0
    
    def run(self, metric_type: Optional[str] = None) -> bool:
        """
        Run MongoDB export.
        
        Args:
            metric_type: Specific metric type to export (or None for all)
            
        Returns:
            True if successful
        """
        logger.info(f"Starting MongoDB export for {metric_type or 'all metrics'}")
        
        total_exported = 0
        
        try:
            if metric_type is None or metric_type == "genre_analytics":
                count = self.export_genre_analytics()
                total_exported += count
                self.metrics.add_metric("genre_analytics_exported", count)
            
            if metric_type is None or metric_type == "trending":
                count = self.export_trending_scores()
                total_exported += count
                self.metrics.add_metric("trending_exported", count)
            
            if metric_type is None or metric_type == "temporal_analytics":
                count = self.export_temporal_analytics()
                total_exported += count
                self.metrics.add_metric("temporal_analytics_exported", count)
            
            self.metrics.add_metric("total_exported", total_exported)
            self.metrics.finish(success=True)
            
            logger.info(f"MongoDB export completed: {total_exported} documents")
            return True
            
        except Exception as e:
            logger.error(f"MongoDB export failed: {e}", exc_info=True)
            self.metrics.finish(success=False)
            return False
        finally:
            stop_spark_session(self.spark)
            self.mongo_client.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Export Gold Layer to MongoDB")
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
        help="Specific metric type to export"
    )
    
    args = parser.parse_args()
    
    execution_date = datetime.strptime(args.execution_date, "%Y-%m-%d")
    
    logger.info("Starting MongoDB export", extra={
        "execution_date": execution_date.isoformat(),
        "metric_type": args.metric_type or "all"
    })
    
    exporter = MongoDBExporter(execution_date)
    success = exporter.run(args.metric_type)
    
    if success:
        print(f"\n✅ MongoDB export completed successfully!")
        sys.exit(0)
    else:
        print(f"\n❌ MongoDB export failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
