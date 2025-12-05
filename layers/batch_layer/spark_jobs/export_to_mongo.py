"""
Export Gold Layer Unified Batch Views to MongoDB

Reads unified batch_views dataset from Gold layer and exports to MongoDB batch_views collection.
The collection contains three view types distinguished by view_type field:
1. sentiment_baseline: Genre/franchise/yearly sentiment patterns
2. viral_threshold: Genre/budget-tier/seasonal viral cutoffs
3. movie_intelligence: Individual movie competitive data

Usage:
    python export_to_mongo.py
"""

import argparse
import os
import sys
from datetime import datetime
from typing import List, Dict, Any

from pymongo import MongoClient, UpdateOne, ReplaceOne, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

# Add utils to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import get_spark_session, stop_spark_session
from utils.logger import get_logger, log_execution, JobMetrics
from utils.s3_utils import get_gold_path

logger = get_logger(__name__)


class MongoDBExporter:
    """
    Export Gold layer unified batch_views to MongoDB.
    
    Features:
    - Single batch_views collection with view_type discriminator
    - Compound indexes for efficient querying by view_type + dimensions
    - Bulk upsert operations with proper filtering
    - Error handling and retry
    """
    
    # Unified batch_views collection configuration
    BATCH_VIEWS_CONFIG = {
        "indexes": [
            # view_type queries
            ([("view_type", 1)], {}),
            
            # Sentiment baseline queries (view_type + dimension)
            ([("view_type", 1), ("genre", 1)], {}),
            ([("view_type", 1), ("franchise", 1)], {}),
            ([("view_type", 1), ("year", 1)], {}),
            
            # Viral threshold queries (view_type + dimension)
            ([("view_type", 1), ("genre", 1), ("budget_tier", 1), ("season", 1)], {}),
            ([("view_type", 1), ("budget_tier", 1)], {}),
            ([("view_type", 1), ("season", 1)], {}),
            
            # Movie intelligence queries (view_type + movie_id)
            ([("view_type", 1), ("movie_id", 1)], {}),
            ([("view_type", 1), ("genre", 1), ("release_year", 1)], {}),
            ([("view_type", 1), ("franchise", 1)], {}),
            
            # Temporal metadata queries
            ([("batch_run_timestamp", -1)], {}),
            ([("aggregation_granularity", 1), ("view_type", 1)], {}),
        ],
        # Filter keys depend on view_type
        "filter_keys_by_view_type": {
            "sentiment_baseline": ["view_type", "genre", "franchise", "year"],
            "viral_threshold": ["view_type", "genre", "budget_tier", "season"],
            "movie_intelligence": ["view_type", "movie_id"]
        }
    }
    
    def __init__(self, connection_string: str, database: str = "moviedb"):
        """
        Initialize MongoDB exporter.
        
        Args:
            connection_string: MongoDB connection string
            database: Database name (default: moviedb)
        """
        self.client = MongoClient(connection_string)
        self.db = self.client[database]
        self.metrics = JobMetrics("mongo_export")
        
        logger.info(f"Connected to MongoDB: {database}")
    
    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("Closed MongoDB connection")
    
    def create_indexes(self):
        """Create indexes on batch_views collection."""
        collection = self.db["batch_views"]
        
        logger.info("Creating indexes on batch_views collection")
        
        indexes = self.BATCH_VIEWS_CONFIG["indexes"]
        
        for keys, options in indexes:
            try:
                collection.create_index(keys, **options)
                logger.info(f"Created index on batch_views: {keys}")
            except Exception as e:
                logger.warning(f"Failed to create index {keys} on batch_views: {str(e)}")
        
        self.metrics.add_metric("batch_views_indexes_created", len(indexes))
    
    def _row_to_dict(self, row):
        """
        Recursively convert Row to dictionary, preserving nested structures.
        """
        if row is None:
            return None
        
        result = {}
        for key in row.__fields__:
            value = getattr(row, key)
            
            # Handle nested Row (struct)
            if hasattr(value, '__fields__'):
                result[key] = self._row_to_dict(value)
            # Handle list of Rows
            elif isinstance(value, list) and len(value) > 0 and hasattr(value[0], '__fields__'):
                result[key] = [self._row_to_dict(item) for item in value]
            # Handle regular values
            else:
                result[key] = value
        
        return result
    
    def export_batch_views(
        self,
        df,
        batch_size: int = 1000
    ) -> int:
        """
        Export unified batch_views DataFrame to MongoDB.
        
        Uses view_type field to determine appropriate filter keys for upserts.
        
        Args:
            df: Spark DataFrame with unified batch_views data
            batch_size: Batch size for bulk writes
        
        Returns:
            Number of documents exported
        """
        logger.info("Exporting to MongoDB collection: batch_views")
        
        # Convert DataFrame to list of dictionaries (preserving nested structures)
        records = df.collect()
        documents = [self._row_to_dict(row) for row in records]
        
        total_count = len(documents)
        logger.info(f"Prepared {total_count} documents for batch_views")
        
        # Bulk upsert in batches
        collection = self.db["batch_views"]
        exported_count = 0
        
        for i in range(0, total_count, batch_size):
            batch = documents[i:i + batch_size]
            
            # Create bulk operations
            operations = []
            for doc in batch:
                # Determine filter keys based on view_type
                view_type = doc.get("view_type")
                filter_keys = self.BATCH_VIEWS_CONFIG["filter_keys_by_view_type"].get(
                    view_type, 
                    ["view_type"]  # Fallback: at least filter by view_type
                )
                
                # Build filter from configured keys (skip None values)
                filter_doc = {
                    key: doc.get(key)
                    for key in filter_keys
                    if doc.get(key) is not None
                }
                
                operations.append(
                    ReplaceOne(
                        filter_doc,
                        doc,
                        upsert=True
                    )
                )
            
            # Execute bulk write
            try:
                result = collection.bulk_write(operations, ordered=False)
                exported_count += result.upserted_count + result.modified_count
                
                logger.info(
                    f"batch_views batch {i // batch_size + 1}: "
                    f"upserted={result.upserted_count}, modified={result.modified_count}"
                )
                
            except BulkWriteError as bwe:
                # Log errors but continue
                logger.error(f"Bulk write error in batch_views: {bwe.details}", exc_info=True)
                # Count successful operations
                exported_count += len(batch) - len(bwe.details.get('writeErrors', []))
        
        logger.info(f"Exported {exported_count}/{total_count} documents to batch_views")
        self.metrics.add_metric("batch_views_exported", exported_count)
        
        return exported_count


class MongoExportJob:
    """
    Job to export unified batch_views from Gold layer to MongoDB.
    
    Exports single unified batch_views dataset containing:
    1. sentiment_baseline (view_type discriminator)
    2. viral_threshold (view_type discriminator)
    3. movie_intelligence (view_type discriminator)
    """
    
    def __init__(self, spark, mongo_exporter: MongoDBExporter):
        self.spark = spark
        self.mongo_exporter = mongo_exporter
    
    @log_execution(logger, "mongo_export")
    def run(self):
        """
        Run MongoDB export for unified batch_views.
        """
        logger.info("Starting MongoDB unified batch_views export")
        
        # Create indexes for batch_views collection
        self.mongo_exporter.create_indexes()
        
        # Export unified batch_views
        self._export_batch_views()
        
        # Log final metrics
        self.mongo_exporter.metrics.log(logger)
        logger.info("MongoDB unified batch_views export completed")
    
    def _export_batch_views(self):
        """Export unified batch_views from Gold to MongoDB."""
        logger.info("Exporting batch_views")
        
        # Read unified batch_views from Gold layer
        gold_path = get_gold_path("batch_views", None).rstrip('/')
        
        try:
            df = self.spark.read.parquet(gold_path)
            count = df.count()
            
            if count == 0:
                logger.warning("No data found in Gold layer for batch_views")
                return
            
            logger.info(f"Read {count} records from {gold_path}")
            
            # Show view_type distribution
            view_type_counts = df.groupBy("view_type").count().collect()
            for row in view_type_counts:
                logger.info(f"  view_type={row['view_type']}: {row['count']} documents")
            
            # Export to MongoDB batch_views collection
            exported = self.mongo_exporter.export_batch_views(df)
            
            logger.info(f"Successfully exported {exported} records to batch_views")
            
        except Exception as e:
            logger.error(f"Failed to export batch_views: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for MongoDB unified batch_views export job."""
    parser = argparse.ArgumentParser(description="Export Gold Layer Unified Batch Views to MongoDB")
    parser.add_argument("--mongo-uri", type=str,
                       default=None,
                       help="MongoDB connection string (default: from env)")
    parser.add_argument("--database", type=str, default="moviedb",
                       help="MongoDB database name (default: moviedb)")
    
    args = parser.parse_args()
    
    # Get MongoDB connection from args or environment
    mongo_uri = args.mongo_uri or os.getenv('MONGODB_CONNECTION_STRING')
    if not mongo_uri:
        logger.error("MongoDB connection string not provided")
        sys.exit(1)
    
    spark = None
    mongo_exporter = None
    
    try:
        # Create Spark session
        spark = get_spark_session("mongo_export")
        
        # Create MongoDB exporter
        mongo_exporter = MongoDBExporter(mongo_uri, args.database)
        
        # Run export
        job = MongoExportJob(spark, mongo_exporter)
        job.run()
        
    except Exception as e:
        logger.error(f"MongoDB multi-goal export failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if mongo_exporter:
            mongo_exporter.close()
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
