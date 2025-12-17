"""
Export Gold Layer to 3 Separate MongoDB Collections

Reads three separate Gold layer datasets and exports to 3 MongoDB collections:
1. sentiment_baselines: Genre/franchise/yearly sentiment patterns (Business Goal #1: PR Crisis Detection)
2. viral_thresholds: Genre/budget-tier/seasonal viral cutoffs (Business Goal #2: Viral Content Detection)
3. movie_intelligence: Individual movie competitive data (Business Goal #3: Content Recommendation)

Each collection maintains its own clean schema without null pollution from other view types.

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
    Export Gold layer to 3 separate MongoDB collections.
    
    Features:
    - 3 separate collections: sentiment_baselines, viral_thresholds, movie_intelligence
    - Custom indexes per collection (no view_type field needed)
    - Bulk upsert operations with collection-specific filter keys
    - Error handling and retry
    """
    
    # Configuration for 3 separate collections
    COLLECTIONS_CONFIG = {
        "sentiment_baselines": {
            "indexes": [
                ([("genre", 1)], {}),
                ([("franchise", 1)], {}),
                ([("year", 1)], {}),
                ([("genre", 1), ("year", 1)], {}),
                ([("franchise", 1), ("year", 1)], {}),
                ([("batch_run_timestamp", -1)], {}),
            ],
            "filter_keys": ["genre", "franchise", "year"]
        },
        "viral_thresholds": {
            "indexes": [
                ([("genre", 1)], {}),
                ([("budget_tier", 1)], {}),
                ([("season", 1)], {}),
                ([("genre", 1), ("budget_tier", 1), ("season", 1)], {}),
                ([("batch_run_timestamp", -1)], {}),
            ],
            "filter_keys": ["genre", "budget_tier", "season"]
        },
        "movie_intelligence": {
            "indexes": [
                ([("movie_id", 1)], {"unique": True}),
                ([("genre", 1)], {}),
                ([("release_year", 1)], {}),
                ([("franchise", 1)], {}),
                ([("genre", 1), ("release_year", 1)], {}),
                ([("budget_tier", 1)], {}),
                ([("batch_run_timestamp", -1)], {}),
            ],
            "filter_keys": ["movie_id"]
        }
    }
    
    DATASETS = ["sentiment_baselines", "viral_thresholds", "movie_intelligence"]
    
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
    
    def create_indexes(self, collection_name: str):
        """Create indexes on a specific collection."""
        collection = self.db[collection_name]
        
        logger.info(f"Creating indexes on {collection_name} collection")
        
        config = self.COLLECTIONS_CONFIG.get(collection_name)
        if not config:
            logger.warning(f"No configuration found for collection: {collection_name}")
            return
        
        indexes = config["indexes"]
        
        for keys, options in indexes:
            try:
                collection.create_index(keys, **options)
                logger.info(f"Created index on {collection_name}: {keys}")
            except Exception as e:
                logger.warning(f"Failed to create index {keys} on {collection_name}: {str(e)}")
        
        self.metrics.add_metric(f"{collection_name}_indexes_created", len(indexes))
    
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
    
    def export_collection(
        self,
        df,
        collection_name: str,
        batch_size: int = 1000
    ) -> int:
        """
        Export DataFrame to a specific MongoDB collection.
        
        Args:
            df: Spark DataFrame with collection data
            collection_name: Name of the MongoDB collection
            batch_size: Batch size for bulk writes
        
        Returns:
            Number of documents exported
        """
        logger.info(f"Exporting to MongoDB collection: {collection_name}")
        
        # Convert DataFrame to list of dictionaries (preserving nested structures)
        records = df.collect()
        documents = [self._row_to_dict(row) for row in records]
        
        total_count = len(documents)
        logger.info(f"Prepared {total_count} documents for {collection_name}")
        
        # Get collection config
        config = self.COLLECTIONS_CONFIG.get(collection_name)
        if not config:
            logger.error(f"No configuration found for collection: {collection_name}")
            return 0
        
        filter_keys = config["filter_keys"]
        
        # Bulk upsert in batches
        collection = self.db[collection_name]
        exported_count = 0
        
        for i in range(0, total_count, batch_size):
            batch = documents[i:i + batch_size]
            
            # Create bulk operations
            operations = []
            for doc in batch:
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
                logger.error(f"Bulk write error in {collection_name}: {bwe.details}", exc_info=True)
                # Count successful operations
                exported_count += len(batch) - len(bwe.details.get('writeErrors', []))
        
        logger.info(f"Exported {exported_count}/{total_count} documents to {collection_name}")
        self.metrics.add_metric(f"{collection_name}_exported", exported_count)
        
        return exported_count


class MongoExportJob:
    """
    Job to export 3 separate collections from Gold layer to MongoDB.
    
    Exports 3 collections for 3 business goals:
    1. sentiment_baselines: For PR Crisis Detection (Business Goal #1)
    2. viral_thresholds: For Viral Content Detection (Business Goal #2)
    3. movie_intelligence: For Content Recommendation (Business Goal #3)
    """
    
    def __init__(self, spark, mongo_exporter: MongoDBExporter):
        self.spark = spark
        self.mongo_exporter = mongo_exporter
    
    @log_execution(logger, "mongo_export")
    def run(self):
        """
        Run MongoDB export for all 3 collections.
        """
        logger.info("Starting MongoDB 3-collection export (for 3 business goals)")
        
        # Export each collection separately
        for collection_name in self.mongo_exporter.DATASETS:
            try:
                # Create indexes
                self.mongo_exporter.create_indexes(collection_name)
                
                # Export data
                self._export_collection(collection_name)
                
            except Exception as e:
                logger.error(f"Failed to export {collection_name}: {str(e)}", exc_info=True)
                # Continue with next collection
        
        # Log final metrics
        self.mongo_exporter.metrics.log(logger)
        logger.info("MongoDB 3-collection export completed")
    
    def _export_collection(self, collection_name: str):
        """Export a single collection from Gold to MongoDB."""
        logger.info(f"Exporting {collection_name}")
        
        # Read from Gold layer (separate path for each collection)
        gold_path = get_gold_path(collection_name, None).rstrip('/')
        
        try:
            df = self.spark.read.parquet(gold_path)
            count = df.count()
            
            if count == 0:
                logger.warning(f"No data found in Gold layer for {collection_name}")
                return
            
            logger.info(f"Read {count} records from {gold_path}")
            
            # Export to MongoDB
            exported = self.mongo_exporter.export_collection(df, collection_name)
            
            logger.info(f"Successfully exported {exported} records to {collection_name}")
            
        except Exception as e:
            logger.error(f"Failed to export {collection_name}: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for MongoDB 3-collection export job."""
    parser = argparse.ArgumentParser(description="Export Gold Layer to 3 MongoDB Collections (3 Business Goals)")
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
