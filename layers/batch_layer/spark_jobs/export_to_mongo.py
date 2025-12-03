"""
Export Gold Layer Multi-Goal Data to MongoDB

Reads three datasets from Gold layer and exports to MongoDB collections:
1. sentiment_baselines: Genre/franchise/director/temporal sentiment patterns
2. viral_thresholds: Genre/budget-tier/seasonal viral cutoffs
3. movie_intelligence: Individual movie competitive data

Usage:
    python export_to_mongo.py
"""

import argparse
import os
import sys
from datetime import datetime
from typing import List, Dict, Any

from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

# Add utils to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import get_spark_session, stop_spark_session
from utils.logger import get_logger, log_execution, JobMetrics
from utils.s3_utils import get_gold_path

logger = get_logger(__name__)


class MongoDBExporter:
    """
    Export Gold layer data to MongoDB.
    
    Features:
    - Bulk upsert operations for three collections
    - Index management per collection
    - Error handling and retry
    """
    
    # Collection configurations
    COLLECTIONS = {
        "sentiment_baselines": {
            "indexes": [
                ([("genre", 1), ("year", 1)], {}),
                ([("franchise", 1)], {}),
                ([("director", 1)], {}),
                ([("type", 1), ("updated_at", -1)], {}),
            ],
            "filter_keys": ["genre", "franchise", "director", "year"]
        },
        "viral_thresholds": {
            "indexes": [
                ([("genre", 1), ("budget_tier", 1), ("season", 1)], {}),
                ([("budget_tier", 1)], {}),
                ([("type", 1), ("updated_at", -1)], {}),
            ],
            "filter_keys": ["genre", "budget_tier", "season"]
        },
        "movie_intelligence": {
            "indexes": [
                ([("movie_id", 1)], {"unique": True}),
                ([("genre", 1), ("release_year", 1)], {}),
                ([("release_month", 1), ("release_year", 1)], {}),
                ([("franchise", 1)], {}),
                ([("director", 1)], {}),
                ([("budget_tier", 1), ("genre", 1)], {}),
                ([("type", 1), ("updated_at", -1)], {}),
            ],
            "filter_keys": ["movie_id"]
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
    
    def create_indexes(self, collection_name: str):
        """Create indexes on specified collection."""
        collection = self.db[collection_name]
        config = self.COLLECTIONS.get(collection_name)
        
        if not config:
            logger.warning(f"No index configuration for collection: {collection_name}")
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
    
    def export_from_dataframe(
        self,
        df,
        collection_name: str,
        batch_size: int = 1000
    ) -> int:
        """
        Export Spark DataFrame to MongoDB collection.
        
        Args:
            df: Spark DataFrame with data
            collection_name: Target MongoDB collection
            batch_size: Batch size for bulk writes
        
        Returns:
            Number of documents exported
        """
        logger.info(f"Exporting to MongoDB collection: {collection_name}")
        
        # Get collection config
        config = self.COLLECTIONS.get(collection_name)
        if not config:
            logger.error(f"Unknown collection: {collection_name}")
            return 0
        
        # Convert DataFrame to list of dictionaries (preserving nested structures)
        records = df.collect()
        documents = [self._row_to_dict(row) for row in records]
        
        total_count = len(documents)
        logger.info(f"Prepared {total_count} documents for {collection_name}")
        
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
                    for key in config["filter_keys"]
                    if doc.get(key) is not None
                }
                
                # Add type field for consistency
                filter_doc["type"] = doc.get("type")
                
                operations.append(
                    UpdateOne(
                        filter_doc,
                        {"$set": doc},
                        upsert=True
                    )
                )
            
            # Execute bulk write
            try:
                result = collection.bulk_write(operations, ordered=False)
                exported_count += result.upserted_count + result.modified_count
                
                logger.info(
                    f"{collection_name} batch {i // batch_size + 1}: "
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
    Job to export multi-goal data from Gold layer to MongoDB.
    
    Exports three datasets:
    1. sentiment_baselines
    2. viral_thresholds
    3. movie_intelligence
    """
    
    # Dataset names matching Gold layer output and MongoDB collections
    DATASETS = [
        "sentiment_baselines",
        "viral_thresholds",
        "movie_intelligence"
    ]
    
    def __init__(self, spark, mongo_exporter: MongoDBExporter):
        self.spark = spark
        self.mongo_exporter = mongo_exporter
    
    @log_execution(logger, "mongo_export")
    def run(self):
        """
        Run MongoDB export for all three datasets.
        """
        logger.info("Starting MongoDB multi-goal export")
        
        # Export each dataset
        for dataset_name in self.DATASETS:
            try:
                logger.info(f"Processing dataset: {dataset_name}")
                
                # Create indexes for this collection
                self.mongo_exporter.create_indexes(dataset_name)
                
                # Export data
                self._export_dataset(dataset_name)
                
            except Exception as e:
                logger.error(f"Failed to export {dataset_name}: {str(e)}", exc_info=True)
                # Continue with next dataset
        
        # Log final metrics
        self.mongo_exporter.metrics.log(logger)
        logger.info("MongoDB multi-goal export completed")
    
    def _export_dataset(self, dataset_name: str):
        """Export a single dataset from Gold to MongoDB."""
        logger.info(f"Exporting {dataset_name}")
        
        # Read from Gold layer
        gold_path = get_gold_path(dataset_name, None).rstrip('/')
        
        try:
            df = self.spark.read.parquet(gold_path)
            count = df.count()
            
            if count == 0:
                logger.warning(f"No data found in Gold layer for {dataset_name}")
                return
            
            logger.info(f"Read {count} records from {gold_path}")
            
            # Export to MongoDB
            exported = self.mongo_exporter.export_from_dataframe(df, dataset_name)
            
            logger.info(f"Successfully exported {exported} records to {dataset_name}")
            
        except Exception as e:
            logger.error(f"Failed to export {dataset_name}: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for MongoDB multi-goal export job."""
    parser = argparse.ArgumentParser(description="Export Gold Layer Multi-Goal Data to MongoDB")
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
