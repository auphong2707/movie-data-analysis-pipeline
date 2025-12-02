"""
Export Gold Layer Baselines to MongoDB

Reads baseline data from Gold layer and exports to MongoDB batch_views collection
for serving layer queries.

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
    - Bulk upsert operations
    - Index management
    - Error handling and retry
    """
    
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
        """Create indexes on batch_views collection for baselines."""
        collection = self.db.batch_views
        
        indexes = [
            # Primary baseline index
            ([("genre", 1), ("type", 1)], {}),
            ([("type", 1), ("updated_at", -1)], {}),
            
            # Query optimization indexes
            ([("genre", 1)], {}),
            ([("updated_at", -1)], {}),
        ]
        
        for keys, options in indexes:
            try:
                collection.create_index(keys, **options)
                logger.info(f"Created index: {keys}")
            except Exception as e:
                logger.warning(f"Failed to create index {keys}: {str(e)}")
        
        self.metrics.add_metric("indexes_created", len(indexes))
    
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
        batch_size: int = 1000
    ) -> int:
        """
        Export Spark DataFrame (baselines) to MongoDB.
        
        Args:
            df: Spark DataFrame with baseline data
            batch_size: Batch size for bulk writes
        
        Returns:
            Number of documents exported
        """
        logger.info("Exporting baselines to MongoDB")
        
        # Convert DataFrame to list of dictionaries (preserving nested structures)
        records = df.collect()
        documents = [self._row_to_dict(row) for row in records]
        
        total_count = len(documents)
        logger.info(f"Prepared {total_count} baseline documents for export")
        
        # Bulk upsert in batches
        collection = self.db.batch_views
        exported_count = 0
        
        for i in range(0, total_count, batch_size):
            batch = documents[i:i + batch_size]
            
            # Create bulk operations
            operations = []
            for doc in batch:
                # Filter by genre and type for baseline documents
                filter_doc = {
                    "genre": doc.get("genre"),
                    "type": "baseline"
                }
                
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
                    f"Batch {i // batch_size + 1}: "
                    f"upserted={result.upserted_count}, modified={result.modified_count}"
                )
                
            except BulkWriteError as bwe:
                # Log errors but continue
                logger.error(f"Bulk write error: {bwe.details}", exc_info=True)
                # Count successful operations
                exported_count += len(batch) - len(bwe.details.get('writeErrors', []))
        
        logger.info(f"Exported {exported_count}/{total_count} baseline documents to MongoDB")
        self.metrics.add_metric("baselines_exported", exported_count)
        
        return exported_count


class MongoExportJob:
    """
    Job to export baseline data from Gold layer to MongoDB.
    """
    
    def __init__(self, spark, mongo_exporter: MongoDBExporter):
        self.spark = spark
        self.mongo_exporter = mongo_exporter
    
    @log_execution(logger, "mongo_export")
    def run(self):
        """
        Run MongoDB baseline export.
        """
        logger.info("Starting MongoDB baseline export")
        
        # Create indexes
        self.mongo_exporter.create_indexes()
        
        # Export baselines
        try:
            self._export_baselines()
        except Exception as e:
            logger.error(f"Failed to export baselines: {str(e)}", exc_info=True)
            raise
        
        # Log final metrics
        self.mongo_exporter.metrics.log(logger)
        logger.info("MongoDB baseline export completed successfully")
    
    def _export_baselines(self):
        """Export baselines from Gold to MongoDB."""
        logger.info("Exporting baselines")
        
        # Read from Gold layer
        gold_path = get_gold_path("baselines", None).rstrip('/')
        
        try:
            df = self.spark.read.parquet(gold_path)
            count = df.count()
            
            if count == 0:
                logger.warning("No baseline data found in Gold layer")
                return
            
            logger.info(f"Read {count} baseline records from {gold_path}")
            
            # Export to MongoDB
            exported = self.mongo_exporter.export_from_dataframe(df)
            
            logger.info(f"Successfully exported {exported} baseline records")
            
        except Exception as e:
            logger.error(f"Failed to export baselines: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for MongoDB baseline export job."""
    parser = argparse.ArgumentParser(description="Export Gold Layer Baselines to MongoDB")
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
        logger.error(f"MongoDB baseline export failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if mongo_exporter:
            mongo_exporter.close()
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
