"""
Export Gold Layer Data to MongoDB

Reads Gold layer aggregations and exports to MongoDB batch_views collection
for serving layer queries.

Usage:
    python export_to_mongo.py --collections genre_analytics,trending_scores
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
        """Create indexes on batch_views collection."""
        collection = self.db.batch_views
        
        indexes = [
            # Compound index for view_type and filtering
            ([("view_type", ASCENDING), ("genre", ASCENDING), ("year", DESCENDING)], {}),
            ([("view_type", ASCENDING), ("movie_id", ASCENDING)], {}),
            ([("computed_at", DESCENDING)], {}),
            ([("view_type", ASCENDING), ("window", ASCENDING)], {}),
        ]
        
        for keys, options in indexes:
            try:
                collection.create_index(keys, **options)
                logger.info(f"Created index: {keys}")
            except Exception as e:
                logger.warning(f"Failed to create index {keys}: {str(e)}")
        
        self.metrics.add_metric("indexes_created", len(indexes))
    
    def export_from_dataframe(
        self,
        df,
        view_type: str,
        batch_size: int = 1000
    ) -> int:
        """
        Export Spark DataFrame to MongoDB.
        
        Args:
            df: Spark DataFrame
            view_type: Type of view (genre_analytics, trending_scores, etc.)
            batch_size: Batch size for bulk writes
        
        Returns:
            Number of documents exported
        """
        logger.info(f"Exporting {view_type} to MongoDB")
        
        # Convert DataFrame to list of dictionaries
        records = df.collect()
        documents = [row.asDict() for row in records]
        
        # Add view_type to each document
        for doc in documents:
            doc['view_type'] = view_type
            # Convert any None values to null for MongoDB
            doc = {k: v for k, v in doc.items() if v is not None}
        
        total_count = len(documents)
        logger.info(f"Prepared {total_count} documents for export")
        
        # Bulk upsert in batches
        collection = self.db.batch_views
        exported_count = 0
        
        for i in range(0, total_count, batch_size):
            batch = documents[i:i + batch_size]
            
            # Create bulk operations
            operations = []
            for doc in batch:
                # Define unique filter based on view_type
                if view_type == "genre_analytics":
                    filter_doc = {
                        "view_type": view_type,
                        "genre": doc.get("genre"),
                        "year": doc.get("year"),
                        "month": doc.get("month")
                    }
                elif view_type == "trending_scores":
                    filter_doc = {
                        "view_type": view_type,
                        "movie_id": doc.get("movie_id"),
                        "window": doc.get("window")
                    }
                elif view_type == "temporal_analysis":
                    filter_doc = {
                        "view_type": view_type,
                        "year": doc.get("year")
                    }
                else:
                    # Generic filter
                    filter_doc = {"view_type": view_type, "_id": doc.get("_id", str(i))}
                
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
        
        logger.info(f"Exported {exported_count}/{total_count} {view_type} documents to MongoDB")
        self.metrics.add_metric(f"{view_type}_exported", exported_count)
        
        return exported_count


class MongoExportJob:
    """
    Job to export all Gold layer data to MongoDB.
    """
    
    def __init__(self, spark, mongo_exporter: MongoDBExporter):
        self.spark = spark
        self.mongo_exporter = mongo_exporter
    
    @log_execution(logger, "mongo_export")
    def run(self, collections: List[str] = None):
        """
        Run MongoDB export.
        
        Args:
            collections: List of Gold collections to export
                       (default: all - genre_analytics, trending_scores, temporal_analysis)
        """
        if collections is None:
            collections = ["genre_analytics", "trending_scores", "temporal_analysis"]
        
        logger.info(f"Starting MongoDB export for collections: {collections}")
        
        # Create indexes
        self.mongo_exporter.create_indexes()
        
        # Export each collection
        for collection in collections:
            try:
                self._export_collection(collection)
            except Exception as e:
                logger.error(f"Failed to export {collection}: {str(e)}", exc_info=True)
                # Continue with other collections
                continue
        
        # Log final metrics
        self.mongo_exporter.metrics.log(logger)
        logger.info("MongoDB export completed successfully")
    
    def _export_collection(self, collection_name: str):
        """Export a single Gold collection to MongoDB."""
        logger.info(f"Exporting collection: {collection_name}")
        
        # Read from Gold layer
        gold_path = get_gold_path(collection_name, None).rstrip('/')
        
        try:
            df = self.spark.read.parquet(gold_path)
            count = df.count()
            
            if count == 0:
                logger.warning(f"No data found in {collection_name}")
                return
            
            logger.info(f"Read {count} records from {gold_path}")
            
            # Remove partition columns (they're internal to Gold layer)
            df = df.drop("partition_year", "partition_month")
            
            # Export to MongoDB
            exported = self.mongo_exporter.export_from_dataframe(
                df,
                view_type=collection_name
            )
            
            logger.info(f"Successfully exported {exported} records from {collection_name}")
            
        except Exception as e:
            logger.error(f"Failed to export {collection_name}: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for MongoDB export job."""
    parser = argparse.ArgumentParser(description="Export Gold Layer to MongoDB")
    parser.add_argument("--collections", type=str, 
                       default="genre_analytics,trending_scores,temporal_analysis",
                       help="Comma-separated list of collections to export")
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
    
    collections = [c.strip() for c in args.collections.split(',')]
    
    spark = None
    mongo_exporter = None
    
    try:
        # Create Spark session
        spark = get_spark_session("mongo_export")
        
        # Create MongoDB exporter
        mongo_exporter = MongoDBExporter(mongo_uri, args.database)
        
        # Run export
        job = MongoExportJob(spark, mongo_exporter)
        job.run(collections=collections)
        
    except Exception as e:
        logger.error(f"MongoDB export failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if mongo_exporter:
            mongo_exporter.close()
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
