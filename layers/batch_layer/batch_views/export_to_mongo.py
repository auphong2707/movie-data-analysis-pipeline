"""
MongoDB Export Module for Batch Views.

This module exports Gold layer aggregations to MongoDB collections
for serving layer consumption with proper indexing and upsert operations.
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_json, struct, lit, coalesce, concat, when,
    collect_list, collect_set, count, max as spark_max
)
import yaml
import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError, ConnectionFailure
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoDBExporter:
    """Exports Gold layer data to MongoDB for batch views serving."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize MongoDB exporter.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.hdfs_config = config.get("hdfs", {})
        self.mongo_config = config.get("mongodb", {})
        
        # Paths
        self.gold_path = self.hdfs_config["paths"]["gold"]
        
        # MongoDB connection
        self.mongo_uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
        self.database_name = self.mongo_config.get("database", "tmdb_analytics")
        self.collection_name = "batch_views"
        
        # Performance settings
        self.batch_size = self.mongo_config.get("batch_size", 1000)
        self.upsert_enabled = self.mongo_config.get("upsert_enabled", True)
        
        # Initialize MongoDB connection
        self._initialize_mongodb()
    
    def _initialize_mongodb(self):
        """Initialize MongoDB connection and create indexes."""
        try:
            self.mongo_client = MongoClient(self.mongo_uri)
            self.database = self.mongo_client[self.database_name]
            self.collection = self.database[self.collection_name]
            
            # Test connection
            self.mongo_client.admin.command('ping')
            logger.info(f"Connected to MongoDB: {self.database_name}.{self.collection_name}")
            
            # Create indexes
            self._create_indexes()
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _create_indexes(self):
        """Create required indexes for batch views collection."""
        try:
            # Index on movie_id and view_type for efficient lookups
            self.collection.create_index([
                ("movie_id", ASCENDING),
                ("view_type", ASCENDING)
            ], unique=True, background=True)
            
            # Index on view_type for filtering by view type
            self.collection.create_index([("view_type", ASCENDING)], background=True)
            
            # Index on computed_at for time-based queries
            self.collection.create_index([("computed_at", DESCENDING)], background=True)
            
            # Compound index for genre analytics queries
            self.collection.create_index([
                ("view_type", ASCENDING),
                ("data.genre", ASCENDING),
                ("data.year", ASCENDING)
            ], background=True)
            
            # Index for trending queries
            self.collection.create_index([
                ("view_type", ASCENDING),
                ("data.window", ASCENDING),
                ("data.computed_date", DESCENDING)
            ], background=True)
            
            logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating MongoDB indexes: {e}")
    
    def export_all_batch_views(
        self, 
        batch_id: str,
        export_timestamp: datetime = None
    ) -> Dict[str, int]:
        """
        Export all Gold layer views to MongoDB.
        
        Args:
            batch_id: Batch identifier
            export_timestamp: Timestamp for export (defaults to now)
            
        Returns:
            Dict[str, int]: Export results by view type
        """
        if not export_timestamp:
            export_timestamp = datetime.utcnow()
        
        logger.info(f"Starting batch views export for batch {batch_id}")
        
        export_results = {}
        
        try:
            # Export genre analytics
            genre_count = self._export_genre_analytics(batch_id, export_timestamp)
            if genre_count > 0:
                export_results["genre_analytics"] = genre_count
            
            # Export trending scores
            trending_count = self._export_trending_scores(batch_id, export_timestamp)
            if trending_count > 0:
                export_results["trending"] = trending_count
            
            # Export temporal analysis
            temporal_count = self._export_temporal_analysis(batch_id, export_timestamp)
            if temporal_count > 0:
                export_results["temporal"] = temporal_count
            
            # Export actor networks
            networks_count = self._export_actor_networks(batch_id, export_timestamp)
            if networks_count > 0:
                export_results["actor_networks"] = networks_count
            
            logger.info(f"Batch views export completed: {export_results}")
            return export_results
            
        except Exception as e:
            logger.error(f"Error in batch views export: {e}")
            raise
    
    def _export_genre_analytics(self, batch_id: str, export_timestamp: datetime) -> int:
        """
        Export genre analytics to MongoDB.
        
        Args:
            batch_id: Batch identifier
            export_timestamp: Export timestamp
            
        Returns:
            int: Number of records exported
        """
        logger.info("Exporting genre analytics to MongoDB")
        
        try:
            # Load genre analytics from Gold layer
            genre_path = f"{self.gold_path}/genre_analytics"
            
            genre_df = (self.spark.read
                       .parquet(genre_path)
                       .withColumn("view_type", lit("genre_analytics"))
                       .withColumn("batch_run_id", lit(batch_id))
                       .withColumn("computed_at", lit(export_timestamp))
                       .withColumn("schema_version", lit("1.0")))
            
            # Transform to MongoDB document format
            mongo_docs_df = (genre_df
                           .withColumn("data", to_json(struct(
                               col("genre"),
                               col("year"),
                               col("month"),
                               col("total_movies"),
                               col("avg_rating"),
                               col("total_revenue"),
                               col("avg_budget"),
                               col("avg_sentiment"),
                               col("top_movies"),
                               col("top_directors")
                           )))
                           .select(
                               col("genre").alias("movie_id"),  # Use genre as identifier for genre analytics
                               col("view_type"),
                               col("data"),
                               col("computed_at"),
                               col("batch_run_id"),
                               col("schema_version")
                           ))
            
            # Export to MongoDB
            return self._write_to_mongodb(mongo_docs_df, "genre_analytics")
            
        except Exception as e:
            logger.error(f"Error exporting genre analytics: {e}")
            return 0
    
    def _export_trending_scores(self, batch_id: str, export_timestamp: datetime) -> int:
        """
        Export trending scores to MongoDB.
        
        Args:
            batch_id: Batch identifier
            export_timestamp: Export timestamp
            
        Returns:
            int: Number of records exported
        """
        logger.info("Exporting trending scores to MongoDB")
        
        try:
            trending_path = f"{self.gold_path}/trending_scores"
            
            trending_df = (self.spark.read
                          .parquet(trending_path)
                          .withColumn("view_type", lit("trending"))
                          .withColumn("batch_run_id", lit(batch_id))
                          .withColumn("computed_at", lit(export_timestamp))
                          .withColumn("schema_version", lit("1.0")))
            
            # Transform to MongoDB document format
            mongo_docs_df = (trending_df
                           .withColumn("data", to_json(struct(
                               col("window"),
                               col("trend_score"),
                               col("velocity"),
                               col("popularity_change"),
                               col("rating_momentum"),
                               col("computed_date")
                           )))
                           .select(
                               col("movie_id"),
                               col("view_type"),
                               col("data"),
                               col("computed_at"),
                               col("batch_run_id"),
                               col("schema_version")
                           ))
            
            return self._write_to_mongodb(mongo_docs_df, "trending")
            
        except Exception as e:
            logger.error(f"Error exporting trending scores: {e}")
            return 0
    
    def _export_temporal_analysis(self, batch_id: str, export_timestamp: datetime) -> int:
        """
        Export temporal analysis to MongoDB.
        
        Args:
            batch_id: Batch identifier
            export_timestamp: Export timestamp
            
        Returns:
            int: Number of records exported
        """
        logger.info("Exporting temporal analysis to MongoDB")
        
        try:
            temporal_path = f"{self.gold_path}/temporal_analysis"
            
            temporal_df = (self.spark.read
                          .parquet(temporal_path)
                          .withColumn("view_type", lit("temporal"))
                          .withColumn("batch_run_id", lit(batch_id))
                          .withColumn("computed_at", lit(export_timestamp))
                          .withColumn("schema_version", lit("1.0")))
            
            # Transform to MongoDB document format
            mongo_docs_df = (temporal_df
                           .withColumn("data", to_json(struct(
                               col("metric_type"),
                               col("year"),
                               col("month"),
                               col("genre"),
                               col("current_value"),
                               col("previous_year_value"),
                               col("yoy_change_percent"),
                               col("yoy_change_absolute"),
                               col("trend_direction")
                           )))
                           .withColumn("temporal_id", concat(
                               col("metric_type"), lit("_"),
                               col("year"), lit("_"),
                               coalesce(col("genre"), lit("all"))
                           ))
                           .select(
                               col("temporal_id").alias("movie_id"),
                               col("view_type"),
                               col("data"),
                               col("computed_at"),
                               col("batch_run_id"),
                               col("schema_version")
                           ))
            
            return self._write_to_mongodb(mongo_docs_df, "temporal")
            
        except Exception as e:
            logger.error(f"Error exporting temporal analysis: {e}")
            return 0
    
    def _export_actor_networks(self, batch_id: str, export_timestamp: datetime) -> int:
        """
        Export actor networks to MongoDB.
        
        Args:
            batch_id: Batch identifier
            export_timestamp: Export timestamp
            
        Returns:
            int: Number of records exported
        """
        logger.info("Exporting actor networks to MongoDB")
        
        try:
            networks_path = f"{self.gold_path}/actor_networks"
            
            networks_df = (self.spark.read
                          .parquet(networks_path)
                          .withColumn("view_type", lit("actor_networks"))
                          .withColumn("batch_run_id", lit(batch_id))
                          .withColumn("computed_at", lit(export_timestamp))
                          .withColumn("schema_version", lit("1.0")))
            
            # Group collaborations by actor for efficient storage
            actor_collaborations = (networks_df
                                  .groupBy("actor_id", "actor_name")
                                  .agg(
                                      collect_list(struct(
                                          col("collaborator_id"),
                                          col("collaborator_name"),
                                          col("collaboration_count"),
                                          col("collaboration_score"),
                                          col("recent_collaboration_date"),
                                          col("shared_movies")
                                      )).alias("collaborations"),
                                      spark_max("network_centrality").alias("network_centrality"),
                                      spark_max("snapshot_month").alias("snapshot_month")
                                  ))
            
            # Transform to MongoDB document format
            mongo_docs_df = (actor_collaborations
                           .withColumn("data", to_json(struct(
                               col("actor_name"),
                               col("collaborations"),
                               col("network_centrality"),
                               col("snapshot_month")
                           )))
                           .withColumn("view_type", lit("actor_networks"))
                           .withColumn("batch_run_id", lit(batch_id))
                           .withColumn("computed_at", lit(export_timestamp))
                           .withColumn("schema_version", lit("1.0"))
                           .select(
                               col("actor_id").alias("movie_id"),
                               col("view_type"),
                               col("data"),
                               col("computed_at"),
                               col("batch_run_id"),
                               col("schema_version")
                           ))
            
            return self._write_to_mongodb(mongo_docs_df, "actor_networks")
            
        except Exception as e:
            logger.error(f"Error exporting actor networks: {e}")
            return 0
    
    def _write_to_mongodb(self, df: DataFrame, view_type: str) -> int:
        """
        Write DataFrame to MongoDB with bulk upsert operations.
        
        Args:
            df: DataFrame to write
            view_type: Type of view being written
            
        Returns:
            int: Number of records written
        """
        logger.info(f"Writing {view_type} data to MongoDB")
        
        try:
            # Collect DataFrame to Python objects
            records = df.collect()
            
            if not records:
                logger.warning(f"No {view_type} records to write")
                return 0
            
            # Prepare bulk operations
            bulk_operations = []
            
            for record in records:
                # Convert Spark Row to dictionary
                doc = record.asDict()
                
                # Parse JSON data field
                if doc.get("data"):
                    try:
                        doc["data"] = json.loads(doc["data"])
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse JSON data for record: {doc.get('movie_id')}")
                        continue
                
                # Prepare upsert operation
                if self.upsert_enabled:
                    bulk_operations.append(
                        pymongo.UpdateOne(
                            {
                                "movie_id": doc["movie_id"],
                                "view_type": doc["view_type"]
                            },
                            {"$set": doc},
                            upsert=True
                        )
                    )
                else:
                    bulk_operations.append(
                        pymongo.InsertOne(doc)
                    )
                
                # Execute bulk operations in batches
                if len(bulk_operations) >= self.batch_size:
                    self._execute_bulk_operations(bulk_operations, view_type)
                    bulk_operations = []
            
            # Execute remaining operations
            if bulk_operations:
                self._execute_bulk_operations(bulk_operations, view_type)
            
            logger.info(f"Successfully wrote {len(records)} {view_type} records to MongoDB")
            return len(records)
            
        except Exception as e:
            logger.error(f"Error writing {view_type} to MongoDB: {e}")
            raise
    
    def _execute_bulk_operations(self, operations: List, view_type: str):
        """
        Execute bulk operations with error handling.
        
        Args:
            operations: List of bulk operations
            view_type: Type of view being processed
        """
        try:
            result = self.collection.bulk_write(operations, ordered=False)
            
            logger.debug(f"{view_type} bulk write results: "
                        f"inserted={result.inserted_count}, "
                        f"modified={result.modified_count}, "
                        f"upserted={result.upserted_count}")
            
        except BulkWriteError as bwe:
            logger.error(f"Bulk write error for {view_type}: {bwe.details}")
            # Continue processing despite errors
        except Exception as e:
            logger.error(f"Unexpected error in bulk write for {view_type}: {e}")
            raise
    
    def cleanup_old_batch_views(self, retention_days: int = 30) -> int:
        """
        Clean up old batch views based on retention policy.
        
        Args:
            retention_days: Number of days to retain data
            
        Returns:
            int: Number of documents deleted
        """
        logger.info(f"Cleaning up batch views older than {retention_days} days")
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
            
            result = self.collection.delete_many({
                "computed_at": {"$lt": cutoff_date}
            })
            
            logger.info(f"Deleted {result.deleted_count} old batch view documents")
            return result.deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old batch views: {e}")
            return 0
    
    def validate_export_quality(self, view_type: str, expected_count: int) -> Dict[str, Any]:
        """
        Validate the quality of exported data.
        
        Args:
            view_type: Type of view to validate
            expected_count: Expected number of records
            
        Returns:
            Dict[str, Any]: Validation results
        """
        try:
            # Count documents in MongoDB
            actual_count = self.collection.count_documents({"view_type": view_type})
            
            # Calculate completeness
            completeness = actual_count / expected_count if expected_count > 0 else 0
            
            # Sample validation
            sample_doc = self.collection.find_one({"view_type": view_type})
            has_valid_structure = bool(
                sample_doc and 
                "data" in sample_doc and 
                "computed_at" in sample_doc and
                "batch_run_id" in sample_doc
            )
            
            return {
                "view_type": view_type,
                "expected_count": expected_count,
                "actual_count": actual_count,
                "completeness": completeness,
                "has_valid_structure": has_valid_structure,
                "is_valid": completeness >= 0.95 and has_valid_structure
            }
            
        except Exception as e:
            logger.error(f"Error validating export quality for {view_type}: {e}")
            return {
                "view_type": view_type,
                "is_valid": False,
                "error": str(e)
            }
    
    def close(self):
        """Close MongoDB connection."""
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()
            logger.info("MongoDB connection closed")


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="MongoDB Export for Batch Views")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--batch-id", required=True, help="Batch ID")
    parser.add_argument("--cleanup", action="store_true", help="Run cleanup of old data")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MongoDB_Batch_Views_Export") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize exporter
        exporter = MongoDBExporter(spark, config)
        
        # Run export
        export_results = exporter.export_all_batch_views(args.batch_id)
        
        # Validate exports
        for view_type, count in export_results.items():
            validation = exporter.validate_export_quality(view_type, count)
            logger.info(f"Export validation for {view_type}: {validation}")
        
        # Cleanup if requested
        if args.cleanup:
            deleted_count = exporter.cleanup_old_batch_views()
            logger.info(f"Cleanup completed: {deleted_count} documents deleted")
        
        logger.info(f"MongoDB export completed successfully: {export_results}")
        
    finally:
        if 'exporter' in locals():
            exporter.close()
        spark.stop()