"""
Integration Tests for Batch Layer Pipeline

Tests the complete end-to-end flow:
- Bronze ingestion from TMDB API
- Silver transformation and enrichment
- Gold aggregation
- MongoDB export

Note: These tests use REAL data from TMDB API (no mocks).
"""

import os
import sys
import unittest
from datetime import datetime
import time

# Add project paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark_jobs'))
from utils.spark_session import get_spark_session
from utils.s3_utils import get_bronze_path, get_silver_path, get_gold_path
from utils.logger import get_logger

logger = get_logger(__name__)


class TestBatchLayerIntegration(unittest.TestCase):
    """Integration tests for Batch Layer pipeline."""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = get_spark_session("integration_tests")
        cls.tmdb_api_key = os.getenv('TMDB_API_KEY')
        
        if not cls.tmdb_api_key:
            raise SkipTest("TMDB_API_KEY not set - skipping integration tests")
        
        logger.info("Starting Batch Layer integration tests")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session."""
        if hasattr(cls, 'spark') and cls.spark:
            cls.spark.stop()
        logger.info("Completed Batch Layer integration tests")
    
    def test_01_bronze_ingestion(self):
        """Test Bronze layer ingestion from TMDB API."""
        logger.info("Testing Bronze layer ingestion...")
        
        from bronze_ingest import BronzeIngestionJob
        
        # Run ingestion with minimal data
        job = BronzeIngestionJob(self.spark, self.tmdb_api_key)
        job.run(
            extraction_window_hours=1,
            categories=["popular"],
            pages_per_category=2  # Minimal for testing
        )
        
        # Verify Bronze data was created
        bronze_path = get_bronze_path("movies", None).rstrip('/')
        
        try:
            df = self.spark.read.parquet(bronze_path)
            count = df.count()
            
            self.assertGreater(count, 0, "Bronze layer should contain data")
            logger.info(f"✓ Bronze layer created: {count} movies")
            
            # Verify required columns
            required_columns = ['id', 'title', 'extraction_timestamp']
            for col in required_columns:
                self.assertIn(col, df.columns, f"Bronze data should have '{col}' column")
            
        except Exception as e:
            self.fail(f"Failed to read Bronze data: {str(e)}")
    
    def test_02_silver_transformation(self):
        """Test Silver layer transformation."""
        logger.info("Testing Silver layer transformation...")
        
        # First, ensure Bronze data exists (may need to run ingestion)
        bronze_path = get_bronze_path("movies", None).rstrip('/')
        
        try:
            bronze_df = self.spark.read.parquet(bronze_path)
            bronze_count = bronze_df.count()
            self.assertGreater(bronze_count, 0, "Bronze data should exist before Silver transformation")
        except:
            self.skipTest("Bronze data not found - run test_01_bronze_ingestion first")
        
        from silver_transform import SilverTransformationJob
        
        # Run transformation
        job = SilverTransformationJob(self.spark)
        job.run(lookback_hours=24)
        
        # Verify Silver data
        silver_path = get_silver_path("movies", None).rstrip('/')
        
        try:
            df = self.spark.read.parquet(silver_path)
            count = df.count()
            
            self.assertGreater(count, 0, "Silver layer should contain data")
            logger.info(f"✓ Silver layer created: {count} movies")
            
            # Verify enrichment columns
            expected_columns = ['movie_id', 'title', 'genres', 'quality_flag']
            for col in expected_columns:
                self.assertIn(col, df.columns, f"Silver data should have '{col}' column")
            
            # Verify no duplicate movie_ids
            movie_ids = df.select('movie_id').collect()
            movie_id_list = [row['movie_id'] for row in movie_ids]
            unique_count = len(set(movie_id_list))
            
            self.assertEqual(count, unique_count, "Silver layer should not have duplicate movie_ids")
            logger.info("✓ No duplicate movie_ids found")
            
        except Exception as e:
            self.fail(f"Failed to read Silver data: {str(e)}")
    
    def test_03_gold_aggregation(self):
        """Test Gold layer aggregation."""
        logger.info("Testing Gold layer aggregation...")
        
        # Ensure Silver data exists
        silver_path = get_silver_path("movies", None).rstrip('/')
        
        try:
            silver_df = self.spark.read.parquet(silver_path)
            silver_count = silver_df.count()
            self.assertGreater(silver_count, 0, "Silver data should exist before Gold aggregation")
        except:
            self.skipTest("Silver data not found - run test_02_silver_transformation first")
        
        from gold_aggregate import GoldAggregationJob
        
        # Run aggregation
        job = GoldAggregationJob(self.spark)
        job.run(lookback_days=30)
        
        # Verify Gold data - genre analytics
        gold_path = get_gold_path("genre_analytics", None).rstrip('/')
        
        try:
            df = self.spark.read.parquet(gold_path)
            count = df.count()
            
            self.assertGreater(count, 0, "Gold layer should contain genre analytics")
            logger.info(f"✓ Gold layer created: {count} genre analytics records")
            
            # Verify aggregation columns
            expected_columns = ['genre', 'year', 'month', 'total_movies', 'avg_rating']
            for col in expected_columns:
                self.assertIn(col, df.columns, f"Gold data should have '{col}' column")
            
            # Verify aggregation correctness (total_movies should be > 0)
            df_with_counts = df.filter(df.total_movies > 0)
            self.assertGreater(df_with_counts.count(), 0, "Should have genres with movie counts")
            
        except Exception as e:
            self.fail(f"Failed to read Gold data: {str(e)}")
    
    def test_04_mongodb_export(self):
        """Test MongoDB export."""
        logger.info("Testing MongoDB export...")
        
        from pymongo import MongoClient
        from export_to_mongo import MongoDBExporter, MongoExportJob
        
        # Get MongoDB connection
        mongo_uri = os.getenv('MONGODB_CONNECTION_STRING', 
                             'mongodb://admin:password@mongodb:27017/moviedb?authSource=admin')
        
        try:
            # Ensure Gold data exists
            gold_path = get_gold_path("genre_analytics", None).rstrip('/')
            gold_df = self.spark.read.parquet(gold_path)
            gold_count = gold_df.count()
            self.assertGreater(gold_count, 0, "Gold data should exist before MongoDB export")
        except:
            self.skipTest("Gold data not found - run test_03_gold_aggregation first")
        
        try:
            # Create exporter and run export
            exporter = MongoDBExporter(mongo_uri, database="moviedb")
            job = MongoExportJob(self.spark, exporter)
            
            job.run(collections=["genre_analytics"])
            
            # Verify data in MongoDB
            client = MongoClient(mongo_uri)
            db = client.moviedb
            collection = db.batch_views
            
            # Count documents
            count = collection.count_documents({"view_type": "genre_analytics"})
            self.assertGreater(count, 0, "MongoDB should contain batch_views documents")
            logger.info(f"✓ MongoDB export successful: {count} documents")
            
            # Verify document structure
            sample_doc = collection.find_one({"view_type": "genre_analytics"})
            self.assertIsNotNone(sample_doc, "Should have at least one document")
            self.assertIn("view_type", sample_doc, "Document should have view_type field")
            self.assertIn("genre", sample_doc, "Document should have genre field")
            
            # Clean up
            exporter.close()
            client.close()
            
        except Exception as e:
            self.fail(f"MongoDB export test failed: {str(e)}")
    
    def test_05_end_to_end_data_flow(self):
        """Test complete end-to-end data flow."""
        logger.info("Testing end-to-end data flow...")
        
        from pymongo import MongoClient
        
        # This test verifies the complete pipeline by checking:
        # 1. Bronze has raw data
        # 2. Silver has cleaned/enriched data  
        # 3. Gold has aggregations
        # 4. MongoDB has serving-ready data
        
        layers = []
        
        # Check Bronze
        try:
            bronze_path = get_bronze_path("movies", None).rstrip('/')
            bronze_df = self.spark.read.parquet(bronze_path)
            bronze_count = bronze_df.count()
            layers.append(f"Bronze: {bronze_count} records")
        except:
            layers.append("Bronze: NOT FOUND")
        
        # Check Silver
        try:
            silver_path = get_silver_path("movies", None).rstrip('/')
            silver_df = self.spark.read.parquet(silver_path)
            silver_count = silver_df.count()
            layers.append(f"Silver: {silver_count} records")
        except:
            layers.append("Silver: NOT FOUND")
        
        # Check Gold
        try:
            gold_path = get_gold_path("genre_analytics", None).rstrip('/')
            gold_df = self.spark.read.parquet(gold_path)
            gold_count = gold_df.count()
            layers.append(f"Gold: {gold_count} records")
        except:
            layers.append("Gold: NOT FOUND")
        
        # Check MongoDB
        try:
            mongo_uri = os.getenv('MONGODB_CONNECTION_STRING', 
                                 'mongodb://admin:password@mongodb:27017/moviedb?authSource=admin')
            client = MongoClient(mongo_uri)
            db = client.moviedb
            mongo_count = db.batch_views.count_documents({})
            layers.append(f"MongoDB: {mongo_count} documents")
            client.close()
        except:
            layers.append("MongoDB: NOT FOUND")
        
        # Log pipeline status
        logger.info("End-to-end pipeline status:")
        for layer in layers:
            logger.info(f"  {layer}")
        
        # All layers should have data
        self.assertNotIn("NOT FOUND", "\n".join(layers), 
                        "All pipeline layers should contain data")
        
        logger.info("✓ End-to-end data flow validated")


def run_integration_tests():
    """Run all integration tests."""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestBatchLayerIntegration)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)
