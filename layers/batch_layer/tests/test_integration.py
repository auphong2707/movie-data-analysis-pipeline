"""
Integration tests for the full batch pipeline.

Runs Bronze → Silver → Gold → MongoDB pipeline with real TMDB API.
Gated by RUN_INTEGRATION environment variable.

Usage:
    RUN_INTEGRATION=1 pytest test_integration.py -v
"""

import pytest
import os
import sys
from datetime import datetime

# Skip if RUN_INTEGRATION not set
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION") != "1",
    reason="Integration tests require RUN_INTEGRATION=1"
)

sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
from config.config import config
from layers.batch_layer.master_dataset.ingestion import TMDBBatchIngestion


@pytest.fixture(scope="module")
def execution_time():
    """Get execution time for test run."""
    return datetime.now()


def test_bronze_ingestion_integration(execution_time):
    """Test Bronze layer ingestion with real TMDB API."""
    if not config.tmdb_api_key:
        pytest.skip("TMDB_API_KEY not configured")
    
    ingestion = TMDBBatchIngestion(config.tmdb_api_key)
    
    # Run small extraction (1 page only for test)
    results = ingestion.run_full_extraction(
        extraction_time=execution_time,
        pages=1,  # Small for testing
        categories=["popular"],
        extract_details=False,
        extract_reviews=False
    )
    
    # Verify results
    assert results["movies"] > 0, "Should extract at least some movies"
    print(f"\nIntegration test extracted {results['movies']} movies")


def test_silver_transformation_integration(execution_time):
    """Test Silver layer transformation."""
    from layers.batch_layer.spark_jobs.silver.run import SilverTransformation
    
    # Run transformation
    transformation = SilverTransformation(execution_time)
    
    # Test movie transformation only (reviews may not exist)
    success = transformation.transform_movies()
    
    assert success, "Silver transformation should succeed"
    print("\nSilver transformation completed successfully")


def test_gold_aggregation_integration(execution_time):
    """Test Gold layer aggregations."""
    from layers.batch_layer.spark_jobs.gold.run import GoldAggregation
    
    # Run aggregation
    aggregation = GoldAggregation(execution_time)
    
    # Test genre analytics only
    success = aggregation.aggregate_genre_analytics()
    
    assert success, "Gold aggregation should succeed"
    print("\nGold aggregation completed successfully")


def test_mongodb_export_integration(execution_time):
    """Test MongoDB export."""
    from layers.batch_layer.batch_views.export_to_mongo import MongoDBExporter
    
    # Run export
    exporter = MongoDBExporter(execution_time)
    
    # Test genre analytics export only
    count = exporter.export_genre_analytics()
    
    assert count >= 0, "Export should complete without error"
    print(f"\nMongoDB export completed: {count} documents")


def test_full_pipeline_integration(execution_time):
    """Test complete end-to-end pipeline."""
    print("\n" + "="*60)
    print("FULL PIPELINE INTEGRATION TEST")
    print("="*60)
    
    # Step 1: Bronze ingestion
    print("\n[1/4] Bronze ingestion...")
    ingestion = TMDBBatchIngestion(config.tmdb_api_key)
    bronze_results = ingestion.run_full_extraction(
        extraction_time=execution_time,
        pages=1,
        categories=["popular"],
        extract_details=True,
        extract_reviews=True
    )
    print(f"✓ Bronze: {bronze_results}")
    
    # Step 2: Silver transformation
    print("\n[2/4] Silver transformation...")
    from layers.batch_layer.spark_jobs.silver.run import SilverTransformation
    silver = SilverTransformation(execution_time)
    silver_success = silver.run()
    assert silver_success, "Silver transformation failed"
    print("✓ Silver: Complete")
    
    # Step 3: Gold aggregation
    print("\n[3/4] Gold aggregation...")
    from layers.batch_layer.spark_jobs.gold.run import GoldAggregation
    gold = GoldAggregation(execution_time)
    gold_success = gold.run()
    assert gold_success, "Gold aggregation failed"
    print("✓ Gold: Complete")
    
    # Step 4: MongoDB export
    print("\n[4/4] MongoDB export...")
    from layers.batch_layer.batch_views.export_to_mongo import MongoDBExporter
    mongo = MongoDBExporter(execution_time)
    mongo_success = mongo.run()
    assert mongo_success, "MongoDB export failed"
    print("✓ MongoDB: Complete")
    
    print("\n" + "="*60)
    print("PIPELINE TEST PASSED ✓")
    print("="*60)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
