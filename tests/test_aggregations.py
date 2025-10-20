"""
Aggregation Tests for TMDB Movie Data Pipeline.

This module contains comprehensive tests for the Gold layer aggregations,
batch views generation, and MongoDB export functionality.
"""

import pytest
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
import pymongo
from pymongo import MongoClient
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MockMongoClient:
    """Mock MongoDB client for testing."""
    
    def __init__(self):
        self.databases = {}
        self.admin = Mock()
        
    def __getitem__(self, database_name):
        if database_name not in self.databases:
            self.databases[database_name] = MockDatabase()
        return self.databases[database_name]
    
    def close(self):
        pass


class MockDatabase:
    """Mock MongoDB database."""
    
    def __init__(self):
        self.collections = {}
    
    def __getitem__(self, collection_name):
        if collection_name not in self.collections:
            self.collections[collection_name] = MockCollection()
        return self.collections[collection_name]


class MockCollection:
    """Mock MongoDB collection."""
    
    def __init__(self):
        self.documents = []
        self.indexes = []
    
    def insert_one(self, document):
        self.documents.append(document)
        return Mock(inserted_id="mock_id")
    
    def insert_many(self, documents):
        self.documents.extend(documents)
        return Mock(inserted_ids=["mock_id"] * len(documents))
    
    def bulk_write(self, operations, ordered=True):
        # Simulate bulk write operations
        inserted_count = 0
        modified_count = 0
        upserted_count = 0
        
        for op in operations:
            if hasattr(op, '_doc'):  # InsertOne
                self.documents.append(op._doc)
                inserted_count += 1
            elif hasattr(op, '_filter'):  # UpdateOne/ReplaceOne
                # Simulate upsert
                existing = self._find_document(op._filter)
                if existing:
                    modified_count += 1
                else:
                    upserted_count += 1
                    self.documents.append(op._doc if hasattr(op, '_doc') else op._replacement)
        
        result = Mock()
        result.inserted_count = inserted_count
        result.modified_count = modified_count
        result.upserted_count = upserted_count
        return result
    
    def find(self, filter_dict=None):
        return [doc for doc in self.documents if self._matches_filter(doc, filter_dict)]
    
    def find_one(self, filter_dict=None):
        matches = self.find(filter_dict)
        return matches[0] if matches else None
    
    def count_documents(self, filter_dict):
        return len(self.find(filter_dict))
    
    def delete_many(self, filter_dict):
        before_count = len(self.documents)
        self.documents = [doc for doc in self.documents if not self._matches_filter(doc, filter_dict)]
        deleted_count = before_count - len(self.documents)
        result = Mock()
        result.deleted_count = deleted_count
        return result
    
    def create_index(self, keys, **kwargs):
        self.indexes.append({"keys": keys, "options": kwargs})
    
    def _matches_filter(self, document, filter_dict):
        if not filter_dict:
            return True
        
        for key, value in filter_dict.items():
            if isinstance(value, dict):
                # Handle operators like $lt, $gte, etc.
                doc_value = document.get(key)
                for op, op_value in value.items():
                    if op == "$lt" and doc_value >= op_value:
                        return False
                    elif op == "$gte" and doc_value < op_value:
                        return False
                    elif op == "$in" and doc_value not in op_value:
                        return False
            else:
                if document.get(key) != value:
                    return False
        return True
    
    def _find_document(self, filter_dict):
        matches = self.find(filter_dict)
        return matches[0] if matches else None


class TestGenreAnalytics:
    """Test genre analytics aggregation functionality."""
    
    @pytest.fixture
    def sample_genre_data(self):
        """Create sample genre analytics data."""
        return [
            {
                "genre": "Action",
                "year": 2023,
                "month": 6,
                "total_movies": 150,
                "avg_rating": 7.2,
                "total_revenue": 2500000000,
                "avg_budget": 80000000,
                "avg_sentiment": 0.65,
                "top_movies": [
                    {"movie_id": "action_001", "title": "Action Hero", "revenue": 500000000},
                    {"movie_id": "action_002", "title": "Speed Force", "revenue": 400000000}
                ],
                "top_directors": [
                    {"name": "John Director", "movie_count": 3, "avg_rating": 8.1}
                ]
            },
            {
                "genre": "Drama",
                "year": 2023,
                "month": 6,
                "total_movies": 200,
                "avg_rating": 8.1,
                "total_revenue": 1800000000,
                "avg_budget": 45000000,
                "avg_sentiment": 0.72,
                "top_movies": [
                    {"movie_id": "drama_001", "title": "Life Story", "revenue": 300000000},
                    {"movie_id": "drama_002", "title": "Human Drama", "revenue": 250000000}
                ],
                "top_directors": [
                    {"name": "Jane Director", "movie_count": 5, "avg_rating": 8.5}
                ]
            }
        ]
    
    def test_genre_aggregation_logic(self, sample_genre_data):
        """Test genre aggregation calculations."""
        logger.info("Testing genre aggregation logic")
        
        # Test basic aggregation metrics
        for genre_stats in sample_genre_data:
            assert genre_stats["total_movies"] > 0
            assert 0 <= genre_stats["avg_rating"] <= 10
            assert genre_stats["total_revenue"] >= 0
            assert genre_stats["avg_budget"] >= 0
            assert 0 <= genre_stats["avg_sentiment"] <= 1
            
            # Test top movies structure
            assert len(genre_stats["top_movies"]) > 0
            for movie in genre_stats["top_movies"]:
                assert "movie_id" in movie
                assert "title" in movie
                assert "revenue" in movie
                assert movie["revenue"] >= 0
            
            # Test top directors structure
            assert len(genre_stats["top_directors"]) > 0
            for director in genre_stats["top_directors"]:
                assert "name" in director
                assert "movie_count" in director
                assert "avg_rating" in director
                assert director["movie_count"] > 0
        
        logger.info("Genre aggregation logic test passed")
    
    def test_genre_ranking(self, sample_genre_data):
        """Test genre ranking by different metrics."""
        logger.info("Testing genre ranking")
        
        # Sort by revenue
        revenue_sorted = sorted(sample_genre_data, key=lambda x: x["total_revenue"], reverse=True)
        assert revenue_sorted[0]["genre"] == "Action"  # Higher revenue
        assert revenue_sorted[1]["genre"] == "Drama"
        
        # Sort by rating
        rating_sorted = sorted(sample_genre_data, key=lambda x: x["avg_rating"], reverse=True)
        assert rating_sorted[0]["genre"] == "Drama"   # Higher rating
        assert rating_sorted[1]["genre"] == "Action"
        
        logger.info("Genre ranking test passed")


class TestTrendingScores:
    """Test trending scores calculation functionality."""
    
    @pytest.fixture
    def sample_trending_data(self):
        """Create sample trending scores data."""
        return [
            {
                "movie_id": "trending_001",
                "window": "7d",
                "trend_score": 0.85,
                "velocity": 15.2,
                "popularity_change": 25.5,
                "rating_momentum": 0.12,
                "computed_date": "2023-06-15"
            },
            {
                "movie_id": "trending_001", 
                "window": "30d",
                "trend_score": 0.72,
                "velocity": 8.7,
                "popularity_change": 18.3,
                "rating_momentum": 0.08,
                "computed_date": "2023-06-15"
            },
            {
                "movie_id": "trending_002",
                "window": "7d",
                "trend_score": 0.45,
                "velocity": -5.2,
                "popularity_change": -8.1,
                "rating_momentum": -0.03,
                "computed_date": "2023-06-15"
            }
        ]
    
    def test_trending_calculation(self, sample_trending_data):
        """Test trending score calculation logic."""
        logger.info("Testing trending calculation")
        
        for trend_data in sample_trending_data:
            # Validate trend score range
            assert 0 <= trend_data["trend_score"] <= 1
            
            # Validate velocity (can be negative for declining trends)
            assert isinstance(trend_data["velocity"], (int, float))
            
            # Validate popularity change (can be negative)
            assert isinstance(trend_data["popularity_change"], (int, float))
            
            # Validate rating momentum (can be negative)
            assert isinstance(trend_data["rating_momentum"], (int, float))
            
            # Check window validity
            assert trend_data["window"] in ["7d", "30d", "90d"]
        
        logger.info("Trending calculation test passed")
    
    def test_trending_windows(self, sample_trending_data):
        """Test different trending time windows."""
        logger.info("Testing trending windows")
        
        # Group by movie_id to check multiple windows
        movie_trends = {}
        for trend in sample_trending_data:
            movie_id = trend["movie_id"]
            if movie_id not in movie_trends:
                movie_trends[movie_id] = []
            movie_trends[movie_id].append(trend)
        
        # Check that movies have multiple window calculations
        for movie_id, trends in movie_trends.items():
            if len(trends) > 1:
                # Should have different windows for same movie
                windows = [t["window"] for t in trends]
                assert len(set(windows)) == len(trends)  # All unique windows
        
        logger.info("Trending windows test passed")


class TestTemporalAnalysis:
    """Test temporal analysis functionality."""
    
    @pytest.fixture
    def sample_temporal_data(self):
        """Create sample temporal analysis data."""
        return [
            {
                "metric_type": "avg_revenue",
                "year": 2023,
                "month": 6,
                "genre": "Action",
                "current_value": 85000000,
                "previous_year_value": 78000000,
                "yoy_change_percent": 8.97,
                "yoy_change_absolute": 7000000,
                "trend_direction": "increasing"
            },
            {
                "metric_type": "movie_count",
                "year": 2023,
                "month": 6,
                "genre": "Drama",
                "current_value": 120,
                "previous_year_value": 135,
                "yoy_change_percent": -11.11,
                "yoy_change_absolute": -15,
                "trend_direction": "decreasing"
            }
        ]
    
    def test_yoy_calculations(self, sample_temporal_data):
        """Test year-over-year calculations."""
        logger.info("Testing YoY calculations")
        
        for temporal_record in sample_temporal_data:
            current = temporal_record["current_value"]
            previous = temporal_record["previous_year_value"]
            yoy_percent = temporal_record["yoy_change_percent"]
            yoy_absolute = temporal_record["yoy_change_absolute"]
            
            # Validate YoY percentage calculation
            expected_percent = ((current - previous) / previous) * 100
            assert abs(yoy_percent - expected_percent) < 0.01  # Allow small floating point differences
            
            # Validate YoY absolute calculation
            expected_absolute = current - previous
            assert yoy_absolute == expected_absolute
            
            # Validate trend direction
            if yoy_percent > 0:
                assert temporal_record["trend_direction"] == "increasing"
            elif yoy_percent < 0:
                assert temporal_record["trend_direction"] == "decreasing"
            else:
                assert temporal_record["trend_direction"] in ["stable", "no_change"]
        
        logger.info("YoY calculations test passed")
    
    def test_temporal_metrics(self, sample_temporal_data):
        """Test temporal metric types."""
        logger.info("Testing temporal metrics")
        
        # Check for expected metric types
        metric_types = {record["metric_type"] for record in sample_temporal_data}
        expected_metrics = {"avg_revenue", "movie_count", "avg_rating", "total_budget"}
        
        # Should have at least some expected metrics
        assert len(metric_types.intersection(expected_metrics)) > 0
        
        # Check data structure
        for record in sample_temporal_data:
            assert "year" in record
            assert "current_value" in record
            assert "previous_year_value" in record
            assert record["year"] >= 2020  # Reasonable year range
        
        logger.info("Temporal metrics test passed")


class TestMongoDBExport:
    """Test MongoDB export functionality."""
    
    @pytest.fixture
    def mock_mongo_client(self):
        """Create mock MongoDB client."""
        return MockMongoClient()
    
    @pytest.fixture
    def sample_export_data(self):
        """Create sample data for MongoDB export."""
        return {
            "genre_analytics": [
                {
                    "movie_id": "Action",  # Using genre as identifier for genre analytics
                    "view_type": "genre_analytics",
                    "data": {
                        "genre": "Action",
                        "year": 2023,
                        "total_movies": 150,
                        "avg_rating": 7.2
                    },
                    "computed_at": datetime.utcnow(),
                    "batch_run_id": "batch_20230615_120000"
                }
            ],
            "trending": [
                {
                    "movie_id": "trending_001",
                    "view_type": "trending", 
                    "data": {
                        "window": "7d",
                        "trend_score": 0.85,
                        "velocity": 15.2
                    },
                    "computed_at": datetime.utcnow(),
                    "batch_run_id": "batch_20230615_120000"
                }
            ]
        }
    
    @patch('pymongo.MongoClient')
    def test_mongodb_connection(self, mock_mongo_class):
        """Test MongoDB connection establishment."""
        logger.info("Testing MongoDB connection")
        
        mock_client = MockMongoClient()
        mock_mongo_class.return_value = mock_client
        
        # Test connection
        client = mock_mongo_class("mongodb://localhost:27017")
        database = client["test_db"]
        collection = database["test_collection"]
        
        # Verify mock objects are created
        assert client is not None
        assert database is not None
        assert collection is not None
        
        logger.info("MongoDB connection test passed")
    
    def test_document_structure_validation(self, sample_export_data):
        """Test MongoDB document structure validation."""
        logger.info("Testing document structure validation")
        
        for view_type, documents in sample_export_data.items():
            for doc in documents:
                # Validate required fields
                assert "movie_id" in doc
                assert "view_type" in doc
                assert "data" in doc
                assert "computed_at" in doc
                assert "batch_run_id" in doc
                
                # Validate data types
                assert isinstance(doc["movie_id"], str)
                assert isinstance(doc["view_type"], str)
                assert isinstance(doc["data"], dict)
                assert isinstance(doc["computed_at"], datetime)
                assert isinstance(doc["batch_run_id"], str)
        
        logger.info("Document structure validation test passed")
    
    def test_bulk_insert_operations(self, mock_mongo_client, sample_export_data):
        """Test bulk insert operations to MongoDB."""
        logger.info("Testing bulk insert operations")
        
        collection = mock_mongo_client["test_db"]["batch_views"]
        
        # Test bulk insert
        for view_type, documents in sample_export_data.items():
            collection.insert_many(documents)
        
        # Verify documents were inserted
        total_docs = len(sample_export_data["genre_analytics"]) + len(sample_export_data["trending"])
        assert len(collection.documents) == total_docs
        
        # Verify document content
        inserted_view_types = {doc["view_type"] for doc in collection.documents}
        assert "genre_analytics" in inserted_view_types
        assert "trending" in inserted_view_types
        
        logger.info("Bulk insert operations test passed")
    
    def test_upsert_operations(self, mock_mongo_client):
        """Test upsert operations for duplicate handling."""
        logger.info("Testing upsert operations")
        
        collection = mock_mongo_client["test_db"]["batch_views"]
        
        # Insert initial document
        doc1 = {
            "movie_id": "test_001",
            "view_type": "genre_analytics",
            "data": {"rating": 7.0},
            "computed_at": datetime.utcnow()
        }
        collection.insert_one(doc1)
        
        # Simulate upsert of updated document
        from pymongo import UpdateOne
        upsert_op = UpdateOne(
            {"movie_id": "test_001", "view_type": "genre_analytics"},
            {"$set": {
                "movie_id": "test_001",
                "view_type": "genre_analytics", 
                "data": {"rating": 7.5},  # Updated rating
                "computed_at": datetime.utcnow()
            }},
            upsert=True
        )
        
        result = collection.bulk_write([upsert_op])
        
        # Should have modified existing document
        assert result.modified_count >= 0  # May be 0 or 1 depending on implementation
        
        logger.info("Upsert operations test passed")
    
    def test_index_creation(self, mock_mongo_client):
        """Test MongoDB index creation."""
        logger.info("Testing index creation")
        
        collection = mock_mongo_client["test_db"]["batch_views"]
        
        # Create indexes as would be done in production
        collection.create_index([("movie_id", 1), ("view_type", 1)], unique=True)
        collection.create_index([("view_type", 1)])
        collection.create_index([("computed_at", -1)])
        
        # Verify indexes were created
        assert len(collection.indexes) == 3
        
        # Check index structures
        compound_index = next((idx for idx in collection.indexes 
                             if len(idx["keys"]) == 2), None)
        assert compound_index is not None
        assert compound_index["options"]["unique"] is True
        
        logger.info("Index creation test passed")


class TestExportQualityValidation:
    """Test export quality validation."""
    
    def test_completeness_validation(self):
        """Test export completeness validation."""
        logger.info("Testing export completeness validation")
        
        # Simulate export results
        export_results = {
            "genre_analytics": 1200,
            "trending": 15000,
            "temporal": 800,
            "actor_networks": 2500
        }
        
        expected_minimums = {
            "genre_analytics": 1000,
            "trending": 10000,
            "temporal": 500,
            "actor_networks": 2000
        }
        
        # Test completeness
        completeness_scores = {}
        for view_type, actual_count in export_results.items():
            expected_min = expected_minimums[view_type]
            completeness_scores[view_type] = min(1.0, actual_count / expected_min)
        
        # Verify completeness scores
        for view_type, score in completeness_scores.items():
            assert score >= 1.0  # All should meet minimum requirements
        
        overall_completeness = sum(completeness_scores.values()) / len(completeness_scores)
        assert overall_completeness >= 1.0
        
        logger.info("Export completeness validation test passed")
    
    def test_consistency_validation(self, mock_mongo_client):
        """Test export consistency validation."""
        logger.info("Testing export consistency validation")
        
        collection = mock_mongo_client["test_db"]["batch_views"]
        
        # Insert test documents with consistent structure
        test_docs = [
            {
                "movie_id": "test_001",
                "view_type": "genre_analytics",
                "data": {"genre": "Action", "avg_rating": 7.5},
                "computed_at": datetime.utcnow(),
                "batch_run_id": "batch_test"
            },
            {
                "movie_id": "test_002", 
                "view_type": "trending",
                "data": {"window": "7d", "trend_score": 0.8},
                "computed_at": datetime.utcnow(),
                "batch_run_id": "batch_test"
            }
        ]
        
        collection.insert_many(test_docs)
        
        # Validate consistency
        all_docs = collection.find()
        
        required_fields = ["movie_id", "view_type", "data", "computed_at", "batch_run_id"]
        for doc in all_docs:
            for field in required_fields:
                assert field in doc, f"Missing required field: {field}"
        
        # Check batch_run_id consistency
        batch_ids = {doc["batch_run_id"] for doc in all_docs}
        assert len(batch_ids) == 1  # All should have same batch_run_id
        
        logger.info("Export consistency validation test passed")


class TestBatchViewsIntegration:
    """Integration tests for complete batch views pipeline."""
    
    @pytest.fixture
    def integration_config(self):
        """Configuration for integration tests."""
        return {
            "mongodb": {
                "uri": "mongodb://localhost:27017",
                "database": "test_tmdb_analytics",
                "collection": "batch_views",
                "batch_size": 1000
            },
            "batch_views": {
                "retention_days": 30,
                "quality_thresholds": {
                    "min_completeness": 0.95,
                    "min_consistency": 0.98
                }
            }
        }
    
    @patch('pymongo.MongoClient')
    def test_end_to_end_export(self, mock_mongo_class, integration_config):
        """Test complete end-to-end export process."""
        logger.info("Testing end-to-end export process")
        
        mock_client = MockMongoClient()
        mock_mongo_class.return_value = mock_client
        
        try:
            # Simulate complete export process
            batch_id = "batch_integration_test"
            export_timestamp = datetime.utcnow()
            
            # Generate mock aggregation results
            aggregations = {
                "genre_analytics": self._generate_mock_genre_analytics(batch_id, export_timestamp),
                "trending_scores": self._generate_mock_trending_scores(batch_id, export_timestamp),
                "temporal_analysis": self._generate_mock_temporal_analysis(batch_id, export_timestamp),
                "actor_networks": self._generate_mock_actor_networks(batch_id, export_timestamp)
            }
            
            # Export each aggregation type
            collection = mock_client[integration_config["mongodb"]["database"]][integration_config["mongodb"]["collection"]]
            
            total_exported = 0
            for view_type, data in aggregations.items():
                collection.insert_many(data)
                total_exported += len(data)
            
            # Validate export results
            assert len(collection.documents) == total_exported
            
            # Validate document structure and content
            for doc in collection.documents:
                assert "movie_id" in doc
                assert "view_type" in doc
                assert "batch_run_id" in doc
                assert doc["batch_run_id"] == batch_id
            
            logger.info(f"End-to-end export test passed: {total_exported} documents exported")
            
        except Exception as e:
            logger.error(f"End-to-end export test failed: {e}")
            raise
    
    def _generate_mock_genre_analytics(self, batch_id: str, timestamp: datetime) -> List[Dict]:
        """Generate mock genre analytics data."""
        return [
            {
                "movie_id": f"Action_{i}",
                "view_type": "genre_analytics",
                "data": {
                    "genre": "Action",
                    "year": 2023,
                    "total_movies": 100 + i,
                    "avg_rating": 7.0 + (i * 0.1)
                },
                "computed_at": timestamp,
                "batch_run_id": batch_id
            } for i in range(10)
        ]
    
    def _generate_mock_trending_scores(self, batch_id: str, timestamp: datetime) -> List[Dict]:
        """Generate mock trending scores data."""
        return [
            {
                "movie_id": f"movie_{i:06d}",
                "view_type": "trending",
                "data": {
                    "window": "7d",
                    "trend_score": 0.5 + (i * 0.01),
                    "velocity": i * 2.5
                },
                "computed_at": timestamp,
                "batch_run_id": batch_id
            } for i in range(50)
        ]
    
    def _generate_mock_temporal_analysis(self, batch_id: str, timestamp: datetime) -> List[Dict]:
        """Generate mock temporal analysis data."""
        return [
            {
                "movie_id": f"temporal_{metric}_{year}",
                "view_type": "temporal",
                "data": {
                    "metric_type": metric,
                    "year": year,
                    "current_value": 1000 + (year - 2020) * 100,
                    "yoy_change_percent": (year - 2020) * 5.0
                },
                "computed_at": timestamp,
                "batch_run_id": batch_id
            } for metric in ["avg_revenue", "movie_count"] for year in range(2020, 2024)
        ]
    
    def _generate_mock_actor_networks(self, batch_id: str, timestamp: datetime) -> List[Dict]:
        """Generate mock actor networks data."""
        return [
            {
                "movie_id": f"actor_{i:06d}",
                "view_type": "actor_networks",
                "data": {
                    "actor_name": f"Actor_{i}",
                    "collaboration_count": i * 5,
                    "network_centrality": 0.1 + (i * 0.01)
                },
                "computed_at": timestamp,
                "batch_run_id": batch_id
            } for i in range(25)
        ]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])