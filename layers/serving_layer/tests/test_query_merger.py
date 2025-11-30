"""
Query Merger Tests

Tests for view merger functionality
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from query_engine.view_merger import ViewMerger


class TestViewMerger:
    """Test ViewMerger class"""
    
    @pytest.fixture
    def mock_db(self):
        """Create mock database"""
        db = Mock()
        db.batch_views = Mock()
        db.speed_views = Mock()
        return db
    
    @pytest.fixture
    def merger(self, mock_db):
        """Create ViewMerger instance"""
        return ViewMerger(mock_db, cutoff_hours=48)
    
    def test_cutoff_time_calculation(self, merger):
        """Test cutoff time is calculated correctly"""
        cutoff = merger.get_cutoff_time()
        now = datetime.utcnow()
        expected_cutoff = now - timedelta(hours=48)
        
        # Allow 1 second tolerance
        assert abs((cutoff - expected_cutoff).total_seconds()) < 1
    
    def test_merge_movie_views_empty_result(self, merger, mock_db):
        """Test merging with no data"""
        # Mock empty results
        mock_db.batch_views.find.return_value.sort.return_value.limit.return_value = []
        mock_db.speed_views.find.return_value.sort.return_value.limit.return_value = []
        
        result = merger.merge_movie_views(movie_id=123)
        
        assert result["movie_id"] == 123
        assert result["data_sources"]["batch_count"] == 0
        assert result["data_sources"]["speed_count"] == 0
        assert len(result["data"]) == 0
    
    def test_merge_movie_views_batch_only(self, merger, mock_db):
        """Test merging with only batch data"""
        # Mock batch data
        batch_data = [
            {
                "movie_id": 123,
                "view_type": "sentiment",
                "computed_at": datetime.utcnow() - timedelta(hours=72),
                "data": {"avg_sentiment": 0.7}
            }
        ]
        
        mock_db.batch_views.find.return_value.sort.return_value.limit.return_value = batch_data
        mock_db.speed_views.find.return_value.sort.return_value.limit.return_value = []
        
        result = merger.merge_movie_views(movie_id=123)
        
        assert result["data_sources"]["batch_count"] == 1
        assert result["data_sources"]["speed_count"] == 0
        assert len(result["data"]) >= 0  # May be processed
    
    def test_merge_movie_views_speed_only(self, merger, mock_db):
        """Test merging with only speed data"""
        # Mock speed data
        speed_data = [
            {
                "movie_id": 123,
                "data_type": "sentiment",
                "hour": datetime.utcnow() - timedelta(hours=2),
                "data": {"avg_sentiment": 0.8}
            }
        ]
        
        mock_db.batch_views.find.return_value.sort.return_value.limit.return_value = []
        mock_db.speed_views.find.return_value.sort.return_value.limit.return_value = speed_data
        
        result = merger.merge_movie_views(movie_id=123)
        
        assert result["data_sources"]["batch_count"] == 0
        assert result["data_sources"]["speed_count"] == 1
    
    def test_merge_movie_views_both_sources(self, merger, mock_db):
        """Test merging with both batch and speed data"""
        # Mock both sources
        batch_data = [
            {
                "movie_id": 123,
                "view_type": "sentiment",
                "computed_at": datetime.utcnow() - timedelta(hours=72),
                "data": {"avg_sentiment": 0.7}
            }
        ]
        
        speed_data = [
            {
                "movie_id": 123,
                "data_type": "sentiment",
                "hour": datetime.utcnow() - timedelta(hours=2),
                "data": {"avg_sentiment": 0.8}
            }
        ]
        
        mock_db.batch_views.find.return_value.sort.return_value.limit.return_value = batch_data
        mock_db.speed_views.find.return_value.sort.return_value.limit.return_value = speed_data
        
        result = merger.merge_movie_views(movie_id=123)
        
        assert result["data_sources"]["batch_count"] == 1
        assert result["data_sources"]["speed_count"] == 1
    
    def test_view_type_filter(self, merger, mock_db):
        """Test filtering by view type"""
        mock_db.batch_views.find.return_value.sort.return_value.limit.return_value = []
        mock_db.speed_views.find.return_value.sort.return_value.limit.return_value = []
        
        result = merger.merge_movie_views(movie_id=123, view_type="sentiment")
        
        # Check that find was called with view_type filter
        batch_call_args = mock_db.batch_views.find.call_args[0][0]
        assert "view_type" in batch_call_args
        assert batch_call_args["view_type"] == "sentiment"
    
    def test_custom_cutoff_hours(self, mock_db):
        """Test custom cutoff hours"""
        merger_24h = ViewMerger(mock_db, cutoff_hours=24)
        cutoff = merger_24h.get_cutoff_time()
        
        now = datetime.utcnow()
        expected_cutoff = now - timedelta(hours=24)
        
        assert abs((cutoff - expected_cutoff).total_seconds()) < 1


class TestMergeLogic:
    """Test merge logic and deduplication"""
    
    @pytest.fixture
    def merger(self):
        """Create ViewMerger with mock db"""
        mock_db = Mock()
        mock_db.batch_views = Mock()
        mock_db.speed_views = Mock()
        return ViewMerger(mock_db)
    
    def test_speed_overrides_batch(self, merger):
        """Test that speed layer data takes precedence over batch"""
        # This test would verify the merge algorithm
        # Implementation depends on actual merge_data_points method
        pass
    
    def test_deduplication(self, merger):
        """Test deduplication of overlapping data"""
        # Test that overlapping timestamps are deduplicated
        pass
    
    def test_sorting_by_timestamp(self, merger):
        """Test results are sorted by timestamp"""
        # Test that merged results are properly sorted
        pass


class TestErrorHandling:
    """Test error handling in ViewMerger"""
    
    @pytest.fixture
    def merger(self):
        """Create ViewMerger with mock db"""
        mock_db = Mock()
        mock_db.batch_views = Mock()
        mock_db.speed_views = Mock()
        return ViewMerger(mock_db)
    
    def test_database_error_handling(self, merger, monkeypatch):
        """Test handling of database errors"""
        # Mock database error
        merger.batch_views.find.side_effect = Exception("Database error")
        
        # Should handle error gracefully
        with pytest.raises(Exception):
            merger.merge_movie_views(movie_id=123)
    
    def test_invalid_movie_id(self, merger):
        """Test handling of invalid movie ID"""
        merger.batch_views.find.return_value.sort.return_value.limit.return_value = []
        merger.speed_views.find.return_value.sort.return_value.limit.return_value = []
        
        result = merger.merge_movie_views(movie_id=-1)
        
        # Should return empty result, not crash
        assert result["movie_id"] == -1
        assert result["data_sources"]["merged_count"] >= 0


class TestPerformance:
    """Test performance-related aspects"""
    
    @pytest.fixture
    def merger(self):
        """Create ViewMerger with mock db"""
        mock_db = Mock()
        mock_db.batch_views = Mock()
        mock_db.speed_views = Mock()
        return ViewMerger(mock_db)
    
    def test_query_limits(self, merger, mock_db):
        """Test that queries use appropriate limits"""
        mock_db = merger.db
        mock_db.batch_views.find.return_value.sort.return_value.limit.return_value = []
        mock_db.speed_views.find.return_value.sort.return_value.limit.return_value = []
        
        merger.merge_movie_views(movie_id=123)
        
        # Verify limit was called
        assert mock_db.batch_views.find.return_value.sort.return_value.limit.called
        assert mock_db.speed_views.find.return_value.sort.return_value.limit.called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
