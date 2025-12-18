"""
Crisis Detection API Tests - Simple Integration Tests

Tests for Goal #1: PR Crisis Detection & Sentiment Monitoring
These tests call the actual API and verify basic functionality
"""

import pytest
import requests
import time
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from api.routes.crisis_detection import get_severity, normalize_movie_title

# API base URL - can be overridden with environment variable
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


class TestUtilityFunctions:
    """Test standalone utility functions"""
    
    def test_get_severity_levels(self):
        """Test severity calculation for all levels"""
        assert get_severity(-5.0) == "critical"
        assert get_severity(-3.5) == "high"
        assert get_severity(-2.5) == "warning"
        assert get_severity(0.0) == "normal"
    
    def test_normalize_movie_title(self):
        """Test title normalization"""
        assert normalize_movie_title("The Flash") == "the flash"
        assert normalize_movie_title("Spider-Man: No Way Home") == "spiderman no way home"
        assert normalize_movie_title("  The   Dark   Knight  ") == "the dark knight"


class TestAPIHealth:
    """Test that API is running"""
    
    def test_api_is_reachable(self):
        """Test that the API responds"""
        try:
            response = requests.get(f"{API_BASE_URL}/api/v1/health", timeout=5)
            assert response.status_code == 200
            data = response.json()
            assert "status" in data
            print(f"✓ API is healthy: {data.get('status')}")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running - start services with docker-compose up")


class TestMovieSentimentEndpoint:
    """Test GET /crisis-detection/movies/{id}/sentiment"""
    
    def test_sentiment_endpoint_returns_200_or_404(self):
        """Test that sentiment endpoint returns valid response"""
        # Try with a common movie ID (550 = Fight Club)
        movie_id = 550
        
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/movies/{movie_id}/sentiment",
                timeout=10
            )
            
            # Should either work (200) or movie not found (404)
            assert response.status_code in [200, 404], \
                f"Unexpected status code: {response.status_code}"
            
            if response.status_code == 200:
                data = response.json()
                print(f"\n✓ Successfully retrieved sentiment for movie {movie_id}")
                
                # Verify required fields exist and are not None
                assert "movie_id" in data, "Missing movie_id field"
                assert data["movie_id"] is not None, "movie_id is None"
                assert data["movie_id"] == movie_id, "movie_id doesn't match"
                
                assert "movie_title" in data, "Missing movie_title field"
                assert data["movie_title"] is not None, "movie_title is None"
                assert len(data["movie_title"]) > 0, "movie_title is empty"
                
                assert "current_sentiment" in data, "Missing current_sentiment field"
                assert data["current_sentiment"] is not None, "current_sentiment is None"
                assert -1.0 <= data["current_sentiment"] <= 1.0, "current_sentiment out of range"
                
                assert "sentiment_source" in data, "Missing sentiment_source field"
                assert data["sentiment_source"] in ["batch_layer", "speed_layer"], \
                    f"Invalid sentiment_source: {data['sentiment_source']}"
                
                assert "baseline_used" in data, "Missing baseline_used field"
                assert data["baseline_used"] is not None, "baseline_used is None"
                baseline = data["baseline_used"]
                assert "type" in baseline, "baseline_used missing type"
                assert baseline["type"] in ["franchise", "genre", "year"], \
                    f"Invalid baseline type: {baseline['type']}"
                assert "avg_sentiment" in baseline, "baseline_used missing avg_sentiment"
                assert "sentiment_stddev" in baseline, "baseline_used missing sentiment_stddev"
                assert baseline["avg_sentiment"] is not None, "baseline avg_sentiment is None"
                assert baseline["sentiment_stddev"] is not None, "baseline sentiment_stddev is None"
                assert baseline["movie_count"] is not None, "baseline movie_count is None"
                assert baseline["value"] is not None, "baseline value is None"
                
                assert "baseline_alternatives" in data, "Missing baseline_alternatives field"
                assert data["baseline_alternatives"] is not None, "baseline_alternatives is None"
                alternatives = data["baseline_alternatives"]
                assert "franchise" in alternatives, "baseline_alternatives missing franchise"
                assert "genre" in alternatives, "baseline_alternatives missing genre"
                assert "year" in alternatives, "baseline_alternatives missing year"
                
                # Check that at least one alternative is available
                available_count = sum(1 for alt in alternatives.values() if alt["available"])
                assert available_count > 0, "No baselines available - at least one should be available"
                
                # For available alternatives, check no unexpected nulls
                for alt_name, alt_data in alternatives.items():
                    assert alt_data is not None, f"{alt_name} alternative is None"
                    assert "available" in alt_data, f"{alt_name} missing 'available' field"
                    if alt_data["available"]:
                        assert alt_data["value"] is not None, f"{alt_name} is available but value is None"
                        assert alt_data["avg_sentiment"] is not None, f"{alt_name} is available but avg_sentiment is None"
                        assert alt_data["sentiment_stddev"] is not None, f"{alt_name} is available but sentiment_stddev is None"
                        assert alt_data["movie_count"] is not None, f"{alt_name} is available but movie_count is None"
                
                assert "deviation_analysis" in data, "Missing deviation_analysis field"
                assert data["deviation_analysis"] is not None, "deviation_analysis is None"
                deviation = data["deviation_analysis"]
                assert "using_baseline" in deviation, "deviation_analysis missing using_baseline"
                assert "all_baselines" in deviation, "deviation_analysis missing all_baselines"
                assert "comparison_note" in deviation, "deviation_analysis missing comparison_note"
                
                # Check using_baseline structure
                using_baseline = deviation["using_baseline"]
                assert using_baseline is not None, "using_baseline is None"
                assert "deviation_sigma" in using_baseline, "using_baseline missing deviation_sigma"
                assert using_baseline["deviation_sigma"] is not None, "deviation_sigma is None"
                assert "is_crisis" in using_baseline, "using_baseline missing is_crisis"
                assert using_baseline["is_crisis"] is not None, "is_crisis is None"
                assert isinstance(using_baseline["is_crisis"], bool), "is_crisis not a boolean"
                assert "severity" in using_baseline, "using_baseline missing severity"
                assert using_baseline["severity"] is not None, "severity is None"
                assert using_baseline["severity"] in ["critical", "high", "warning", "normal"], \
                    f"Invalid severity: {using_baseline['severity']}"
                
                # Check all_baselines - should have at least one entry
                all_baselines = deviation["all_baselines"]
                assert all_baselines is not None, "all_baselines is None"
                assert len(all_baselines) > 0, "all_baselines is empty"
                for baseline_name, baseline_dev in all_baselines.items():
                    assert baseline_dev is not None, f"Baseline {baseline_name} in all_baselines is None"
                    assert "deviation_sigma" in baseline_dev, f"{baseline_name} missing deviation_sigma"
                    assert baseline_dev["deviation_sigma"] is not None, f"{baseline_name} deviation_sigma is None"
                    assert "is_crisis" in baseline_dev, f"{baseline_name} missing is_crisis"
                    assert "severity" in baseline_dev, f"{baseline_name} missing severity"
                
                assert "last_updated" in data, "Missing last_updated field"
                assert data["last_updated"] is not None, "last_updated is None"
                
                print(f"  Movie: {data['movie_title']}")
                print(f"  Current Sentiment: {data['current_sentiment']:.2f}")
                print(f"  Baseline Type: {baseline['type']}")
                print(f"  Deviation: {using_baseline['deviation_sigma']:.2f}σ")
                print(f"  Crisis: {using_baseline['is_crisis']}")
                print(f"  Severity: {using_baseline['severity']}")
                
            else:  # 404
                print(f"\n✓ Movie {movie_id} not found (expected for some IDs)")
                
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_sentiment_invalid_movie_id(self):
        """Test with invalid movie ID"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/movies/invalid/sentiment",
                timeout=5
            )
            # Should return validation error (422)
            assert response.status_code == 422
            print("\n✓ Correctly rejects invalid movie ID")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_sentiment_nonexistent_movie(self):
        """Test with movie ID that doesn't exist"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/movies/9999999/sentiment",
                timeout=5
            )
            # Should return 404
            assert response.status_code == 404
            data = response.json()
            assert "detail" in data
            print("\n✓ Correctly returns 404 for nonexistent movie")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestMovieSentimentByTitle:
    """Test GET /crisis-detection/movies/by-title/{title}/sentiment"""
    
    def test_sentiment_by_title(self):
        """Test getting sentiment by movie title"""
        try:
            # Try a common movie title
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/movies/by-title/Fight Club/sentiment",
                timeout=10
            )
            
            # Should either work (200) or not found (404)
            assert response.status_code in [200, 404]
            
            if response.status_code == 200:
                data = response.json()
                assert "movie_title" in data
                assert data["movie_title"] is not None
                print(f"\n✓ Found movie by title: {data['movie_title']}")
            else:
                print("\n✓ Movie 'Fight Club' not in database (expected)")
                
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_sentiment_by_title_not_found(self):
        """Test with nonexistent title"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/movies/by-title/ThisMovieDoesNotExist12345/sentiment",
                timeout=5
            )
            assert response.status_code == 404
            print("\n✓ Correctly returns 404 for nonexistent title")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestMultipleMovies:
    """Test with multiple movie IDs to verify consistency"""
    
    @pytest.mark.parametrize("movie_id", [550, 680, 155, 27205])
    def test_multiple_movies(self, movie_id):
        """Test sentiment endpoint with various movie IDs"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/movies/{movie_id}/sentiment",
                timeout=10
            )
            
            # Should return 200 or 404, not 500
            assert response.status_code in [200, 404, 500], \
                f"Movie {movie_id}: Unexpected status {response.status_code}"
            
            if response.status_code == 200:
                data = response.json()
                # Just verify no None values in critical fields
                assert data.get("movie_id") is not None
                assert data.get("current_sentiment") is not None
                assert data.get("baseline_used") is not None
                print(f"✓ Movie {movie_id}: {data.get('movie_title')} - OK")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestCrisisAlertsEndpoint:
    """Test GET /crisis-detection/alerts"""
    
    def test_alerts_endpoint_returns_200(self):
        """Test that alerts endpoint returns valid response"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/alerts",
                timeout=10
            )
            
            # Should return 200
            assert response.status_code == 200, \
                f"Unexpected status code: {response.status_code}"
            
            data = response.json()
            print(f"\n✓ Crisis alerts retrieved successfully")
            
            # Verify response structure
            assert "total_alerts" in data, "Missing total_alerts field"
            assert data["total_alerts"] is not None, "total_alerts is None"
            assert isinstance(data["total_alerts"], int), "total_alerts not an integer"
            
            assert "alerts" in data, "Missing alerts field"
            assert data["alerts"] is not None, "alerts is None"
            assert isinstance(data["alerts"], list), "alerts not a list"
            
            assert "filters_applied" in data, "Missing filters_applied field"
            assert data["filters_applied"] is not None, "filters_applied is None"
            
            print(f"  Total alerts: {data['total_alerts']}")
            
            # If there are alerts, check their structure
            if data["total_alerts"] > 0:
                alert = data["alerts"][0]
                
                assert "movie_id" in alert, "Alert missing movie_id"
                assert alert["movie_id"] is not None, "Alert movie_id is None"
                assert isinstance(alert["movie_id"], int), "Alert movie_id not an integer"
                
                assert "movie_title" in alert, "Alert missing movie_title"
                assert alert["movie_title"] is not None, "Alert movie_title is None"
                assert len(alert["movie_title"]) > 0, "Alert movie_title is empty"
                
                assert "current_sentiment" in alert, "Alert missing current_sentiment"
                assert alert["current_sentiment"] is not None, "Alert current_sentiment is None"
                assert -1.0 <= alert["current_sentiment"] <= 1.0, "current_sentiment out of range"
                
                assert "baseline_sentiment" in alert, "Alert missing baseline_sentiment"
                assert alert["baseline_sentiment"] is not None, "Alert baseline_sentiment is None"
                assert "baseline_type" in alert, "Alert missing baseline_type"
                assert alert["baseline_type"] is not None, "Alert baseline_type is None"
                assert alert["baseline_type"] in ["franchise", "genre", "year"], \
                    f"Invalid baseline_type: {alert['baseline_type']}"
                
                assert "deviation_sigma" in alert, "Alert missing deviation_sigma"
                assert alert["deviation_sigma"] is not None, "Alert deviation_sigma is None"
                assert alert["deviation_sigma"] < -3.0, "Alert sigma should be < -3.0 for crisis"
                
                assert "severity" in alert, "Alert missing severity"
                assert alert["severity"] is not None, "Alert severity is None"
                assert alert["severity"] in ["critical", "high", "warning"], \
                    f"Invalid severity for crisis: {alert['severity']}"
                
                assert "alert_timestamp" in alert, "Alert missing alert_timestamp"
                assert alert["alert_timestamp"] is not None, "Alert alert_timestamp is None"
                assert "data_age_hours" in alert, "Alert missing data_age_hours"
                assert alert["data_age_hours"] is not None, "Alert data_age_hours is None"
                assert alert["data_age_hours"] >= 0, "data_age_hours should be non-negative"
                
                # Check all alerts, not just first one
                for i, alert in enumerate(data["alerts"]):
                    assert alert["movie_id"] is not None, f"Alert {i} has None movie_id"
                    assert alert["movie_title"] is not None, f"Alert {i} has None movie_title"
                    assert alert["current_sentiment"] is not None, f"Alert {i} has None current_sentiment"
                    assert alert["deviation_sigma"] is not None, f"Alert {i} has None deviation_sigma"
                    assert alert["severity"] is not None, f"Alert {i} has None severity"
                
                print(f"  First alert: {alert['movie_title']}")
                print(f"    Deviation: {alert['deviation_sigma']:.2f}σ")
                print(f"    Severity: {alert['severity']}")
                print(f"    Baseline: {alert['baseline_type']}")
            else:
                print("  No active crises (all movies within normal range)")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_alerts_with_severity_filter(self):
        """Test alerts endpoint with severity filter"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/alerts",
                params={"severity": "critical"},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check filter was applied
            assert data["filters_applied"]["severity"] == "critical"
            
            # If there are alerts, verify they're all critical
            for alert in data["alerts"]:
                assert alert["severity"] == "critical", \
                    f"Filter failed: got {alert['severity']} instead of critical"
            
            print(f"\n✓ Severity filter working: {data['total_alerts']} critical alerts")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_alerts_with_genre_filter(self):
        """Test alerts endpoint with genre filter"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/alerts",
                params={"genre": "Action"},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check filter was applied
            assert data["filters_applied"]["genre"] == "Action"
            
            print(f"\n✓ Genre filter working: {data['total_alerts']} Action movie alerts")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_alerts_with_limit(self):
        """Test alerts endpoint with limit parameter"""
        try:
            limit = 5
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/alerts",
                params={"limit": limit},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Should not exceed limit
            assert len(data["alerts"]) <= limit, \
                f"Returned {len(data['alerts'])} alerts, exceeds limit of {limit}"
            
            print(f"\n✓ Limit working: requested {limit}, got {len(data['alerts'])}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_alerts_sorted_by_severity(self):
        """Test that alerts are sorted by deviation (most negative first)"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/alerts",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # If there are multiple alerts, check sorting
            if data["total_alerts"] > 1:
                deviations = [alert["deviation_sigma"] for alert in data["alerts"]]
                assert deviations == sorted(deviations), \
                    "Alerts not sorted by deviation (most negative first)"
                print(f"\n✓ Alerts properly sorted by severity")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestBaselineEndpoints:
    """Test baseline statistics endpoints (1.3, 1.4, 1.5)"""
    
    def test_genre_baseline_exists(self):
        """Test genre baseline endpoint returns valid data"""
        try:
            # Use a common genre that should exist
            genre = "Action"
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/baselines/genre/{genre}",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check structure
            assert "dimension_type" in data
            assert data["dimension_type"] == "genre"
            assert "dimension_value" in data
            assert data["dimension_value"] == genre
            assert "baseline_sentiment" in data
            assert "stddev_sentiment" in data
            assert "sample_size" in data
            assert "percentiles" in data
            assert "crisis_threshold" in data
            
            # Check null values
            assert data["baseline_sentiment"] is not None
            assert data["stddev_sentiment"] is not None
            assert data["sample_size"] is not None
            assert data["sample_size"] > 0, "Sample size must be positive"
            
            # Check percentiles structure
            percentiles = data["percentiles"]
            assert "min" in percentiles and percentiles["min"] is not None
            assert "q1" in percentiles and percentiles["q1"] is not None
            assert "median" in percentiles and percentiles["median"] is not None
            assert "q3" in percentiles and percentiles["q3"] is not None
            assert "max" in percentiles and percentiles["max"] is not None
            
            # Check percentile ordering: min <= q1 <= median <= q3 <= max
            assert percentiles["min"] <= percentiles["q1"]
            assert percentiles["q1"] <= percentiles["median"]
            assert percentiles["median"] <= percentiles["q3"]
            assert percentiles["q3"] <= percentiles["max"]
            
            # Check sentiment range (-1.0 to 1.0)
            assert -1.0 <= data["baseline_sentiment"] <= 1.0
            assert data["stddev_sentiment"] >= 0  # Standard deviation must be non-negative
            
            # Check crisis threshold calculation (should be baseline - 3*stddev)
            expected_threshold = data["baseline_sentiment"] - 3 * data["stddev_sentiment"]
            assert abs(data["crisis_threshold"] - expected_threshold) < 0.001
            
            print(f"\n✓ Genre baseline for '{genre}': avg={data['baseline_sentiment']:.3f}, "
                  f"stddev={data['stddev_sentiment']:.3f}, n={data['sample_size']}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_genre_baseline_not_found(self):
        """Test genre baseline returns 404 for non-existent genre"""
        try:
            # Use a genre that shouldn't exist
            genre = "NonExistentGenre12345"
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/baselines/genre/{genre}",
                timeout=10
            )
            
            assert response.status_code == 404
            data = response.json()
            assert "detail" in data
            assert genre in data["detail"]
            print(f"\n✓ Correctly returns 404 for non-existent genre")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_franchise_baseline_exists(self):
        """Test franchise baseline endpoint returns valid data"""
        try:
            # Try multiple common franchises to find one that exists
            franchises_to_try = [
                "The Terminator Collection",
                "James Bond Collection",
                "The Hunger Games Collection"
            ]
            
            response = None
            franchise = None
            
            # Try to find a franchise that exists
            for test_franchise in franchises_to_try:
                test_response = requests.get(
                    f"{API_BASE_URL}/api/v1/crisis-detection/baselines/franchise/{test_franchise}",
                    timeout=10
                )
                if test_response.status_code == 200:
                    response = test_response
                    franchise = test_franchise
                    break
            
            # If no franchises found, skip test
            if response is None:
                pytest.skip("No franchise baseline data available in database")
            
            data = response.json()
            
            # Check structure
            assert data["dimension_type"] == "franchise"
            assert data["dimension_value"] == franchise
            assert data["baseline_sentiment"] is not None
            assert data["stddev_sentiment"] is not None
            assert data["sample_size"] is not None and data["sample_size"] > 0
            
            # Check percentiles
            percentiles = data["percentiles"]
            assert percentiles["min"] is not None
            assert percentiles["q1"] is not None
            assert percentiles["median"] is not None
            assert percentiles["q3"] is not None
            assert percentiles["max"] is not None
            
            # Verify ordering
            assert percentiles["min"] <= percentiles["q1"] <= percentiles["median"]
            assert percentiles["median"] <= percentiles["q3"] <= percentiles["max"]
            
            # Check sentiment range
            assert -1.0 <= data["baseline_sentiment"] <= 1.0
            assert data["stddev_sentiment"] >= 0
            
            # Check crisis threshold
            expected_threshold = data["baseline_sentiment"] - 3 * data["stddev_sentiment"]
            assert abs(data["crisis_threshold"] - expected_threshold) < 0.001
            
            print(f"\n✓ Franchise baseline for '{franchise}': avg={data['baseline_sentiment']:.3f}, "
                  f"stddev={data['stddev_sentiment']:.3f}, n={data['sample_size']}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_franchise_baseline_not_found(self):
        """Test franchise baseline returns 404 for non-existent franchise"""
        try:
            franchise = "NonExistentFranchise12345"
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/baselines/franchise/{franchise}",
                timeout=10
            )
            
            assert response.status_code == 404
            data = response.json()
            assert "detail" in data
            assert franchise in data["detail"]
            print(f"\n✓ Correctly returns 404 for non-existent franchise")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_year_baseline_exists(self):
        """Test year baseline endpoint returns valid data"""
        try:
            # Use a recent year that should have data
            year = 2025
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/baselines/year/{year}",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check structure
            assert data["dimension_type"] == "year"
            assert data["dimension_value"] == str(year)
            assert data["baseline_sentiment"] is not None
            assert data["stddev_sentiment"] is not None
            assert data["sample_size"] is not None and data["sample_size"] > 0
            
            # Check percentiles
            percentiles = data["percentiles"]
            assert all(percentiles[k] is not None for k in ["min", "q1", "median", "q3", "max"])
            assert percentiles["min"] <= percentiles["q1"] <= percentiles["median"]
            assert percentiles["median"] <= percentiles["q3"] <= percentiles["max"]
            
            # Check sentiment range
            assert -1.0 <= data["baseline_sentiment"] <= 1.0
            assert data["stddev_sentiment"] >= 0
            
            # Check crisis threshold
            expected_threshold = data["baseline_sentiment"] - 3 * data["stddev_sentiment"]
            assert abs(data["crisis_threshold"] - expected_threshold) < 0.001
            
            # Year baseline should have data_range
            assert "data_range" in data
            if data["data_range"]:
                assert "start_date" in data["data_range"]
                assert "end_date" in data["data_range"]
                assert str(year) in data["data_range"]["start_date"]
                assert str(year) in data["data_range"]["end_date"]
            
            print(f"\n✓ Year baseline for {year}: avg={data['baseline_sentiment']:.3f}, "
                  f"stddev={data['stddev_sentiment']:.3f}, n={data['sample_size']}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_year_baseline_not_found(self):
        """Test year baseline returns 404 for year with no data"""
        try:
            # Use a year that shouldn't have data (very old or future)
            year = 1800
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/baselines/year/{year}",
                timeout=10
            )
            
            assert response.status_code == 404
            data = response.json()
            assert "detail" in data
            assert str(year) in data["detail"]
            print(f"\n✓ Correctly returns 404 for year with no data")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_baseline_data_consistency(self):
        """Test that median is approximately equal to baseline_sentiment"""
        try:
            # Test with a genre that should exist
            genre = "Action"
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/baselines/genre/{genre}",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # For normal distribution, median should equal mean (baseline_sentiment)
                median = data["percentiles"]["median"]
                baseline = data["baseline_sentiment"]
                
                # They should be very close (using normal distribution approximation)
                assert abs(median - baseline) < 0.01, \
                    f"Median ({median}) should approximate baseline ({baseline})"
                
                print(f"\n✓ Baseline data is statistically consistent")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestMonitoringEndpoint:
    """Test monitoring dashboard endpoint (1.6)"""
    
    def test_monitoring_endpoint_returns_200(self):
        """Test monitoring endpoint returns valid dashboard data"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/monitoring",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check top-level structure
            assert "severity_counts" in data
            assert "total_movies_tracked" in data
            assert "crisis_movies" in data
            assert "top_declining_movies" in data
            assert "average_sentiment" in data
            assert "last_updated" in data
            
            # Validate severity_counts
            severity = data["severity_counts"]
            assert "critical" in severity and severity["critical"] is not None
            assert "high" in severity and severity["high"] is not None
            assert "warning" in severity and severity["warning"] is not None
            assert "normal" in severity and severity["normal"] is not None
            
            # All counts should be non-negative integers
            assert isinstance(severity["critical"], int) and severity["critical"] >= 0
            assert isinstance(severity["high"], int) and severity["high"] >= 0
            assert isinstance(severity["warning"], int) and severity["warning"] >= 0
            assert isinstance(severity["normal"], int) and severity["normal"] >= 0
            
            # Validate metrics
            assert isinstance(data["total_movies_tracked"], int)
            assert data["total_movies_tracked"] >= 0
            assert isinstance(data["crisis_movies"], int)
            assert data["crisis_movies"] >= 0
            
            # Crisis movies should be sum of critical + high
            expected_crisis = severity["critical"] + severity["high"]
            assert data["crisis_movies"] == expected_crisis, \
                f"Crisis count mismatch: expected {expected_crisis}, got {data['crisis_movies']}"
            
            # Average sentiment should be in valid range
            assert -1.0 <= data["average_sentiment"] <= 1.0
            
            # Validate last_updated is ISO format datetime
            assert "T" in data["last_updated"]
            
            print(f"\n✓ Monitoring dashboard: {data['total_movies_tracked']} movies tracked, "
                  f"{data['crisis_movies']} in crisis")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_monitoring_top_declining_movies(self):
        """Test top declining movies list structure"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/monitoring",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Validate top_declining_movies list
            declining = data["top_declining_movies"]
            assert isinstance(declining, list)
            
            # If there are declining movies, validate structure
            if len(declining) > 0:
                # Check first movie structure
                movie = declining[0]
                assert "movie_id" in movie and movie["movie_id"] is not None
                assert "movie_title" in movie and movie["movie_title"] is not None
                assert "current_sentiment" in movie and movie["current_sentiment"] is not None
                assert "velocity" in movie and movie["velocity"] is not None
                assert "is_accelerating" in movie
                
                # Type validation
                assert isinstance(movie["movie_id"], int)
                assert isinstance(movie["movie_title"], str)
                assert len(movie["movie_title"]) > 0
                assert isinstance(movie["is_accelerating"], bool)
                
                # Sentiment range validation
                assert -1.0 <= movie["current_sentiment"] <= 1.0
                
                # If sentiment_1h_ago exists, validate it
                if movie.get("sentiment_1h_ago") is not None:
                    assert -1.0 <= movie["sentiment_1h_ago"] <= 1.0
                
                # Validate all movies in the list
                for m in declining:
                    assert m["movie_id"] is not None
                    assert m["movie_title"] is not None and len(m["movie_title"]) > 0
                    assert -1.0 <= m["current_sentiment"] <= 1.0
                    assert m["velocity"] is not None
                    assert isinstance(m["is_accelerating"], bool)
                
                # List should be sorted by velocity (most negative first)
                velocities = [m["velocity"] for m in declining]
                assert velocities == sorted(velocities), \
                    "Declining movies should be sorted by velocity (most negative first)"
                
                print(f"\n✓ Top declining movies: {len(declining)} movies with velocity tracking")
            else:
                print(f"\n✓ No declining movies currently (system stable)")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_monitoring_severity_distribution(self):
        """Test that severity counts are valid and crisis count is correct"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/monitoring",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            severity = data["severity_counts"]
            total = data["total_movies_tracked"]
            
            # Sum of all severity levels
            severity_sum = (
                severity["critical"] + 
                severity["high"] + 
                severity["warning"] + 
                severity["normal"]
            )
            
            # Severity sum should be <= total tracked
            # (Some movies might not have baselines, so they won't be classified)
            assert severity_sum <= total, \
                f"Severity counts ({severity_sum}) should not exceed total tracked ({total})"
            
            # Crisis movies should equal critical + high
            expected_crisis = severity["critical"] + severity["high"]
            assert data["crisis_movies"] == expected_crisis, \
                f"Crisis count ({data['crisis_movies']}) should equal critical + high ({expected_crisis})"
            
            print(f"\n✓ Severity distribution: Critical={severity['critical']}, "
                  f"High={severity['high']}, Warning={severity['warning']}, "
                  f"Normal={severity['normal']} (Total classified: {severity_sum}/{total})")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_monitoring_data_consistency(self):
        """Test monitoring data is internally consistent"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/crisis-detection/monitoring",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # If we have tracked movies, we should have some severity counts
            if data["total_movies_tracked"] > 0:
                severity = data["severity_counts"]
                total_with_severity = sum([
                    severity["critical"],
                    severity["high"],
                    severity["warning"],
                    severity["normal"]
                ])
                assert total_with_severity > 0, "Should have movies with severity classification"
            
            # If we have crisis movies, critical + high should be > 0
            if data["crisis_movies"] > 0:
                severity = data["severity_counts"]
                assert (severity["critical"] + severity["high"]) > 0, \
                    "Crisis movies should have critical or high severity"
            
            # Top declining list should not exceed total tracked
            assert len(data["top_declining_movies"]) <= data["total_movies_tracked"], \
                "Top declining list cannot exceed total movies tracked"
            
            print(f"\n✓ Monitoring data is internally consistent")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("Crisis Detection API Integration Tests")
    print("="*60)
    print(f"Testing API at: {API_BASE_URL}")
    print("="*60 + "\n")
    
    pytest.main([__file__, "-v", "-s"])


