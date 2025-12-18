"""
Viral Detection API Tests - Integration Tests

Tests for Goal #2: Viral Content Identification
These tests call the actual API and verify functionality
"""

import pytest
import requests
import time
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from api.routes.viral_detection import normalize_movie_title

# API base URL - can be overridden with environment variable
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


class TestUtilityFunctions:
    """Test standalone utility functions"""
    
    def test_normalize_movie_title(self):
        """Test title normalization"""
        assert normalize_movie_title("The Flash") == "the flash"
        assert normalize_movie_title("Spider-Man: No Way Home") == "spiderman no way home"
        assert normalize_movie_title("  The   Dark   Knight  ") == "the dark knight"
        assert normalize_movie_title("Avengers: Endgame!") == "avengers endgame"


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


class TestTrendingEndpoint:
    """Test GET /viral-detection/trending"""
    
    def test_trending_endpoint_basic(self):
        """Test that trending endpoint returns valid response"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                timeout=10
            )
            
            # Should return 200 (even if empty list)
            assert response.status_code == 200, \
                f"Unexpected status code: {response.status_code}"
            
            data = response.json()
            print(f"\n✓ Successfully retrieved trending movies")
            
            # Verify response structure
            assert "movies" in data, "Missing movies field"
            assert "count" in data, "Missing count field"
            assert "filters_applied" in data, "Missing filters_applied field"
            
            # Verify data types
            assert isinstance(data["movies"], list), "movies should be a list"
            assert isinstance(data["count"], int), "count should be an integer"
            assert isinstance(data["filters_applied"], dict), "filters_applied should be a dict"
            
            # Count should match array length
            assert data["count"] == len(data["movies"]), \
                f"Count mismatch: count={data['count']}, len(movies)={len(data['movies'])}"
            
            print(f"  Found {data['count']} trending movies")
            
            # If we have movies, verify their structure
            if data["movies"]:
                self._verify_movie_structure(data["movies"][0])
                print(f"  Top movie: {data['movies'][0]['movie_title']} " +
                      f"(V={data['movies'][0]['viral_metrics']['viral_coefficient']:.4f})")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running - start services with docker-compose up")
    
    def test_trending_with_limit(self):
        """Test trending endpoint with limit parameter"""
        try:
            limit = 5
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"limit": limit},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Should respect limit
            assert len(data["movies"]) <= limit, \
                f"Returned {len(data['movies'])} movies, expected at most {limit}"
            
            # Verify filters_applied shows the limit
            assert data["filters_applied"]["limit"] == limit
            
            print(f"✓ Limit parameter working: requested {limit}, got {len(data['movies'])}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_trending_with_genre_filter(self):
        """Test trending endpoint with genre filter"""
        try:
            genre = "Action"
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"genre": genre},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify filters_applied shows the genre
            assert data["filters_applied"]["genre"] == genre
            
            # All returned movies should match genre
            for movie in data["movies"]:
                assert movie["movie_intelligence"]["genre"].lower() == genre.lower(), \
                    f"Movie genre mismatch: expected {genre}, got {movie['movie_intelligence']['genre']}"
            
            print(f"✓ Genre filter working: {len(data['movies'])} {genre} movies found")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_trending_with_viral_threshold(self):
        """Test trending endpoint with viral threshold filter"""
        try:
            threshold = 0.1
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"viral_threshold": threshold},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify filters_applied shows the threshold
            assert data["filters_applied"]["viral_threshold"] == threshold
            
            # All returned movies should have V >= threshold
            for movie in data["movies"]:
                V = movie["viral_metrics"]["viral_coefficient"]
                assert V >= threshold, \
                    f"Viral coefficient {V} is below threshold {threshold}"
            
            print(f"✓ Viral threshold filter working: {len(data['movies'])} movies with V >= {threshold}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_trending_with_custom_window(self):
        """Test trending endpoint with custom time window"""
        try:
            window = 24  # 24 hours instead of default 48
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"window": window},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify filters_applied shows the window
            assert data["filters_applied"]["window_hours"] == window
            
            print(f"✓ Time window parameter working: {window}h window, {len(data['movies'])} movies")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_trending_sorting_order(self):
        """Test that movies are sorted by viral coefficient descending"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"limit": 10},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Extract viral coefficients
            coefficients = [
                movie["viral_metrics"]["viral_coefficient"]
                for movie in data["movies"]
            ]
            
            # Verify descending order
            for i in range(len(coefficients) - 1):
                assert coefficients[i] >= coefficients[i+1], \
                    f"Movies not sorted: V[{i}]={coefficients[i]} < V[{i+1}]={coefficients[i+1]}"
            
            print(f"✓ Sorting verified: movies ordered by viral coefficient (descending)")
            if coefficients:
                print(f"  Range: {coefficients[0]:.4f} to {coefficients[-1]:.4f}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_trending_viral_status_classification(self):
        """Test that viral status is correctly classified"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"limit": 50},  # Get more movies to test different statuses
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify status thresholds for each movie
            for movie in data["movies"]:
                V = movie["viral_metrics"]["viral_coefficient"]
                status = movie["viral_metrics"]["viral_status"]
                
                if V >= 0.3:
                    assert status == "viral", \
                        f"Expected 'viral' for V={V}, got '{status}'"
                elif V >= 0.15:
                    assert status == "trending", \
                        f"Expected 'trending' for V={V}, got '{status}'"
                elif V >= 0.05:
                    assert status == "growing", \
                        f"Expected 'growing' for V={V}, got '{status}'"
                else:
                    assert status == "stable", \
                        f"Expected 'stable' for V={V}, got '{status}'"
            
            # Count statuses
            status_counts = {}
            for movie in data["movies"]:
                status = movie["viral_metrics"]["viral_status"]
                status_counts[status] = status_counts.get(status, 0) + 1
            
            print(f"✓ Viral status classification verified")
            print(f"  Status distribution: {status_counts}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_trending_threshold_context(self):
        """Test that threshold context is properly set"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"limit": 10},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            for movie in data["movies"]:
                threshold_ctx = movie["threshold_context"]
                
                # Verify required fields
                assert "threshold_used" in threshold_ctx
                assert threshold_ctx["threshold_used"] > 0, "Threshold should be positive"
                
                assert "threshold_type" in threshold_ctx
                assert threshold_ctx["threshold_type"] == "avg_popularity", \
                    "Should use avg_popularity as threshold type"
                
                assert "threshold_dimension" in threshold_ctx
                assert threshold_ctx["threshold_dimension"] in ["genre", "global"], \
                    f"Invalid threshold dimension: {threshold_ctx['threshold_dimension']}"
            
            print(f"✓ Threshold context verified")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_trending_empty_results(self):
        """Test behavior with filters that match no movies"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={
                    "viral_threshold": 999.0,  # Impossibly high threshold
                    "limit": 10
                },
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Should return empty list, not error
            assert data["movies"] == []
            assert data["count"] == 0
            
            print(f"✓ Empty results handled correctly")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_trending_invalid_parameters(self):
        """Test error handling for invalid parameters"""
        try:
            # Test negative limit
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"limit": -5},
                timeout=10
            )
            assert response.status_code == 422, "Should reject negative limit"
            
            # Test limit too high
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"limit": 1000},
                timeout=10
            )
            assert response.status_code == 422, "Should reject limit > 100"
            
            # Test negative viral threshold
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"viral_threshold": -1.0},
                timeout=10
            )
            assert response.status_code == 422, "Should reject negative threshold"
            
            print(f"✓ Parameter validation working correctly")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def _verify_movie_structure(self, movie: dict):
        """Helper to verify structure of a trending movie response"""
        
        # Movie identification
        assert "movie_id" in movie
        assert isinstance(movie["movie_id"], int)
        assert "movie_title" in movie
        assert isinstance(movie["movie_title"], str)
        assert len(movie["movie_title"]) > 0
        
        # Viral metrics
        assert "viral_metrics" in movie
        vm = movie["viral_metrics"]
        assert "viral_coefficient" in vm
        assert isinstance(vm["viral_coefficient"], (int, float))
        assert vm["viral_coefficient"] >= 0
        assert "viral_score" in vm
        assert isinstance(vm["viral_score"], (int, float))
        assert "viral_status" in vm
        assert vm["viral_status"] in ["viral", "trending", "growing", "stable"]
        assert "upvote_velocity" in vm
        assert "comment_velocity" in vm
        assert "award_velocity" in vm
        
        # Reddit engagement
        assert "reddit_engagement" in movie
        re_data = movie["reddit_engagement"]
        assert "total_upvotes" in re_data
        assert isinstance(re_data["total_upvotes"], int)
        assert "total_comments" in re_data
        assert isinstance(re_data["total_comments"], int)
        assert "total_awards" in re_data
        assert isinstance(re_data["total_awards"], int)
        assert "avg_sentiment" in re_data
        assert isinstance(re_data["avg_sentiment"], (int, float))
        
        # Movie intelligence
        assert "movie_intelligence" in movie
        mi = movie["movie_intelligence"]
        assert "genre" in mi
        assert isinstance(mi["genre"], str)
        
        # Threshold context
        assert "threshold_context" in movie
        tc = movie["threshold_context"]
        assert "threshold_used" in tc
        assert "threshold_type" in tc
        assert "threshold_dimension" in tc
        
        # Timestamps
        assert "last_window_start" in movie
        # Verify timestamp is valid ISO format
        datetime.fromisoformat(movie["last_window_start"].replace('Z', '+00:00'))


class TestTrendingPerformance:
    """Test performance characteristics"""
    
    def test_trending_response_time(self):
        """Test that trending endpoint responds within reasonable time"""
        try:
            start_time = time.time()
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending",
                params={"limit": 20},
                timeout=10
            )
            end_time = time.time()
            
            assert response.status_code == 200
            
            response_time = end_time - start_time
            assert response_time < 5.0, \
                f"Response too slow: {response_time:.2f}s (expected < 5s)"
            
            print(f"✓ Response time acceptable: {response_time:.2f}s")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestViralScoreEndpoint:
    """Test GET /viral-detection/movies/{id}/viral-score"""
    
    def test_viral_score_valid_movie(self):
        """Test viral score endpoint with a movie that has Reddit activity"""
        try:
            # First get a movie ID from trending
            trending_response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending?limit=1",
                timeout=10
            )
            
            if trending_response.status_code != 200 or not trending_response.json().get("movies"):
                pytest.skip("No trending movies available for testing")
            
            movie_id = trending_response.json()["movies"][0]["movie_id"]
            
            # Test the viral score endpoint
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/movies/{movie_id}/viral-score",
                timeout=10
            )
            
            assert response.status_code == 200, \
                f"Expected 200, got {response.status_code}"
            
            data = response.json()
            print(f"\n✓ Successfully retrieved viral score for movie {movie_id}")
            
            # Verify response structure
            self._verify_viral_score_structure(data)
            
            print(f"  Movie: {data['movie_title']}")
            print(f"  Viral Coefficient: {data['viral_metrics']['viral_coefficient']:.4f}")
            print(f"  Status: {data['viral_metrics']['viral_status']}")
            print(f"  Windows: {data['window_count']}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_viral_score_movie_not_found(self):
        """Test viral score endpoint with non-existent movie ID"""
        try:
            # Use a very unlikely movie ID
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/movies/99999999/viral-score",
                timeout=10
            )
            
            assert response.status_code == 404, \
                f"Expected 404 for non-existent movie, got {response.status_code}"
            
            print(f"\n✓ Correctly returns 404 for non-existent movie")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_viral_score_no_reddit_activity(self):
        """Test viral score endpoint with movie that has no Reddit activity"""
        try:
            # Get a movie from batch layer that might not have Reddit activity
            # We'll use a movie with very low popularity
            # This test might skip if all movies have some activity
            
            # Try a few different movie IDs
            test_ids = [550, 238, 424, 680]  # Classic movies unlikely to have recent Reddit buzz
            
            found_inactive = False
            for movie_id in test_ids:
                response = requests.get(
                    f"{API_BASE_URL}/api/v1/viral-detection/movies/{movie_id}/viral-score",
                    timeout=10
                )
                
                if response.status_code == 404:
                    detail = response.json().get("detail", "")
                    if "No recent discussion data" in detail:
                        found_inactive = True
                        print(f"\n✓ Correctly returns 404 for movie without recent Reddit activity")
                        break
            
            if not found_inactive:
                pytest.skip("All test movies have Reddit activity")
                
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_viral_score_metrics_consistency(self):
        """Test that viral score metrics are consistent with trending endpoint"""
        try:
            # Get a movie from trending
            trending_response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending?limit=1",
                timeout=10
            )
            
            if trending_response.status_code != 200 or not trending_response.json().get("movies"):
                pytest.skip("No trending movies available")
            
            trending_movie = trending_response.json()["movies"][0]
            movie_id = trending_movie["movie_id"]
            
            # Get detailed viral score
            score_response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/movies/{movie_id}/viral-score",
                timeout=10
            )
            
            assert score_response.status_code == 200
            score_data = score_response.json()
            
            # Compare key metrics (they should be identical or very close)
            trending_coef = trending_movie["viral_metrics"]["viral_coefficient"]
            score_coef = score_data["viral_metrics"]["viral_coefficient"]
            
            # Allow small floating point differences
            assert abs(trending_coef - score_coef) < 0.001, \
                f"Viral coefficient mismatch: trending={trending_coef}, score={score_coef}"
            
            # Compare viral status
            assert trending_movie["viral_metrics"]["viral_status"] == score_data["viral_metrics"]["viral_status"], \
                "Viral status should match between endpoints"
            
            print(f"\n✓ Metrics consistent between trending and viral-score endpoints")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_viral_score_time_range(self):
        """Test that time range is calculated correctly"""
        try:
            # Get a movie with viral score
            trending_response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending?limit=1",
                timeout=10
            )
            
            if trending_response.status_code != 200 or not trending_response.json().get("movies"):
                pytest.skip("No trending movies available")
            
            movie_id = trending_response.json()["movies"][0]["movie_id"]
            
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/movies/{movie_id}/viral-score",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify time range is within TTL (48 hours)
            time_range = data["time_range_hours"]
            assert 0 <= time_range <= 48, \
                f"Time range {time_range}h should be within 0-48h (TTL period)"
            
            # Verify window count is positive
            assert data["window_count"] > 0, "Should have at least one window"
            
            # Verify timestamps are valid and in order
            first_window = datetime.fromisoformat(data["first_window_start"].replace('Z', '+00:00'))
            last_window = datetime.fromisoformat(data["last_window_start"].replace('Z', '+00:00'))
            
            assert last_window >= first_window, \
                "Last window should be after or equal to first window"
            
            print(f"\n✓ Time range validation passed")
            print(f"  Time range: {time_range}h")
            print(f"  Window count: {data['window_count']}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_viral_score_threshold_context(self):
        """Test that threshold context is properly included"""
        try:
            # Get a movie with viral score
            trending_response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending?limit=1",
                timeout=10
            )
            
            if trending_response.status_code != 200 or not trending_response.json().get("movies"):
                pytest.skip("No trending movies available")
            
            movie_id = trending_response.json()["movies"][0]["movie_id"]
            
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/movies/{movie_id}/viral-score",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify threshold context
            threshold_ctx = data["threshold_context"]
            assert "threshold_used" in threshold_ctx
            assert threshold_ctx["threshold_used"] > 0
            assert threshold_ctx["threshold_type"] == "avg_popularity"
            assert threshold_ctx["threshold_dimension"] in ["genre", "global"]
            
            print(f"\n✓ Threshold context validated")
            print(f"  Dimension: {threshold_ctx['threshold_dimension']}")
            print(f"  Threshold: {threshold_ctx['threshold_used']:.2f}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def _verify_viral_score_structure(self, data: dict):
        """Helper to verify structure of viral score response"""
        
        # Movie identification
        assert "movie_id" in data
        assert isinstance(data["movie_id"], int)
        assert "movie_title" in data
        assert isinstance(data["movie_title"], str)
        
        # Viral metrics
        assert "viral_metrics" in data
        vm = data["viral_metrics"]
        assert "viral_coefficient" in vm
        assert isinstance(vm["viral_coefficient"], (int, float))
        assert vm["viral_coefficient"] >= 0
        assert "viral_score" in vm
        assert "viral_status" in vm
        assert vm["viral_status"] in ["viral", "trending", "growing", "stable"]
        
        # Reddit engagement
        assert "reddit_engagement" in data
        
        # Time series data
        assert "window_count" in data
        assert isinstance(data["window_count"], int)
        assert data["window_count"] > 0
        assert "time_range_hours" in data
        assert isinstance(data["time_range_hours"], int)
        
        # Movie intelligence
        assert "movie_intelligence" in data
        
        # Threshold context
        assert "threshold_context" in data
        
        # Timestamps
        assert "first_window_start" in data
        assert "last_window_start" in data


class TestThresholdsEndpoint:
    """Test GET /viral-detection/thresholds"""
    
    def test_thresholds_all_genres(self):
        """Test getting all genre thresholds (no filter)"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/thresholds",
                timeout=10
            )
            
            assert response.status_code == 200, \
                f"Expected 200, got {response.status_code}"
            
            data = response.json()
            print(f"\n✓ Successfully retrieved all genre thresholds")
            
            # Verify response structure
            assert "thresholds" in data, "Missing thresholds field"
            assert "count" in data, "Missing count field"
            assert "note" in data, "Missing note field"
            
            # Verify data types
            assert isinstance(data["thresholds"], list), "thresholds should be a list"
            assert isinstance(data["count"], int), "count should be an integer"
            assert data["count"] == len(data["thresholds"]), "count should match array length"
            
            # Verify note about avg_popularity usage
            assert "avg_popularity" in data["note"], \
                "Note should explain avg_popularity usage"
            
            print(f"  Found {data['count']} genre thresholds")
            
            # Verify each threshold structure
            if data["thresholds"]:
                self._verify_genre_threshold_structure(data["thresholds"][0])
                print(f"  Sample: {data['thresholds'][0]['genre']} - " +
                      f"threshold={data['thresholds'][0]['threshold_used_in_calculation']:.2f}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_thresholds_specific_genre(self):
        """Test getting threshold for specific genre"""
        try:
            genre = "Horror"
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/thresholds",
                params={"genre": genre},
                timeout=10
            )
            
            assert response.status_code == 200, \
                f"Expected 200, got {response.status_code}"
            
            data = response.json()
            print(f"\n✓ Successfully retrieved threshold for {genre}")
            
            # Verify response structure for single threshold
            assert "dimension" in data
            assert data["dimension"] == "genre"
            assert "value" in data
            assert data["value"] == genre
            assert "threshold_used_in_calculation" in data
            assert "avg_popularity" in data
            assert "viral_threshold" in data
            assert "movie_count" in data
            assert "note" in data
            
            # Verify threshold values are positive
            assert data["threshold_used_in_calculation"] > 0
            assert data["avg_popularity"] > 0
            assert data["viral_threshold"] > 0
            
            # Verify threshold_used_in_calculation equals avg_popularity
            assert data["threshold_used_in_calculation"] == data["avg_popularity"], \
                "threshold_used_in_calculation should equal avg_popularity"
            
            print(f"  Threshold: {data['threshold_used_in_calculation']:.2f}")
            print(f"  Movie count: {data['movie_count']}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_thresholds_invalid_genre(self):
        """Test getting threshold for non-existent genre"""
        try:
            genre = "NonExistentGenre12345"
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/thresholds",
                params={"genre": genre},
                timeout=10
            )
            
            assert response.status_code == 404, \
                f"Expected 404 for invalid genre, got {response.status_code}"
            
            print(f"\n✓ Correctly returns 404 for invalid genre")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_thresholds_all_have_required_fields(self):
        """Test that all thresholds have required fields"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/thresholds",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify each threshold has required fields
            for threshold in data["thresholds"]:
                assert "genre" in threshold
                assert isinstance(threshold["genre"], str)
                assert len(threshold["genre"]) > 0
                
                assert "threshold_used_in_calculation" in threshold
                assert threshold["threshold_used_in_calculation"] > 0
                
                assert "avg_popularity" in threshold
                assert threshold["avg_popularity"] > 0
                
                assert "viral_threshold" in threshold
                assert threshold["viral_threshold"] > 0
                
                assert "movie_count" in threshold
                assert threshold["movie_count"] > 0
                
                # Verify consistency
                assert threshold["threshold_used_in_calculation"] == threshold["avg_popularity"]
            
            print(f"\n✓ All {len(data['thresholds'])} thresholds have valid data")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_thresholds_sorted_by_genre(self):
        """Test that thresholds are sorted alphabetically by genre"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/thresholds",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Extract genres
            genres = [t["genre"] for t in data["thresholds"]]
            
            # Verify alphabetical order
            sorted_genres = sorted(genres)
            assert genres == sorted_genres, \
                "Genres should be sorted alphabetically"
            
            print(f"\n✓ Thresholds are sorted alphabetically")
            print(f"  Genres: {', '.join(genres[:5])}...")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_thresholds_consistency_with_trending(self):
        """Test that thresholds match those used in trending endpoint"""
        try:
            # Get all thresholds
            thresholds_response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/thresholds",
                timeout=10
            )
            assert thresholds_response.status_code == 200
            thresholds_data = thresholds_response.json()
            
            # Get trending movies
            trending_response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/trending?limit=10",
                timeout=10
            )
            assert trending_response.status_code == 200
            trending_data = trending_response.json()
            
            # Build threshold lookup by genre
            threshold_by_genre = {
                t["genre"]: t["threshold_used_in_calculation"]
                for t in thresholds_data["thresholds"]
            }
            
            # Verify trending movies use correct thresholds
            for movie in trending_data["movies"]:
                genre = movie["movie_intelligence"]["genre"]
                threshold_used = movie["threshold_context"]["threshold_used"]
                
                if movie["threshold_context"]["threshold_dimension"] == "genre":
                    # Should match genre threshold from thresholds endpoint
                    expected_threshold = threshold_by_genre.get(genre)
                    if expected_threshold:
                        assert abs(threshold_used - expected_threshold) < 0.001, \
                            f"Threshold mismatch for {genre}: {threshold_used} vs {expected_threshold}"
            
            print(f"\n✓ Thresholds consistent between endpoints")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_thresholds_data_quality(self):
        """Test threshold data quality and reasonable values"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/viral-detection/thresholds",
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            for threshold in data["thresholds"]:
                # avg_popularity should be in reasonable range (based on TMDB data)
                avg_pop = threshold["avg_popularity"]
                assert 0 < avg_pop < 1000, \
                    f"avg_popularity {avg_pop} out of reasonable range for {threshold['genre']}"
                
                # viral_threshold should be higher than avg_popularity (99th percentile)
                viral_thresh = threshold["viral_threshold"]
                assert viral_thresh > 0, \
                    f"viral_threshold should be positive for {threshold['genre']}"
                
                # movie_count should be reasonable
                count = threshold["movie_count"]
                assert count > 0, \
                    f"movie_count should be positive for {threshold['genre']}"
            
            print(f"\n✓ All thresholds have reasonable values")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def _verify_genre_threshold_structure(self, threshold: dict):
        """Helper to verify structure of a genre threshold"""
        assert "genre" in threshold
        assert isinstance(threshold["genre"], str)
        
        assert "threshold_used_in_calculation" in threshold
        assert isinstance(threshold["threshold_used_in_calculation"], (int, float))
        
        assert "avg_popularity" in threshold
        assert isinstance(threshold["avg_popularity"], (int, float))
        
        assert "viral_threshold" in threshold
        assert isinstance(threshold["viral_threshold"], (int, float))
        
        assert "movie_count" in threshold
        assert isinstance(threshold["movie_count"], int)


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])

