"""
Recommendation API Tests - Simple Integration Tests

Tests for Goal #3: Content Recommendation Optimization
These tests call the actual API and verify basic functionality
"""
import pytest
import requests
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from api.routes.recommendations import calculate_recency_weight, normalize_scores

# API base URL - can be overridden with environment variable
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


class TestUtilityFunctions:
    """Test standalone utility functions"""
    
    def test_calculate_recency_weight_24h(self):
        """Test recency weight for 24 hours"""
        assert calculate_recency_weight(12) == 1.0
        assert calculate_recency_weight(24) == 1.0
    
    def test_calculate_recency_weight_48h(self):
        """Test recency weight for 48 hours"""
        assert calculate_recency_weight(36) == 0.8
        assert calculate_recency_weight(48) == 0.8
    
    def test_calculate_recency_weight_7d(self):
        """Test recency weight for 7 days"""
        assert calculate_recency_weight(100) == 0.6
        assert calculate_recency_weight(168) == 0.6
    
    def test_calculate_recency_weight_30d(self):
        """Test recency weight for 30 days"""
        assert calculate_recency_weight(500) == 0.4
        assert calculate_recency_weight(720) == 0.4
    
    def test_calculate_recency_weight_old(self):
        """Test recency weight for old content"""
        assert calculate_recency_weight(1000) == 0.2
    
    def test_normalize_scores_empty(self):
        """Test normalization with empty list"""
        assert normalize_scores([]) == []
    
    def test_normalize_scores_single(self):
        """Test normalization with single value"""
        assert normalize_scores([5.0]) == [50.0]
    
    def test_normalize_scores_uniform(self):
        """Test normalization with all same values"""
        result = normalize_scores([3.0, 3.0, 3.0])
        assert all(v == 50.0 for v in result)
    
    def test_normalize_scores_range(self):
        """Test normalization with range of values"""
        result = normalize_scores([0, 50, 100])
        assert result[0] == 0.0
        assert result[1] == 50.0
        assert result[2] == 100.0


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


class TestDualSuccessEndpoint:
    """Test GET /recommendations/dual-success"""
    
    def test_dual_success_endpoint_returns_200(self):
        """Test that dual-success endpoint returns valid response"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success",
                params={"limit": 10},
                timeout=10
            )
            
            # Should return 200
            assert response.status_code == 200, \
                f"Unexpected status code: {response.status_code}"
            
            data = response.json()
            print(f"\n✓ Dual-success recommendations retrieved successfully")
            
            # Verify response structure
            assert "recommendations" in data, "Missing recommendations field"
            assert data["recommendations"] is not None, "recommendations is None"
            assert isinstance(data["recommendations"], list), "recommendations not a list"
            
            assert "total_count" in data, "Missing total_count field"
            assert data["total_count"] is not None, "total_count is None"
            assert isinstance(data["total_count"], int), "total_count not an integer"
            
            assert "filters_applied" in data, "Missing filters_applied field"
            assert data["filters_applied"] is not None, "filters_applied is None"
            
            assert "timestamp" in data, "Missing timestamp field"
            assert data["timestamp"] is not None, "timestamp is None"
            
            print(f"  Total recommendations: {data['total_count']}")
            
            # If there are recommendations, check their structure
            if data["total_count"] > 0:
                rec = data["recommendations"][0]
                
                # Required fields
                assert "rank" in rec, "Missing rank field"
                assert rec["rank"] is not None, "rank is None"
                assert rec["rank"] == 1, "First recommendation should have rank 1"
                
                assert "movie_id" in rec, "Missing movie_id field"
                assert rec["movie_id"] is not None, "movie_id is None"
                
                assert "movie_title" in rec, "Missing movie_title field"
                assert rec["movie_title"] is not None, "movie_title is None"
                assert len(rec["movie_title"]) > 0, "movie_title is empty"
                
                assert "dual_success_score" in rec, "Missing dual_success_score field"
                assert rec["dual_success_score"] is not None, "dual_success_score is None"
                assert 0 <= rec["dual_success_score"] <= 100, "dual_success_score out of range"
                
                assert "reddit_buzz_score" in rec, "Missing reddit_buzz_score field"
                assert rec["reddit_buzz_score"] is not None, "reddit_buzz_score is None"
                assert 0 <= rec["reddit_buzz_score"] <= 100, "reddit_buzz_score out of range"
                
                assert "tmdb_score" in rec, "Missing tmdb_score field"
                assert rec["tmdb_score"] is not None, "tmdb_score is None"
                assert 0 <= rec["tmdb_score"] <= 100, "tmdb_score out of range"
                
                assert "vote_average" in rec, "Missing vote_average field"
                assert rec["vote_average"] is not None, "vote_average is None"
                assert 0 <= rec["vote_average"] <= 10, "vote_average out of range"
                
                assert "vote_count" in rec, "Missing vote_count field"
                assert rec["vote_count"] is not None, "vote_count is None"
                assert rec["vote_count"] >= 0, "vote_count cannot be negative"
                
                assert "popularity" in rec, "Missing popularity field"
                assert rec["popularity"] is not None, "popularity is None"
                assert rec["popularity"] >= 0, "popularity cannot be negative"
                
                assert "reddit_mentions" in rec, "Missing reddit_mentions field"
                assert rec["reddit_mentions"] is not None, "reddit_mentions is None"
                assert rec["reddit_mentions"] >= 0, "reddit_mentions cannot be negative"
                
                assert "speed_layer_contribution" in rec, "Missing speed_layer_contribution field"
                assert rec["speed_layer_contribution"] is not None, "speed_layer_contribution is None"
                assert isinstance(rec["speed_layer_contribution"], bool), "speed_layer_contribution not boolean"
                
                # Verify dual-success formula: D = 0.6 * Reddit + 0.4 * TMDB
                expected_dual = 0.6 * rec["reddit_buzz_score"] + 0.4 * rec["tmdb_score"]
                assert abs(rec["dual_success_score"] - expected_dual) < 0.2, \
                    f"Dual-success score mismatch: expected {expected_dual:.1f}, got {rec['dual_success_score']}"
                
                print(f"  First recommendation: {rec['movie_title']}")
                print(f"    Dual-Success Score: {rec['dual_success_score']:.1f}")
                print(f"    Reddit Buzz: {rec['reddit_buzz_score']:.1f}")
                print(f"    TMDB Score: {rec['tmdb_score']:.1f}")
                print(f"    Speed Layer: {rec['speed_layer_contribution']}")
            else:
                print("  No recommendations found (this is okay if database is empty)")
                
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_dual_success_with_genre_filter(self):
        """Test dual-success with genre filter"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success",
                params={"genre": "Action", "limit": 10},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check filter was applied
            assert data["filters_applied"]["genre"] == "Action"
            
            print(f"\n✓ Genre filter working: {data['total_count']} Action recommendations")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_dual_success_with_min_rating(self):
        """Test dual-success with minimum rating filter"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success",
                params={"min_rating": 7.5, "limit": 10},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check filter was applied
            assert data["filters_applied"]["min_rating"] == 7.5
            
            # All recommendations should meet minimum rating
            for rec in data["recommendations"]:
                assert rec["vote_average"] >= 7.5, \
                    f"Movie {rec['movie_title']} has rating {rec['vote_average']}, below min 7.5"
            
            print(f"\n✓ Min rating filter working: {data['total_count']} movies with rating >= 7.5")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_dual_success_with_limit(self):
        """Test limit parameter"""
        try:
            limit = 5
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success",
                params={"limit": limit},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Should not exceed limit
            assert len(data["recommendations"]) <= limit, \
                f"Returned {len(data['recommendations'])} recommendations, exceeds limit of {limit}"
            
            print(f"\n✓ Limit working: requested {limit}, got {len(data['recommendations'])}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_dual_success_sorted_by_score(self):
        """Test that recommendations are sorted by dual-success score"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success",
                params={"limit": 20},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # If there are multiple recommendations, check sorting
            if data["total_count"] > 1:
                scores = [rec["dual_success_score"] for rec in data["recommendations"]]
                assert scores == sorted(scores, reverse=True), \
                    "Recommendations should be sorted by dual-success score (descending)"
                
                # Check ranks are sequential
                for i, rec in enumerate(data["recommendations"]):
                    assert rec["rank"] == i + 1, f"Rank mismatch at position {i}"
                
                print(f"\n✓ Sorting verified: scores range from {scores[0]:.1f} to {scores[-1]:.1f}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_dual_success_genre_endpoint(self):
        """Test genre-specific endpoint /recommendations/dual-success/genre/{genre}"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success/genre/Horror",
                params={"limit": 10},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Should have genre filter applied
            assert data["filters_applied"]["genre"] == "Horror"
            
            print(f"\n✓ Genre endpoint working: {data['total_count']} Horror recommendations")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_dual_success_genre_all_endpoint(self):
        """Test genre endpoint with 'All' to get recommendations from all genres"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success/genre/All",
                params={"limit": 20},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Should have no genre filter (None)
            assert data["filters_applied"]["genre"] is None
            
            # Should have movies from multiple genres
            if data["recommendations"]:
                genres = set(rec["genre"] for rec in data["recommendations"])
                assert len(genres) > 1, "Expected movies from multiple genres when using 'All'"
                print(f"\n✓ Genre 'All' endpoint working: {data['total_count']} recommendations from {len(genres)} genres: {', '.join(sorted(genres))}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestEndpointValidation:
    """Test endpoint parameter validation"""
    
    def test_invalid_min_rating_low(self):
        """Test min_rating below 0"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success",
                params={"min_rating": -1},
                timeout=5
            )
            assert response.status_code == 422
            print("\n✓ Correctly rejects min_rating < 0")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_invalid_min_rating_high(self):
        """Test min_rating above 10"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success",
                params={"min_rating": 11},
                timeout=5
            )
            assert response.status_code == 422
            print("\n✓ Correctly rejects min_rating > 10")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_invalid_limit_low(self):
        """Test limit below 1"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success",
                params={"limit": 0},
                timeout=5
            )
            assert response.status_code == 422
            print("\n✓ Correctly rejects limit < 1")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_invalid_limit_high(self):
        """Test limit above 100"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/dual-success",
                params={"limit": 101},
                timeout=5
            )
            assert response.status_code == 422
            print("\n✓ Correctly rejects limit > 100")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestSimilarMoviesEndpoint:
    """Test GET /recommendations/similar endpoints"""
    
    def test_similar_movies_single_id(self):
        """Test similar movies with single movie ID"""
        try:
            # Use a popular movie ID (19995 = Avatar)
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/similar/19995",
                params={"limit": 5},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Verify response structure
            assert "input_movies" in data
            assert len(data["input_movies"]) == 1
            assert data["input_movies"][0]["movie_id"] == 19995
            
            assert "strategy" in data
            assert data["strategy"] == "average"
            
            assert "similar_movies" in data
            assert "total_count" in data
            
            if data["total_count"] > 0:
                sim = data["similar_movies"][0]
                
                assert "rank" in sim and sim["rank"] == 1
                assert "movie_id" in sim
                assert "movie_title" in sim
                assert "similarity_score" in sim
                assert 0 <= sim["similarity_score"] <= 1.2  # With sentiment boost
                assert "shared_genre" in sim
                assert "release_year_diff" in sim
                assert "popularity" in sim
                assert "vote_average" in sim
                assert "vote_count" in sim
                
                print(f"\n✓ Similar movies for Avatar:")
                for i, movie in enumerate(data["similar_movies"][:3]):
                    print(f"  {i+1}. {movie['movie_title']} (similarity: {movie['similarity_score']:.3f})")
            else:
                print("\n✓ No similar movies found (database may be limited)")
                
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_similar_movies_multiple_ids(self):
        """Test similar movies with multiple movie IDs"""
        try:
            # Use multiple movie IDs
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/similar",
                params={"ids": "19995,278", "limit": 5, "strategy": "average"},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            assert "input_movies" in data
            assert len(data["input_movies"]) == 2
            
            assert "strategy" in data
            assert data["strategy"] == "average"
            
            if data["total_count"] > 0:
                # Check matched_with field for multi-movie input
                sim = data["similar_movies"][0]
                assert "matched_with" in sim
                if sim["matched_with"] is not None:
                    assert isinstance(sim["matched_with"], int)
                    assert sim["matched_with"] >= 0
                
                print(f"\n✓ Similar movies for multiple inputs (strategy: average):")
                print(f"  Found {data['total_count']} recommendations")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_similar_movies_union_strategy(self):
        """Test similar movies with union strategy"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/similar",
                params={"ids": "19995,550", "limit": 5, "strategy": "union"},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data["strategy"] == "union"
            
            print(f"\n✓ Union strategy working: {data['total_count']} diverse recommendations")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_similar_movies_intersection_strategy(self):
        """Test similar movies with intersection strategy"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/similar",
                params={"ids": "19995,550", "limit": 5, "strategy": "intersection"},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data["strategy"] == "intersection"
            
            print(f"\n✓ Intersection strategy working: {data['total_count']} focused recommendations")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_similar_movies_invalid_id(self):
        """Test with invalid movie ID"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/similar/9999999",
                timeout=5
            )
            assert response.status_code == 404
            print("\n✓ Correctly returns 404 for invalid movie ID")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_similar_movies_invalid_strategy(self):
        """Test with invalid strategy"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/similar",
                params={"ids": "19995", "strategy": "invalid"},
                timeout=5
            )
            assert response.status_code == 422
            print("\n✓ Correctly rejects invalid strategy")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_similar_movies_sorted_by_similarity(self):
        """Test that results are sorted by similarity score"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/similar/19995",
                params={"limit": 10},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            if data["total_count"] > 1:
                scores = [m["similarity_score"] for m in data["similar_movies"]]
                assert scores == sorted(scores, reverse=True), \
                    "Similar movies should be sorted by similarity score (descending)"
                
                print(f"\n✓ Sorting verified: scores range from {scores[0]:.3f} to {scores[-1]:.3f}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestRedditBuzzEndpoint:
    """Test GET /recommendations/reddit-buzz"""
    
    def test_reddit_buzz_endpoint_returns_200(self):
        """Test that Reddit buzz endpoint returns valid response"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/reddit-buzz",
                params={"limit": 10},
                timeout=10
            )
            
            assert response.status_code == 200, \
                f"Unexpected status code: {response.status_code}"
            
            data = response.json()
            print(f"\n✓ Reddit buzz recommendations retrieved successfully")
            
            # Verify response structure
            assert "recommendations" in data, "Missing recommendations field"
            assert data["recommendations"] is not None, "recommendations is None"
            assert isinstance(data["recommendations"], list), "recommendations not a list"
            
            assert "total_count" in data, "Missing total_count field"
            assert data["total_count"] is not None, "total_count is None"
            assert isinstance(data["total_count"], int), "total_count not an integer"
            
            assert "filters_applied" in data, "Missing filters_applied field"
            assert data["filters_applied"] is not None, "filters_applied is None"
            
            assert "timestamp" in data, "Missing timestamp field"
            assert data["timestamp"] is not None, "timestamp is None"
            
            print(f"  Total recommendations: {data['total_count']}")
            
            # If there are recommendations, check their structure
            if data["total_count"] > 0:
                rec = data["recommendations"][0]
                
                assert "rank" in rec, "Missing rank field"
                assert rec["rank"] == 1, "First recommendation should have rank 1"
                
                assert "movie_title" in rec, "Missing movie_title field"
                assert rec["movie_title"] is not None, "movie_title is None"
                assert len(rec["movie_title"]) > 0, "movie_title is empty"
                
                assert "reddit_buzz_score" in rec, "Missing reddit_buzz_score field"
                assert rec["reddit_buzz_score"] is not None, "reddit_buzz_score is None"
                assert rec["reddit_buzz_score"] > 0, "reddit_buzz_score should be positive"
                
                assert "total_engagement" in rec, "Missing total_engagement field"
                assert rec["total_engagement"] is not None, "total_engagement is None"
                assert rec["total_engagement"] >= 0, "total_engagement cannot be negative"
                
                assert "reddit_mentions" in rec, "Missing reddit_mentions field"
                assert rec["reddit_mentions"] is not None, "reddit_mentions is None"
                assert rec["reddit_mentions"] >= 0, "reddit_mentions cannot be negative"
                
                print(f"  First recommendation: {rec['movie_title']}")
                print(f"    Reddit Buzz Score: {rec['reddit_buzz_score']:.1f}")
                print(f"    Total Engagement: {rec['total_engagement']}")
                print(f"    Reddit Mentions: {rec['reddit_mentions']}")
            else:
                print("  No recommendations found (this is okay if no recent Reddit data)")
                
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_reddit_buzz_with_genre_filter(self):
        """Test Reddit buzz with genre filter"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/reddit-buzz",
                params={"genre": "Action", "limit": 10},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check filter was applied
            assert data["filters_applied"]["genre"] == "Action"
            
            print(f"\n✓ Genre filter working: {data['total_count']} Action Reddit buzz recommendations")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_reddit_buzz_with_days_back(self):
        """Test Reddit buzz with custom days_back parameter"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/reddit-buzz",
                params={"days_back": 3, "limit": 10},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check filter was applied
            assert data["filters_applied"]["days_back"] == 3
            
            print(f"\n✓ Days back filter working: {data['total_count']} recommendations from last 3 days")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_reddit_buzz_sorted_by_score(self):
        """Test that recommendations are sorted by Reddit buzz score"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/reddit-buzz",
                params={"limit": 20},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # If there are multiple recommendations, check sorting
            if data["total_count"] > 1:
                scores = [rec["reddit_buzz_score"] for rec in data["recommendations"]]
                assert scores == sorted(scores, reverse=True), \
                    "Recommendations should be sorted by Reddit buzz score (descending)"
                
                # Check ranks are sequential
                for i, rec in enumerate(data["recommendations"]):
                    assert rec["rank"] == i + 1, f"Rank mismatch at position {i}"
                
                print(f"\n✓ Sorting verified: scores range from {scores[0]:.1f} to {scores[-1]:.1f}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_reddit_buzz_with_limit(self):
        """Test limit parameter"""
        try:
            limit = 5
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/reddit-buzz",
                params={"limit": limit},
                timeout=10
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Should not exceed limit
            assert len(data["recommendations"]) <= limit, \
                f"Returned {len(data['recommendations'])} recommendations, exceeds limit of {limit}"
            
            print(f"\n✓ Limit working: requested {limit}, got {len(data['recommendations'])}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")
    
    def test_reddit_buzz_invalid_days_back(self):
        """Test invalid days_back parameter"""
        try:
            # Test days_back > 30
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/reddit-buzz",
                params={"days_back": 31},
                timeout=5
            )
            assert response.status_code == 422
            
            # Test days_back < 1
            response = requests.get(
                f"{API_BASE_URL}/api/v1/recommendations/reddit-buzz",
                params={"days_back": 0},
                timeout=5
            )
            assert response.status_code == 422
            
            print("\n✓ Correctly rejects invalid days_back values")
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running")


class TestTMDBQualityEndpoint:
    """Tests for TMDB Quality Recommendations Endpoint"""
    
    def test_tmdb_quality_endpoint_returns_200(self):
        """Test basic TMDB quality endpoint"""
        response = requests.get(f"{API_BASE_URL}/api/v1/recommendations/tmdb-quality?limit=10")
        assert response.status_code == 200
        
        data = response.json()
        assert "recommendations" in data
        assert "total_count" in data
        assert "filters_applied" in data
        
        if data["total_count"] > 0:
            print(f"\n✓ TMDB quality recommendations retrieved successfully")
            print(f"  Total recommendations: {data['total_count']}")
            
            first = data["recommendations"][0]
            print(f"  First recommendation: {first['movie_title']}")
            print(f"    TMDB Quality Score: {first['tmdb_quality_score']}")
            print(f"    Weighted Rating: {first['weighted_rating']}")
            print(f"    Popularity Factor: {first['popularity_factor']}")
            print(f"    Vote Average: {first['vote_average']}")
            print(f"    Vote Count: {first['vote_count']}")
    
    def test_tmdb_quality_with_genre_filter(self):
        """Test TMDB quality with genre filter"""
        response = requests.get(f"{API_BASE_URL}/api/v1/recommendations/tmdb-quality?genre=Drama&limit=10")
        assert response.status_code == 200
        
        data = response.json()
        assert data["filters_applied"]["genre"] == "Drama"
        
        if data["total_count"] > 0:
            # Verify all are Drama genre
            for rec in data["recommendations"]:
                if rec["genre"]:  # Only check if genre is not None
                    assert rec["genre"] == "Drama", f"Expected Drama, got {rec['genre']}"
            
            print(f"\n✓ Genre filter working: {data['total_count']} Drama TMDB quality recommendations")
    
    def test_tmdb_quality_with_min_vote_count(self):
        """Test TMDB quality with custom min_vote_count"""
        response = requests.get(f"{API_BASE_URL}/api/v1/recommendations/tmdb-quality?min_vote_count=500&limit=10")
        assert response.status_code == 200
        
        data = response.json()
        assert data["filters_applied"]["min_vote_count"] == 500
        
        if data["total_count"] > 0:
            # Verify all have vote_count >= 500
            for rec in data["recommendations"]:
                assert rec["vote_count"] >= 500, f"Expected vote_count >= 500, got {rec['vote_count']}"
            
            print(f"\n✓ Min vote count filter working: {data['total_count']} recommendations with vote_count >= 500")
    
    def test_tmdb_quality_sorted_by_score(self):
        """Test that results are sorted by TMDB quality score descending"""
        response = requests.get(f"{API_BASE_URL}/api/v1/recommendations/tmdb-quality?limit=20")
        assert response.status_code == 200
        
        data = response.json()
        
        if data["total_count"] >= 2:
            scores = [rec["tmdb_quality_score"] for rec in data["recommendations"]]
            
            # Check descending order
            for i in range(len(scores) - 1):
                assert scores[i] >= scores[i+1], f"Scores not in descending order: {scores[i]} < {scores[i+1]}"
            
            print(f"\n✓ Sorting verified: scores range from {scores[0]} to {scores[-1]}")
    
    def test_tmdb_quality_with_limit(self):
        """Test limit parameter"""
        response = requests.get(f"{API_BASE_URL}/api/v1/recommendations/tmdb-quality?limit=5")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data["recommendations"]) <= 5
        
        print(f"\n✓ Limit working: requested 5, got {len(data['recommendations'])}")
    
    def test_tmdb_quality_formula_validation(self):
        """Test that TMDB quality formula components are correct"""
        response = requests.get(f"{API_BASE_URL}/api/v1/recommendations/tmdb-quality?limit=5")
        assert response.status_code == 200
        
        data = response.json()
        
        if data["total_count"] > 0:
            rec = data["recommendations"][0]
            
            # Verify weighted_rating is between 0-10
            assert 0 <= rec["weighted_rating"] <= 10, f"Weighted rating out of range: {rec['weighted_rating']}"
            
            # Verify popularity_factor is between 0-1
            assert 0 <= rec["popularity_factor"] <= 1, f"Popularity factor out of range: {rec['popularity_factor']}"
            
            # Verify tmdb_quality_score is positive
            assert rec["tmdb_quality_score"] > 0, "TMDB quality score should be positive"
            
            print(f"\n✓ Formula validation passed for {rec['movie_title']}")
            print(f"  Weighted Rating: {rec['weighted_rating']} (should be 0-10)")
            print(f"  Popularity Factor: {rec['popularity_factor']} (should be 0-1)")
            print(f"  TMDB Quality Score: {rec['tmdb_quality_score']}")
    
    def test_tmdb_quality_invalid_min_vote_count(self):
        """Test invalid min_vote_count values are rejected"""
        # Test negative value
        response = requests.get(f"{API_BASE_URL}/api/v1/recommendations/tmdb-quality?min_vote_count=-1")
        assert response.status_code == 422
        
        print("\n✓ Correctly rejects invalid min_vote_count values")



if __name__ == "__main__":
    print("\n" + "="*60)
    print("Recommendation API Integration Tests")
    print("="*60)
    print(f"Testing API at: {API_BASE_URL}")
    print("="*60 + "\n")
    
    pytest.main([__file__, "-v", "-s"])


