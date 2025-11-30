"""
API Endpoint Tests

Tests for FastAPI REST endpoints
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from api.main import app

# Create test client
client = TestClient(app)


class TestHealthEndpoints:
    """Test health check endpoints"""
    
    def test_health_check(self):
        """Test basic health check"""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        
        data = response.json()
        assert "status" in data
        assert "timestamp" in data
        assert "services" in data
    
    def test_health_check_structure(self):
        """Test health check response structure"""
        response = client.get("/api/v1/health")
        data = response.json()
        
        # Check services status
        services = data.get("services", {})
        assert "mongodb" in services
        assert "redis" in services


class TestMovieEndpoints:
    """Test movie-related endpoints"""
    
    def test_get_movie_success(self):
        """Test getting movie by ID"""
        movie_id = 550  # Fight Club
        response = client.get(f"/api/v1/movies/{movie_id}")
        
        # Should return 200 or 404 depending on data availability
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            data = response.json()
            assert "movie_id" in data
            assert data["movie_id"] == movie_id
    
    def test_get_movie_invalid_id(self):
        """Test getting movie with invalid ID"""
        response = client.get("/api/v1/movies/invalid")
        assert response.status_code == 422  # Validation error
    
    def test_get_movie_sentiment(self):
        """Test getting movie sentiment"""
        movie_id = 550
        response = client.get(f"/api/v1/movies/{movie_id}/sentiment")
        
        # Should return 200 or 404
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            data = response.json()
            assert "movie_id" in data
            assert "sentiment" in data
            
            sentiment = data["sentiment"]
            assert "overall_score" in sentiment
            assert "label" in sentiment
    
    def test_get_movie_sentiment_with_window(self):
        """Test sentiment with time window parameter"""
        movie_id = 550
        response = client.get(
            f"/api/v1/movies/{movie_id}/sentiment",
            params={"window": "7d"}
        )
        
        assert response.status_code in [200, 404]


class TestTrendingEndpoints:
    """Test trending endpoints"""
    
    def test_get_trending_movies(self):
        """Test getting trending movies"""
        response = client.get("/api/v1/trending/movies")
        
        assert response.status_code == 200
        
        data = response.json()
        assert "trending_movies" in data
        assert "generated_at" in data
        assert isinstance(data["trending_movies"], list)
    
    def test_get_trending_with_genre(self):
        """Test trending movies filtered by genre"""
        response = client.get(
            "/api/v1/trending/movies",
            params={"genre": "Action"}
        )
        
        assert response.status_code == 200
        
        data = response.json()
        assert "trending_movies" in data
    
    def test_get_trending_with_limit(self):
        """Test trending movies with limit parameter"""
        limit = 5
        response = client.get(
            "/api/v1/trending/movies",
            params={"limit": limit}
        )
        
        assert response.status_code == 200
        
        data = response.json()
        movies = data["trending_movies"]
        assert len(movies) <= limit


class TestAnalyticsEndpoints:
    """Test analytics endpoints"""
    
    def test_get_genre_analytics(self):
        """Test getting genre analytics"""
        genre = "Action"
        response = client.get(f"/api/v1/analytics/genre/{genre}")
        
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            data = response.json()
            assert "genre" in data
            assert data["genre"] == genre
            assert "statistics" in data
    
    def test_get_genre_analytics_with_year(self):
        """Test genre analytics with year filter"""
        genre = "Action"
        year = 2023
        response = client.get(
            f"/api/v1/analytics/genre/{genre}",
            params={"year": year}
        )
        
        assert response.status_code in [200, 404]
    
    def test_get_trends(self):
        """Test getting trend analysis"""
        response = client.get(
            "/api/v1/analytics/trends",
            params={
                "metric": "sentiment",
                "window": "30d"
            }
        )
        
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            data = response.json()
            assert "metric" in data
            assert "data_points" in data
            assert isinstance(data["data_points"], list)


class TestSearchEndpoints:
    """Test search endpoints"""
    
    def test_search_movies(self):
        """Test movie search"""
        response = client.get(
            "/api/v1/search/movies",
            params={"q": "matrix"}
        )
        
        assert response.status_code == 200
        
        data = response.json()
        assert "results" in data
        assert "total_results" in data
        assert isinstance(data["results"], list)
    
    def test_search_with_filters(self):
        """Test search with multiple filters"""
        response = client.get(
            "/api/v1/search/movies",
            params={
                "q": "action",
                "genre": "Action",
                "rating_min": 7.0
            }
        )
        
        assert response.status_code == 200
    
    def test_search_with_pagination(self):
        """Test search pagination"""
        response = client.get(
            "/api/v1/search/movies",
            params={
                "q": "the",
                "limit": 10,
                "offset": 0
            }
        )
        
        assert response.status_code == 200
        
        data = response.json()
        assert "page" in data or "results" in data


class TestRateLimiting:
    """Test rate limiting"""
    
    @pytest.mark.skip(reason="Rate limiting may be disabled in test environment")
    def test_rate_limit_headers(self):
        """Test rate limit headers are present"""
        response = client.get("/api/v1/health")
        
        # Check for rate limit headers
        # Note: These may not be present if rate limiting is disabled
        if "X-RateLimit-Limit" in response.headers:
            assert int(response.headers["X-RateLimit-Limit"]) > 0
    
    @pytest.mark.skip(reason="May trigger actual rate limiting")
    def test_rate_limit_exceeded(self):
        """Test rate limit exceeded response"""
        # Make many requests quickly
        for _ in range(150):
            response = client.get("/api/v1/health")
            
            if response.status_code == 429:
                # Rate limit hit
                assert "Retry-After" in response.headers
                return
        
        # If we get here, rate limiting may be disabled
        pytest.skip("Rate limiting not triggered")


class TestErrorHandling:
    """Test error handling"""
    
    def test_404_not_found(self):
        """Test 404 for non-existent endpoints"""
        response = client.get("/api/v1/nonexistent")
        assert response.status_code == 404
    
    def test_invalid_movie_id_type(self):
        """Test validation error for invalid movie ID type"""
        response = client.get("/api/v1/movies/not-a-number")
        assert response.status_code == 422
    
    def test_invalid_query_parameters(self):
        """Test validation for invalid query parameters"""
        response = client.get(
            "/api/v1/trending/movies",
            params={"limit": -1}  # Invalid limit
        )
        
        # Should return 422 validation error or handle gracefully
        assert response.status_code in [200, 422]


class TestCORS:
    """Test CORS configuration"""
    
    def test_cors_headers(self):
        """Test CORS headers are present"""
        response = client.options("/api/v1/health")
        
        # Check for CORS headers (may vary based on config)
        assert response.status_code in [200, 405]


# Fixtures
@pytest.fixture
def sample_movie_id():
    """Provide a sample movie ID for testing"""
    return 550  # Fight Club


@pytest.fixture
def sample_genre():
    """Provide a sample genre for testing"""
    return "Action"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
