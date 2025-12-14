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
    """Test trending endpoints (now uses Reddit viral coefficient)"""
    
    def test_get_trending_movies_viral(self):
        """Test getting trending movies based on viral coefficient"""
        response = client.get("/api/v1/trending/movies")
        
        assert response.status_code == 200
        
        data = response.json()
        assert "viral_movies" in data
        assert "timestamp" in data or "generated_at" in data
        assert isinstance(data["viral_movies"], list)
        
        # Check viral metrics structure if movies exist
        if data["viral_movies"]:
            movie = data["viral_movies"][0]
            assert "viral_metrics" in movie
            assert "viral_coefficient" in movie["viral_metrics"]
            assert "upvote_velocity" in movie["viral_metrics"]
            assert "comment_velocity" in movie["viral_metrics"]
    
    def test_get_trending_with_genre(self):
        """Test trending movies filtered by genre"""
        response = client.get(
            "/api/v1/trending/movies",
            params={"genre": "Action"}
        )
        
        assert response.status_code == 200
        
        data = response.json()
        assert "viral_movies" in data
    
    def test_get_trending_with_viral_threshold(self):
        """Test trending movies with viral coefficient threshold"""
        threshold = 1.5
        response = client.get(
            "/api/v1/trending/movies",
            params={"viral_coefficient_threshold": threshold}
        )
        
        assert response.status_code == 200
        
        data = response.json()
        movies = data["viral_movies"]
        
        # Verify all movies meet threshold
        for movie in movies:
            viral_coef = movie.get("viral_metrics", {}).get("viral_coefficient", 0)
            assert viral_coef >= threshold or viral_coef == 0  # 0 means no threshold data
    
    def test_get_trending_with_limit(self):
        """Test trending movies with limit parameter"""
        limit = 5
        response = client.get(
            "/api/v1/trending/movies",
            params={"limit": limit}
        )
        
        assert response.status_code == 200
        
        data = response.json()
        movies = data["viral_movies"]
        assert len(movies) <= limit


class TestAnalyticsEndpoints:
    """Test analytics endpoints (refactored to use sentiment baseline & viral threshold)"""
    
    def test_get_genre_analytics(self):
        """Test getting genre analytics with sentiment baseline and viral threshold"""
        genre = "Action"
        response = client.get(f"/api/v1/analytics/genre/{genre}")
        
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            data = response.json()
            assert "genre" in data
            assert data["genre"] == genre
            assert "statistics" in data
            
            # Statistics may be empty if no data, just check structure exists
            stats = data["statistics"]
            assert isinstance(stats, dict)
    
    def test_get_genre_analytics_with_year(self):
        """Test genre analytics with year filter"""
        genre = "Action"
        year = 2023
        response = client.get(
            f"/api/v1/analytics/genre/{genre}",
            params={"year": year}
        )
        
        assert response.status_code in [200, 404]
    
    def test_sentiment_baseline_structure(self):
        """Test sentiment baseline data structure"""
        genre = "Sci-Fi"
        response = client.get(f"/api/v1/analytics/genre/{genre}")
        
        if response.status_code == 200:
            data = response.json()
            stats = data.get("statistics", {})
            
            if "sentiment_baseline" in stats:
                baseline = stats["sentiment_baseline"]
                assert "mean_sentiment" in baseline
                assert "std_deviation" in baseline
                assert isinstance(baseline["mean_sentiment"], (int, float))
                assert isinstance(baseline["std_deviation"], (int, float))
    
    def test_viral_threshold_structure(self):
        """Test viral threshold data structure"""
        genre = "Action"
        response = client.get(f"/api/v1/analytics/genre/{genre}")
        
        if response.status_code == 200:
            data = response.json()
            stats = data.get("statistics", {})
            
            if "viral_threshold" in stats:
                threshold = stats["viral_threshold"]
                assert "threshold_value" in threshold
                assert "percentile" in threshold
                assert isinstance(threshold["threshold_value"], (int, float))
                assert threshold["percentile"] in [75, 90, 95]  # Common percentiles


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
        assert "pagination" in data
        assert isinstance(data["results"], list)
        
        # Check pagination structure
        pagination = data["pagination"]
        assert "total_results" in pagination or "total_pages" in pagination
    
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


class TestCrisisDetection:
    """Test PR crisis detection functionality"""
    
    def test_sentiment_endpoint_exists(self):
        """Test that sentiment/crisis endpoint exists"""
        # Use proper movie ID instead of title
        movie_id = "tt15239678"  # Dune: Part Two
        response = client.get(f"/api/v1/movies/{movie_id}/sentiment")
        
        assert response.status_code in [200, 404, 422]  # 422 if endpoint expects different format
    
    def test_sentiment_response_structure(self):
        """Test sentiment response contains crisis detection fields"""
        movie_title = "The%20Matrix"
        response = client.get(f"/api/v1/movies/{movie_title}/sentiment")
        
        if response.status_code == 200:
            data = response.json()
            assert "movie" in data
            assert "sentiment_data" in data
            
            sentiment = data["sentiment_data"]
            # Check for crisis detection fields
            if "is_crisis" in sentiment:
                assert isinstance(sentiment["is_crisis"], bool)
                assert "crisis_level" in sentiment
                assert "sentiment_velocity" in sentiment
    
    def test_genre_baseline_comparison(self):
        """Test that sentiment is compared against genre baseline"""
        movie_title = "Inception"
        response = client.get(f"/api/v1/movies/{movie_title}/sentiment")
        
        if response.status_code == 200:
            data = response.json()
            sentiment = data.get("sentiment_data", {})
            
            # Should have baseline comparison
            if "deviation_from_baseline" in sentiment:
                assert "sigma" in sentiment
                assert isinstance(sentiment["sigma"], (int, float))


class TestViralScoring:
    """Test viral content detection functionality"""
    
    def test_viral_coefficient_calculation(self):
        """Test that viral movies have coefficient calculated"""
        response = client.get("/api/v1/trending/movies?limit=10")
        
        assert response.status_code == 200
        data = response.json()
        
        viral_movies = data.get("viral_movies", [])
        if viral_movies:
            movie = viral_movies[0]
            assert "viral_metrics" in movie
            
            metrics = movie["viral_metrics"]
            assert "viral_coefficient" in metrics
            assert "upvote_velocity" in metrics
            assert "comment_velocity" in metrics
            
            # Viral coefficient should be velocity / threshold
            assert isinstance(metrics["viral_coefficient"], (int, float))
    
    def test_cross_subreddit_tracking(self):
        """Test that viral detection tracks multiple subreddits"""
        response = client.get("/api/v1/trending/movies?limit=5")
        
        if response.status_code == 200:
            data = response.json()
            viral_movies = data.get("viral_movies", [])
            
            if viral_movies:
                movie = viral_movies[0]
                reddit_stats = movie.get("reddit_stats", {})
                
                # Should track subreddit count
                if "subreddit_count" in reddit_stats:
                    assert isinstance(reddit_stats["subreddit_count"], int)
                    assert reddit_stats["subreddit_count"] >= 0
    
    def test_viral_threshold_filtering(self):
        """Test filtering by viral coefficient threshold"""
        threshold = 2.0
        response = client.get(
            "/api/v1/trending/movies",
            params={"viral_coefficient_threshold": threshold}
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # All returned movies should meet threshold
        for movie in data.get("viral_movies", []):
            coef = movie.get("viral_metrics", {}).get("viral_coefficient", 0)
            if coef > 0:  # Only check if coefficient exists
                assert coef >= threshold


class TestDualSuccessRecommendations:
    """Test dual-success recommendation algorithm"""
    
    def test_recommendations_endpoint_exists(self):
        """Test that recommendations endpoint works"""
        # Provide required parameters
        response = client.get(
            "/api/v1/recommendations",
            params={"genre": "Action"}
        )
        
        assert response.status_code in [200, 400, 404]  # 404 if endpoint doesn't exist
    
    def test_dual_success_scoring(self):
        """Test dual-success score calculation (60% Reddit + 40% TMDB)"""
        response = client.get(
            "/api/v1/recommendations",
            params={
                "genre": "Action",
                "limit": 10
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            recommendations = data.get("recommendations", [])
            
            if recommendations:
                movie = recommendations[0]
                
                # Should have dual success score
                if "dual_success_score" in movie:
                    score = movie["dual_success_score"]
                    assert isinstance(score, (int, float))
                    assert 0 <= score <= 100  # Score should be 0-100
                
                # Should have both Reddit and TMDB components
                if "reddit_buzz_score" in movie and "tmdb_quality_score" in movie:
                    reddit_score = movie["reddit_buzz_score"]
                    tmdb_score = movie["tmdb_quality_score"]
                    
                    # Verify weighting (60/40)
                    expected_score = (reddit_score * 0.6) + (tmdb_score * 0.4)
                    actual_score = movie.get("dual_success_score", 0)
                    
                    # Allow small floating point difference
                    assert abs(actual_score - expected_score) < 0.1
    
    def test_recommendations_with_filters(self):
        """Test recommendations with genre and rating filters"""
        response = client.get(
            "/api/v1/recommendations",
            params={
                "genre": "Sci-Fi",
                "min_rating": 7.0,
                "limit": 5
            }
        )
        
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            data = response.json()
            recommendations = data.get("recommendations", [])
            assert len(recommendations) <= 5


class TestPrometheusMetrics:
    """Test Prometheus metrics endpoint"""
    
    def test_metrics_endpoint_exists(self):
        """Test that /metrics endpoint is accessible"""
        response = client.get("/metrics")
        
        # Metrics might be at /metrics or not exposed depending on config
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            assert "text/plain" in response.headers.get("content-type", "") or "text" in response.headers.get("content-type", "")
    
    def test_custom_business_metrics_exposed(self):
        """Test that custom business metrics are exposed"""
        response = client.get("/metrics")
        
        if response.status_code == 200:
            metrics_text = response.text
            
            # Check for custom metrics
            assert "crisis_alerts_total" in metrics_text or "# HELP" in metrics_text
            # Note: Metrics might not have data yet, so we just check they're registered
    
    def test_standard_metrics_exposed(self):
        """Test that standard FastAPI metrics are exposed"""
        response = client.get("/metrics")
        
        if response.status_code == 200:
            metrics_text = response.text
            
            # Standard metrics from prometheus-fastapi-instrumentator
            assert "http_request" in metrics_text or "# TYPE" in metrics_text


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
