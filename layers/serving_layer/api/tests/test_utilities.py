"""
Utilities API Tests - Simple Integration Tests

Tests for utility/support endpoints
"""

import pytest
import requests
import os

# API base URL - can be overridden with environment variable
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


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


class TestGenresEndpoint:
    """Test GET /utilities/genres"""
    
    def test_genres_endpoint_returns_200(self):
        """Test that genres endpoint returns valid response"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/utilities/genres",
                timeout=10
            )
            
            assert response.status_code == 200, \
                f"Expected 200, got {response.status_code}: {response.text}"
            
            data = response.json()
            print(f"\n✓ Successfully retrieved genres list")
            
            # Verify response structure
            assert "genres" in data, "Missing 'genres' field in response"
            assert isinstance(data["genres"], list), "genres should be a list"
            assert len(data["genres"]) > 0, "genres list should not be empty"
            
            # Verify total count
            assert "total" in data, "Missing 'total' field in response"
            assert data["total"] == len(data["genres"]), "total doesn't match genres count"
            
            # Verify each genre has required fields
            for genre in data["genres"]:
                assert "name" in genre, "Genre missing 'name' field"
                assert isinstance(genre["name"], str), "Genre name should be string"
                assert len(genre["name"]) > 0, "Genre name should not be empty"
                
                assert "movie_count" in genre, "Genre missing 'movie_count' field"
                assert isinstance(genre["movie_count"], int), "movie_count should be int"
                assert genre["movie_count"] >= 0, "movie_count should be non-negative"
            
            # Print sample
            print(f"  Total genres: {data['total']}")
            print(f"  Sample genres: {[g['name'] for g in data['genres'][:5]]}")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running - start services with docker-compose up")
    
    def test_genres_are_sorted(self):
        """Test that genres are sorted by movie count"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/utilities/genres",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                genres = data["genres"]
                
                # Verify descending order by movie_count
                counts = [g["movie_count"] for g in genres]
                assert counts == sorted(counts, reverse=True), \
                    "Genres should be sorted by movie_count descending"
                
                print(f"\n✓ Genres are properly sorted by movie count")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running - start services with docker-compose up")
    
    def test_genres_no_duplicates(self):
        """Test that there are no duplicate genres"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/utilities/genres",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                genre_names = [g["name"] for g in data["genres"]]
                
                assert len(genre_names) == len(set(genre_names)), \
                    "There should be no duplicate genres"
                
                print(f"\n✓ No duplicate genres found")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running - start services with docker-compose up")


class TestSearchEndpoint:
    """Test GET /utilities/search"""
    
    def test_search_endpoint_exists(self):
        """Test that search endpoint is accessible"""
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/utilities/search?q=avengers",
                timeout=10
            )
            
            # Endpoint should exist (even if not fully implemented)
            assert response.status_code in [200, 501], \
                f"Expected 200 or 501, got {response.status_code}"
            
            if response.status_code == 200:
                print(f"\n✓ Search endpoint is implemented")
            else:
                print(f"\n⚠ Search endpoint exists but not implemented (501)")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running - start services with docker-compose up")


class TestMovieDetailsEndpoint:
    """Test GET /utilities/movies/{id}"""
    
    def test_movie_details_endpoint_exists(self):
        """Test that movie details endpoint is accessible"""
        try:
            movie_id = 550  # Fight Club
            response = requests.get(
                f"{API_BASE_URL}/api/v1/utilities/movies/{movie_id}",
                timeout=10
            )
            
            # Endpoint should exist (even if not fully implemented)
            assert response.status_code in [200, 404, 501], \
                f"Expected 200, 404, or 501, got {response.status_code}"
            
            if response.status_code == 200:
                print(f"\n✓ Movie details endpoint is implemented")
            elif response.status_code == 404:
                print(f"\n⚠ Movie details endpoint works but movie not found")
            else:
                print(f"\n⚠ Movie details endpoint exists but not implemented (501)")
            
        except requests.exceptions.ConnectionError:
            pytest.skip("API is not running - start services with docker-compose up")
