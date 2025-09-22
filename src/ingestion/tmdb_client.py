"""
TMDB API client for fetching movie data.
"""
import requests
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class TMDBClient:
    """Client for interacting with The Movie Database (TMDB) API."""
    
    def __init__(self, api_key: str, base_url: str = "https://api.themoviedb.org/3"):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.rate_limit_delay = 0.25  # 4 requests per second
        
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make a request to the TMDB API with rate limiting."""
        if params is None:
            params = {}
        
        params['api_key'] = self.api_key
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            time.sleep(self.rate_limit_delay)
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error making request to {url}: {e}")
            return None
    
    def get_popular_movies(self, page: int = 1) -> Optional[Dict]:
        """Get popular movies."""
        return self._make_request("movie/popular", {"page": page})
    
    def get_top_rated_movies(self, page: int = 1) -> Optional[Dict]:
        """Get top rated movies."""
        return self._make_request("movie/top_rated", {"page": page})
    
    def get_now_playing_movies(self, page: int = 1) -> Optional[Dict]:
        """Get now playing movies."""
        return self._make_request("movie/now_playing", {"page": page})
    
    def get_upcoming_movies(self, page: int = 1) -> Optional[Dict]:
        """Get upcoming movies."""
        return self._make_request("movie/upcoming", {"page": page})
    
    def get_movie_details(self, movie_id: int) -> Optional[Dict]:
        """Get detailed information about a specific movie."""
        return self._make_request(f"movie/{movie_id}")
    
    def get_movie_credits(self, movie_id: int) -> Optional[Dict]:
        """Get cast and crew information for a movie."""
        return self._make_request(f"movie/{movie_id}/credits")
    
    def get_movie_reviews(self, movie_id: int, page: int = 1) -> Optional[Dict]:
        """Get reviews for a movie."""
        return self._make_request(f"movie/{movie_id}/reviews", {"page": page})
    
    def get_person_details(self, person_id: int) -> Optional[Dict]:
        """Get detailed information about a person."""
        return self._make_request(f"person/{person_id}")
    
    def get_person_movie_credits(self, person_id: int) -> Optional[Dict]:
        """Get movie credits for a person."""
        return self._make_request(f"person/{person_id}/movie_credits")
    
    def discover_movies(self, **kwargs) -> Optional[Dict]:
        """Discover movies with filters."""
        return self._make_request("discover/movie", kwargs)
    
    def search_movies(self, query: str, page: int = 1) -> Optional[Dict]:
        """Search for movies."""
        return self._make_request("search/movie", {"query": query, "page": page})
    
    def search_people(self, query: str, page: int = 1) -> Optional[Dict]:
        """Search for people."""
        return self._make_request("search/person", {"query": query, "page": page})
    
    def get_trending_movies(self, time_window: str = "day") -> Optional[Dict]:
        """Get trending movies (day/week)."""
        return self._make_request(f"trending/movie/{time_window}")
    
    def get_trending_people(self, time_window: str = "day") -> Optional[Dict]:
        """Get trending people (day/week)."""
        return self._make_request(f"trending/person/{time_window}")
    
    def get_movie_genres(self) -> Optional[Dict]:
        """Get list of movie genres."""
        return self._make_request("genre/movie/list")
    
    def get_configuration(self) -> Optional[Dict]:
        """Get API configuration."""
        return self._make_request("configuration")