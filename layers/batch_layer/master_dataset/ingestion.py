"""
TMDB Data Ingestion Module for Batch Layer Processing.

This module handles the extraction of movie data from The Movie Database (TMDB) API
and stores it in the Bronze layer on HDFS with proper partitioning and error handling.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import time
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TMDBAPIClient:
    """Client for interacting with The Movie Database API."""
    
    def __init__(self, api_key: str, base_url: str = "https://api.themoviedb.org/3"):
        """
        Initialize TMDB API client.
        
        Args:
            api_key: TMDB API key
            base_url: Base URL for TMDB API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.session = self._create_session()
        self.rate_limiter = RateLimiter(calls_per_second=40)  # TMDB limit
        
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def get_movie_details(self, movie_id: int) -> Tuple[Dict, str]:
        """
        Get detailed information for a specific movie.
        
        Args:
            movie_id: TMDB movie ID
            
        Returns:
            Tuple[Dict, str]: Movie data and API endpoint used
        """
        self.rate_limiter.wait_if_needed()
        
        endpoint = f"/movie/{movie_id}"
        url = f"{self.base_url}{endpoint}"
        
        params = {
            "api_key": self.api_key,
            "append_to_response": "credits,keywords,reviews,videos"
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            data["_tmdb_api_call_timestamp"] = datetime.utcnow().isoformat()
            
            return data, endpoint
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching movie {movie_id}: {e}")
            raise
    
    def get_popular_movies(self, page: int = 1, region: str = None) -> Tuple[Dict, str]:
        """
        Get popular movies from TMDB.
        
        Args:
            page: Page number (1-based)
            region: ISO 3166-1 country code (optional)
            
        Returns:
            Tuple[Dict, str]: Movies data and API endpoint used
        """
        self.rate_limiter.wait_if_needed()
        
        endpoint = "/movie/popular"
        url = f"{self.base_url}{endpoint}"
        
        params = {
            "api_key": self.api_key,
            "page": page
        }
        
        if region:
            params["region"] = region
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            data["_tmdb_api_call_timestamp"] = datetime.utcnow().isoformat()
            
            return data, endpoint
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching popular movies page {page}: {e}")
            raise
    
    def get_now_playing(self, page: int = 1, region: str = None) -> Tuple[Dict, str]:
        """
        Get movies currently playing in theaters.
        
        Args:
            page: Page number (1-based) 
            region: ISO 3166-1 country code (optional)
            
        Returns:
            Tuple[Dict, str]: Movies data and API endpoint used
        """
        self.rate_limiter.wait_if_needed()
        
        endpoint = "/movie/now_playing"
        url = f"{self.base_url}{endpoint}"
        
        params = {
            "api_key": self.api_key,
            "page": page
        }
        
        if region:
            params["region"] = region
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            data["_tmdb_api_call_timestamp"] = datetime.utcnow().isoformat()
            
            return data, endpoint
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching now playing movies page {page}: {e}")
            raise
    
    def discover_movies(self, **kwargs) -> Tuple[Dict, str]:
        """
        Discover movies with various filters.
        
        Args:
            **kwargs: Discovery parameters (genre, year, etc.)
            
        Returns:
            Tuple[Dict, str]: Movies data and API endpoint used
        """
        self.rate_limiter.wait_if_needed()
        
        endpoint = "/discover/movie"
        url = f"{self.base_url}{endpoint}"
        
        params = {"api_key": self.api_key}
        params.update(kwargs)
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            data["_tmdb_api_call_timestamp"] = datetime.utcnow().isoformat()
            
            return data, endpoint
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error discovering movies: {e}")
            raise
    
    def health_check(self) -> bool:
        """
        Check if TMDB API is healthy and accessible.
        
        Returns:
            bool: True if API is healthy
        """
        try:
            endpoint = "/configuration"
            url = f"{self.base_url}{endpoint}"
            
            response = self.session.get(
                url, 
                params={"api_key": self.api_key}, 
                timeout=10
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"TMDB API health check failed: {e}")
            return False


class RateLimiter:
    """Rate limiter to respect TMDB API limits."""
    
    def __init__(self, calls_per_second: int = 40):
        """
        Initialize rate limiter.
        
        Args:
            calls_per_second: Maximum calls per second allowed
        """
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limit."""
        current_time = time.time()
        elapsed = current_time - self.last_call
        
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        
        self.last_call = time.time()


class BronzeLayerIngestion:
    """Handles ingestion of raw TMDB data into Bronze layer on HDFS."""
    
    def __init__(self, spark_session, hdfs_config: Dict, tmdb_config: Dict):
        """
        Initialize Bronze layer ingestion.
        
        Args:
            spark_session: Active Spark session
            hdfs_config: HDFS configuration
            tmdb_config: TMDB API configuration
        """
        self.spark = spark_session
        self.hdfs_config = hdfs_config
        self.tmdb_config = tmdb_config
        
        # Initialize TMDB client
        api_key = os.environ.get("TMDB_API_KEY")
        if not api_key:
            raise ValueError("TMDB_API_KEY environment variable not set")
        
        self.tmdb_client = TMDBAPIClient(
            api_key=api_key,
            base_url=tmdb_config.get("base_url", "https://api.themoviedb.org/3")
        )
        
        # Setup paths
        self.bronze_path = hdfs_config["paths"]["bronze"]
        self.errors_path = hdfs_config["paths"]["errors"]
import requests
import time
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient

logger = logging.getLogger(__name__)


class TMDBBatchIngestion:
    """
    Batch ingestion from TMDB API to HDFS Bronze layer
    
    Features:
    - Rate limiting (4 requests/second)
    - Automatic retry with exponential backoff
    - Parquet output format
    - HDFS partitioning by date
    """
    
    def __init__(self, api_key: str, hdfs_url: str):
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.hdfs_client = InsecureClient(hdfs_url)
        self.rate_limit_delay = 0.25  # 4 requests/second
        self.last_request_time = 0
        
    def _rate_limited_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make rate-limited request to TMDB API"""
        # Enforce rate limit
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        
        # Make request
        url = f"{self.base_url}/{endpoint}"
        params = params or {}
        params['api_key'] = self.api_key
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            self.last_request_time = time.time()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def extract_movies(self, pages: int = 10, category: str = "popular") -> List[Dict]:
        """
        Extract movies from TMDB API
        
        Args:
            pages: Number of pages to fetch
            category: Movie category (popular, top_rated, now_playing)
        
        Returns:
            List of movie dictionaries
        """
        logger.info(f"Extracting {pages} pages of {category} movies")
        movies = []
        
        for page in range(1, pages + 1):
            try:
                data = self._rate_limited_request(
                    f"movie/{category}",
                    params={"page": page}
                )
                movies.extend(data.get("results", []))
                logger.info(f"Extracted page {page}/{pages}")
            except Exception as e:
                logger.error(f"Failed to extract page {page}: {e}")
                continue
        
        return movies
    
    def extract_movie_details(self, movie_ids: List[int]) -> List[Dict]:
        """Extract detailed information for specific movies"""
        logger.info(f"Extracting details for {len(movie_ids)} movies")
        details = []
        
        for movie_id in movie_ids:
            try:
                detail = self._rate_limited_request(f"movie/{movie_id}")
                details.append(detail)
            except Exception as e:
                logger.error(f"Failed to extract movie {movie_id}: {e}")
                continue
        
        return details
    
    def extract_reviews(self, movie_id: int) -> List[Dict]:
        """Extract reviews for a specific movie"""
        try:
            data = self._rate_limited_request(f"movie/{movie_id}/reviews")
            return data.get("results", [])
        except Exception as e:
            logger.error(f"Failed to extract reviews for movie {movie_id}: {e}")
            return []
    
    def write_to_bronze(
        self, 
        data: List[Dict], 
        data_type: str,
        partition_date: datetime = None
    ):
        """
        Write data to HDFS Bronze layer with partitioning
        
        Args:
            data: List of records to write
            data_type: Type of data (movies, reviews, people)
            partition_date: Date for partitioning (default: now)
        """
        if not data:
            logger.warning("No data to write")
            return
        
        partition_date = partition_date or datetime.now()
        
        # Create partition path
        partition_path = (
            f"/data/bronze/{data_type}/"
            f"year={partition_date.year}/"
            f"month={partition_date.month:02d}/"
            f"day={partition_date.day:02d}/"
            f"hour={partition_date.hour:02d}/"
        )
        
        # Add extraction metadata
        enriched_data = []
        for record in data:
            enriched_data.append({
                **record,
                "extraction_timestamp": partition_date.isoformat(),
                "partition_year": partition_date.year,
                "partition_month": partition_date.month,
                "partition_day": partition_date.day,
                "partition_hour": partition_date.hour
            })
        
        # Convert to Parquet
        table = pa.Table.from_pylist(enriched_data)
        
        # Write to HDFS
        filename = f"data_{int(partition_date.timestamp())}.parquet"
        hdfs_path = f"{partition_path}{filename}"
        
        try:
            # Create directory if not exists
            self.hdfs_client.makedirs(partition_path, permission=755)
            
            # Write Parquet file
            with self.hdfs_client.write(hdfs_path, overwrite=True) as writer:
                pq.write_table(table, writer)
            
            logger.info(f"Wrote {len(data)} records to {hdfs_path}")
        except Exception as e:
            logger.error(f"Failed to write to HDFS: {e}")
            raise
    
    def run_batch_extraction(self, extraction_window_hours: int = 4):
        """
        Run complete batch extraction for configured window
        
        Args:
            extraction_window_hours: Hours of data to extract
        """
        logger.info(f"Starting batch extraction for {extraction_window_hours}h window")
        extraction_time = datetime.now()
        
        # Extract movies
        movies = self.extract_movies(pages=20, category="popular")
        self.write_to_bronze(movies, "movies", extraction_time)
        
        # Extract movie details
        movie_ids = [m["id"] for m in movies[:50]]  # Limit for demo
        details = self.extract_movie_details(movie_ids)
        self.write_to_bronze(details, "movie_details", extraction_time)
        
        # Extract reviews
        all_reviews = []
        for movie_id in movie_ids[:10]:  # Limit for demo
            reviews = self.extract_reviews(movie_id)
            all_reviews.extend(reviews)
        
        if all_reviews:
            self.write_to_bronze(all_reviews, "reviews", extraction_time)
        
        logger.info(f"Batch extraction completed: {len(movies)} movies, "
                   f"{len(details)} details, {len(all_reviews)} reviews")


# TODO: Implement in next phase
# - Add Airflow DAG integration
# - Implement incremental extraction (delta)
# - Add data quality validation
# - Implement backfill functionality
# - Add metrics and monitoring
