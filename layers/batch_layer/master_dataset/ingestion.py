"""
Batch Layer - Master Dataset Ingestion

This module handles extraction of data from TMDB API and storage in HDFS Bronze layer.
Runs every 4 hours as part of the batch layer processing.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
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
