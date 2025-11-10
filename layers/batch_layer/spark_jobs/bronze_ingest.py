"""
Bronze Layer Ingestion - TMDB API to MinIO/S3A

Fetches data from TMDB API and writes raw data as Parquet to Bronze layer.
Implements rate limiting (4 req/s), retry logic, and partitioned storage.

Usage:
    spark-submit bronze_ingest.py --extraction-window 4 --categories popular,top_rated
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)
import pyspark.sql.functions as F

# Add utils to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import get_spark_session, stop_spark_session
from utils.logger import get_logger, log_execution, JobMetrics
from utils.s3_utils import get_bronze_path

logger = get_logger(__name__)


class TMDBAPIClient:
    """
    TMDB API client with rate limiting and retry logic.
    
    Features:
    - Rate limiting (4 requests/second)
    - Automatic retry with exponential backoff
    - Request logging
    """
    
    def __init__(self, api_key: str, rate_limit: float = 4.0):
        """
        Initialize TMDB API client.
        
        Args:
            api_key: TMDB API key
            rate_limit: Max requests per second (default: 4.0)
        """
        self.api_key = api_key
        self.base_url = "https://api.themoviedb.org/3"
        self.rate_limit = rate_limit
        self.min_request_interval = 1.0 / rate_limit
        self.last_request_time = 0
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]  # Changed from method_whitelist for urllib3 v2
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
    
    def _rate_limit(self):
        """Enforce rate limiting."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make API request with rate limiting.
        
        Args:
            endpoint: API endpoint (e.g., "movie/popular")
            params: Query parameters
        
        Returns:
            JSON response as dictionary
        """
        self._rate_limit()
        
        url = f"{self.base_url}/{endpoint}"
        params = params or {}
        params['api_key'] = self.api_key
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API request failed: {endpoint}", 
                        extra={"context": {"error": str(e), "endpoint": endpoint}})
            raise
    
    def fetch_movies(self, category: str = "popular", pages: int = None, max_pages: int = 500) -> List[Dict]:
        """
        Fetch movies from TMDB API.
        
        Args:
            category: Movie category (popular, top_rated, now_playing, upcoming)
            pages: Number of pages to fetch (20 movies per page). If None, fetch all available pages.
            max_pages: Maximum pages to fetch as safety limit (default: 500)
        
        Returns:
            List of movie dictionaries
        """
        movies = []
        
        # First request to get total_pages
        try:
            first_data = self._make_request(f"movie/{category}", params={"page": 1})
            total_available_pages = first_data.get("total_pages", 1)
            results = first_data.get("results", [])
            movies.extend(results)
            
            # Determine how many pages to fetch
            if pages is None:
                # Fetch all pages, but cap at max_pages
                pages_to_fetch = min(total_available_pages, max_pages)
            else:
                pages_to_fetch = min(pages, total_available_pages, max_pages)
            
            logger.info(f"Fetching {pages_to_fetch} pages of {category} movies (total available: {total_available_pages})")
            logger.info(f"Fetched page 1/{pages_to_fetch}: {len(results)} movies",
                       extra={"context": {"category": category, "page": 1}})
            
            # Fetch remaining pages
            for page in range(2, pages_to_fetch + 1):
                try:
                    data = self._make_request(f"movie/{category}", params={"page": page})
                    results = data.get("results", [])
                    movies.extend(results)
                    
                    logger.info(f"Fetched page {page}/{pages_to_fetch}: {len(results)} movies",
                               extra={"context": {"category": category, "page": page}})
                    
                except Exception as e:
                    logger.error(f"Failed to fetch page {page}", 
                                extra={"context": {"error": str(e), "page": page}})
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to fetch movies for category {category}", 
                        extra={"context": {"error": str(e), "category": category}})
        
        return movies
    
    def fetch_movie_details(self, movie_id: int) -> Optional[Dict]:
        """Fetch detailed information for a specific movie."""
        try:
            return self._make_request(f"movie/{movie_id}")
        except Exception as e:
            logger.error(f"Failed to fetch movie details: {movie_id}",
                        extra={"context": {"error": str(e), "movie_id": movie_id}})
            return None
    
    def fetch_movie_credits(self, movie_id: int) -> Optional[Dict]:
        """Fetch cast and crew information for a movie."""
        try:
            return self._make_request(f"movie/{movie_id}/credits")
        except Exception as e:
            logger.error(f"Failed to fetch credits: {movie_id}",
                        extra={"context": {"error": str(e), "movie_id": movie_id}})
            return None
    
    def fetch_movie_reviews(self, movie_id: int, pages: int = 1) -> List[Dict]:
        """Fetch reviews for a movie."""
        reviews = []
        
        for page in range(1, pages + 1):
            try:
                data = self._make_request(f"movie/{movie_id}/reviews", params={"page": page})
                results = data.get("results", [])
                reviews.extend(results)
            except Exception as e:
                logger.error(f"Failed to fetch reviews: {movie_id}",
                            extra={"context": {"error": str(e), "movie_id": movie_id}})
                break
        
        return reviews


class BronzeIngestionJob:
    """
    Bronze Layer ingestion job.
    
    Extracts data from TMDB API and writes to MinIO/S3A as Parquet.
    """
    
    def __init__(self, spark, api_key: str):
        self.spark = spark
        self.api_client = TMDBAPIClient(api_key)
        self.metrics = JobMetrics("bronze_ingest")
    
    @log_execution(logger, "bronze_ingest")
    def run(
        self,
        extraction_window_hours: int = 4,
        categories: List[str] = None,
        pages_per_category: int = 250,
        max_pages: int = 500
    ):
        """
        Run Bronze layer ingestion.
        
        Args:
            extraction_window_hours: Hours of data to extract (for logging/tracking)
            categories: Movie categories to extract (default: ["popular", "top_rated"])
            pages_per_category: Number of pages to fetch per category (default: 250 = 5000 movies/category, use 500 for max)
            max_pages: Maximum pages to fetch as safety limit (default: 500)
        """
        if categories is None:
            categories = ["popular", "top_rated"]
        
        extraction_time = datetime.utcnow()
        
        logger.info("Starting Bronze layer ingestion",
                   extra={"context": {
                       "extraction_time": extraction_time.isoformat() + "Z",
                       "categories": categories,
                       "pages_per_category": pages_per_category if pages_per_category else "all",
                       "max_pages": max_pages
                   }})
        
        # Fetch movies from each category
        all_movies = []
        for category in categories:
            movies = self.api_client.fetch_movies(category, pages_per_category, max_pages)
            all_movies.extend(movies)
            self.metrics.add_metric(f"movies_fetched_{category}", len(movies))
        
        # Deduplicate by movie_id (across categories)
        unique_movies = {m['id']: m for m in all_movies}.values()
        unique_movies = list(unique_movies)
        
        logger.info(f"Fetched {len(all_movies)} total movies, {len(unique_movies)} unique")
        self.metrics.add_metric("total_movies_fetched", len(all_movies))
        self.metrics.add_metric("unique_movies", len(unique_movies))
        
        # Write movies to Bronze
        self._write_movies_to_bronze(unique_movies, extraction_time)
        
        # Fetch details for sample of movies
        movie_ids = [m['id'] for m in unique_movies[:50]]  # Limit for performance
        details = self._fetch_movie_details_batch(movie_ids)
        self._write_details_to_bronze(details, extraction_time)
        
        # Fetch credits for sample
        credits = self._fetch_credits_batch(movie_ids[:30])
        self._write_credits_to_bronze(credits, extraction_time)
        
        # Fetch reviews for top movies
        reviews = self._fetch_reviews_batch(movie_ids[:20])
        self._write_reviews_to_bronze(reviews, extraction_time)
        
        # Log final metrics
        self.metrics.log(logger)
        
        logger.info("Bronze layer ingestion completed successfully")
    
    def _fetch_movie_details_batch(self, movie_ids: List[int]) -> List[Dict]:
        """Fetch details for multiple movies."""
        logger.info(f"Fetching details for {len(movie_ids)} movies")
        details = []
        
        for movie_id in movie_ids:
            detail = self.api_client.fetch_movie_details(movie_id)
            if detail:
                details.append(detail)
        
        self.metrics.add_metric("movie_details_fetched", len(details))
        return details
    
    def _fetch_credits_batch(self, movie_ids: List[int]) -> List[Dict]:
        """Fetch credits for multiple movies."""
        logger.info(f"Fetching credits for {len(movie_ids)} movies")
        credits_list = []
        
        for movie_id in movie_ids:
            credits = self.api_client.fetch_movie_credits(movie_id)
            if credits:
                credits['movie_id'] = movie_id
                credits_list.append(credits)
        
        self.metrics.add_metric("credits_fetched", len(credits_list))
        return credits_list
    
    def _fetch_reviews_batch(self, movie_ids: List[int]) -> List[Dict]:
        """Fetch reviews for multiple movies."""
        logger.info(f"Fetching reviews for {len(movie_ids)} movies")
        all_reviews = []
        
        for movie_id in movie_ids:
            reviews = self.api_client.fetch_movie_reviews(movie_id, pages=1)
            for review in reviews:
                review['movie_id'] = movie_id
            all_reviews.extend(reviews)
        
        self.metrics.add_metric("reviews_fetched", len(all_reviews))
        return all_reviews
    
    def _write_movies_to_bronze(self, movies: List[Dict], extraction_time: datetime):
        """Write movies to Bronze layer."""
        if not movies:
            logger.warning("No movies to write to Bronze")
            return
        
        logger.info(f"Writing {len(movies)} movies to Bronze layer")
        
        # Add extraction metadata
        for movie in movies:
            movie['extraction_timestamp'] = extraction_time.isoformat() + "Z"
            movie['partition_year'] = extraction_time.year
            movie['partition_month'] = extraction_time.month
            movie['partition_day'] = extraction_time.day
            movie['partition_hour'] = extraction_time.hour
            # Convert entire movie dict to JSON string for raw storage
            movie['raw_data'] = str(movie)
        
        # Create DataFrame
        df = self.spark.createDataFrame(movies)
        
        # Write to S3A with partitioning
        output_path = get_bronze_path("movies", extraction_time)
        
        df.write \
            .mode("append") \
            .partitionBy("partition_year", "partition_month", "partition_day", "partition_hour") \
            .parquet(output_path)
        
        logger.info(f"Wrote {len(movies)} movies to {output_path}")
        self.metrics.add_metric("movies_written", len(movies))
    
    def _write_details_to_bronze(self, details: List[Dict], extraction_time: datetime):
        """Write movie details to Bronze layer."""
        if not details:
            logger.warning("No details to write to Bronze")
            return
        
        logger.info(f"Writing {len(details)} movie details to Bronze layer")
        
        # Add extraction metadata
        for detail in details:
            detail['extraction_timestamp'] = extraction_time.isoformat() + "Z"
            detail['partition_year'] = extraction_time.year
            detail['partition_month'] = extraction_time.month
            detail['partition_day'] = extraction_time.day
            detail['partition_hour'] = extraction_time.hour
            detail['raw_data'] = str(detail)
        
        df = self.spark.createDataFrame(details)
        output_path = get_bronze_path("movie_details", extraction_time)
        
        df.write \
            .mode("append") \
            .partitionBy("partition_year", "partition_month", "partition_day", "partition_hour") \
            .parquet(output_path)
        
        logger.info(f"Wrote {len(details)} details to {output_path}")
        self.metrics.add_metric("details_written", len(details))
    
    def _write_credits_to_bronze(self, credits: List[Dict], extraction_time: datetime):
        """Write credits to Bronze layer."""
        if not credits:
            logger.warning("No credits to write to Bronze")
            return
        
        logger.info(f"Writing {len(credits)} credits to Bronze layer")
        
        for credit in credits:
            credit['extraction_timestamp'] = extraction_time.isoformat() + "Z"
            credit['partition_year'] = extraction_time.year
            credit['partition_month'] = extraction_time.month
            credit['partition_day'] = extraction_time.day
            credit['partition_hour'] = extraction_time.hour
            credit['raw_data'] = str(credit)
        
        df = self.spark.createDataFrame(credits)
        output_path = get_bronze_path("credits", extraction_time)
        
        df.write \
            .mode("append") \
            .partitionBy("partition_year", "partition_month", "partition_day", "partition_hour") \
            .parquet(output_path)
        
        logger.info(f"Wrote {len(credits)} credits to {output_path}")
        self.metrics.add_metric("credits_written", len(credits))
    
    def _write_reviews_to_bronze(self, reviews: List[Dict], extraction_time: datetime):
        """Write reviews to Bronze layer."""
        if not reviews:
            logger.warning("No reviews to write to Bronze")
            return
        
        logger.info(f"Writing {len(reviews)} reviews to Bronze layer")
        
        for review in reviews:
            review['extraction_timestamp'] = extraction_time.isoformat() + "Z"
            review['partition_year'] = extraction_time.year
            review['partition_month'] = extraction_time.month
            review['partition_day'] = extraction_time.day
            review['partition_hour'] = extraction_time.hour
            review['raw_data'] = str(review)
        
        df = self.spark.createDataFrame(reviews)
        output_path = get_bronze_path("reviews", extraction_time)
        
        df.write \
            .mode("append") \
            .partitionBy("partition_year", "partition_month", "partition_day", "partition_hour") \
            .parquet(output_path)
        
        logger.info(f"Wrote {len(reviews)} reviews to {output_path}")
        self.metrics.add_metric("reviews_written", len(reviews))


def main():
    """Main entry point for Bronze ingestion job."""
    parser = argparse.ArgumentParser(description="Bronze Layer TMDB Ingestion")
    parser.add_argument("--extraction-window", type=int, default=4,
                       help="Extraction window in hours (default: 4)")
    parser.add_argument("--categories", type=str, default="popular,top_rated",
                       help="Comma-separated list of movie categories")
    parser.add_argument("--pages-per-category", type=int, default=250,
                       help="Number of pages to fetch per category (default: 250 = 5000 movies/category, max: 500)")
    parser.add_argument("--max-pages", type=int, default=500,
                       help="Maximum pages to fetch as safety limit (default: 500)")
    
    args = parser.parse_args()
    
    # Get API key from environment
    api_key = os.getenv('TMDB_API_KEY')
    if not api_key:
        logger.error("TMDB_API_KEY environment variable not set")
        sys.exit(1)
    
    categories = [c.strip() for c in args.categories.split(',')]
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("bronze_ingest")
        
        # Run ingestion
        job = BronzeIngestionJob(spark, api_key)
        job.run(
            extraction_window_hours=args.extraction_window,
            categories=categories,
            pages_per_category=args.pages_per_category,
            max_pages=args.max_pages
        )
        
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        if spark:
            stop_spark_session(spark)


if __name__ == "__main__":
    main()
