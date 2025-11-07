"""
Batch Layer - Master Dataset Ingestion

This module handles extraction of data from TMDB API and storage in HDFS Bronze layer.
Runs every 4 hours as part of the batch layer processing.

Features:
- TMDB API integration with rate limiting (4 req/s)
- Retry logic with exponential backoff
- HDFS Bronze layer writes
- CLI support with date range specification
- Structured logging

Usage:
    # Extract data for a specific date range
    python ingestion.py --start-date 2025-01-01 --end-date 2025-01-02
    
    # Extract recent data (default: last 4 hours)
    python ingestion.py
    
    # Extract with custom config
    python ingestion.py --pages 50 --categories popular,top_rated
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
from config.config import config
from layers.batch_layer.spark_jobs.utils.logging_utils import get_logger, JobMetrics


logger = get_logger(__name__)


class TMDBAPIClient:
    """
    TMDB API client with rate limiting and retry logic.
    
    Features:
    - Rate limiting (4 requests/second as per TMDB)
    - Automatic retry with exponential backoff
    - Request tracking and metrics
    """
    
    def __init__(self, api_key: str):
        """
        Initialize TMDB API client.
        
        Args:
            api_key: TMDB API key
        """
        self.api_key = api_key
        self.base_url = config.tmdb_base_url
        self.rate_limit = config.tmdb_rate_limit  # requests per second
        self.rate_limit_delay = 1.0 / self.rate_limit
        self.last_request_time = 0
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=config.tmdb_retry_attempts,
            backoff_factor=config.tmdb_retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Metrics
        self.request_count = 0
        self.error_count = 0
    
    def _rate_limit_wait(self):
        """Enforce rate limiting by waiting if necessary."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            wait_time = self.rate_limit_delay - time_since_last
            time.sleep(wait_time)
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make rate-limited request to TMDB API.
        
        Args:
            endpoint: API endpoint (e.g., "movie/popular")
            params: Query parameters
            
        Returns:
            JSON response as dictionary
            
        Raises:
            requests.RequestException: If request fails after retries
        """
        self._rate_limit_wait()
        
        url = f"{self.base_url}/{endpoint}"
        params = params or {}
        params['api_key'] = self.api_key
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            self.last_request_time = time.time()
            self.request_count += 1
            
            return response.json()
            
        except requests.RequestException as e:
            self.error_count += 1
            logger.error(f"API request failed: {endpoint}", extra={
                "error": str(e),
                "status_code": getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            })
            raise
    
    def get_popular_movies(self, page: int = 1) -> Dict:
        """Get popular movies."""
        return self._make_request("movie/popular", {"page": page})
    
    def get_top_rated_movies(self, page: int = 1) -> Dict:
        """Get top rated movies."""
        return self._make_request("movie/top_rated", {"page": page})
    
    def get_now_playing_movies(self, page: int = 1) -> Dict:
        """Get now playing movies."""
        return self._make_request("movie/now_playing", {"page": page})
    
    def get_upcoming_movies(self, page: int = 1) -> Dict:
        """Get upcoming movies."""
        return self._make_request("movie/upcoming", {"page": page})
    
    def get_movie_details(self, movie_id: int) -> Dict:
        """Get detailed information for a movie."""
        return self._make_request(f"movie/{movie_id}", {
            "append_to_response": "credits,keywords,reviews"
        })
    
    def get_movie_reviews(self, movie_id: int, page: int = 1) -> Dict:
        """Get reviews for a movie."""
        return self._make_request(f"movie/{movie_id}/reviews", {"page": page})
    
    def get_movie_credits(self, movie_id: int) -> Dict:
        """Get cast and crew for a movie."""
        return self._make_request(f"movie/{movie_id}/credits")
    
    def get_genres(self) -> Dict:
        """Get list of movie genres."""
        return self._make_request("genre/movie/list")


class BronzeLayerWriter:
    """
    Writer for Bronze layer data to HDFS.
    
    Writes raw JSON data as Parquet files with partitioning.
    """
    
    def __init__(self):
        """Initialize Bronze layer writer."""
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
        
        self.spark = SparkSession.builder \
            .appName("bronze_layer_ingestion") \
            .config("spark.hadoop.fs.defaultFS", config.hdfs_namenode) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Bronze schema
        self.schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("raw_json", StringType(), False),
            StructField("api_endpoint", StringType(), False),
            StructField("extraction_timestamp", TimestampType(), False),
            StructField("partition_year", IntegerType(), False),
            StructField("partition_month", IntegerType(), False),
            StructField("partition_day", IntegerType(), False),
            StructField("partition_hour", IntegerType(), False)
        ])
    
    def write_to_bronze(
        self,
        data: List[Dict],
        data_type: str,
        extraction_time: datetime
    ) -> int:
        """
        Write data to HDFS Bronze layer.
        
        Args:
            data: List of records to write
            data_type: Type of data (movies, reviews, etc.)
            extraction_time: Timestamp of extraction
            
        Returns:
            Number of records written
        """
        if not data:
            logger.warning(f"No data to write for {data_type}")
            return 0
        
        # Prepare records
        records = []
        for record in data:
            records.append({
                "id": record.get("id", 0),
                "raw_json": json.dumps(record),
                "api_endpoint": data_type,
                "extraction_timestamp": extraction_time,
                "partition_year": extraction_time.year,
                "partition_month": extraction_time.month,
                "partition_day": extraction_time.day,
                "partition_hour": extraction_time.hour
            })
        
        # Create DataFrame
        df = self.spark.createDataFrame(records, schema=self.schema)
        
        # Construct path
        base_path = f"{config.hdfs_namenode}{config.hdfs_paths['bronze']}/{data_type}"
        
        # Write as Parquet with partitioning
        df.write \
            .mode("append") \
            .partitionBy("partition_year", "partition_month", "partition_day", "partition_hour") \
            .parquet(base_path)
        
        logger.info(f"Wrote {len(records)} records to Bronze layer", extra={
            "data_type": data_type,
            "record_count": len(records),
            "path": base_path
        })
        
        return len(records)
    
    def close(self):
        """Close Spark session."""
        if self.spark:
            self.spark.stop()


class TMDBBatchIngestion:
    """
    Main batch ingestion orchestrator.
    
    Coordinates TMDB API extraction and Bronze layer writes.
    """
    
    def __init__(self, api_key: str):
        """
        Initialize batch ingestion.
        
        Args:
            api_key: TMDB API key
        """
        self.api_client = TMDBAPIClient(api_key)
        self.bronze_writer = BronzeLayerWriter()
        self.metrics = JobMetrics("tmdb_batch_ingestion", logger)
    
    def extract_movies(
        self,
        categories: List[str] = None,
        pages_per_category: int = 20
    ) -> List[Dict]:
        """
        Extract movies from multiple categories.
        
        Args:
            categories: List of categories (popular, top_rated, now_playing, upcoming)
            pages_per_category: Number of pages to fetch per category
            
        Returns:
            List of movie records
        """
        categories = categories or ["popular", "top_rated", "now_playing"]
        all_movies = []
        movie_ids = set()
        
        category_methods = {
            "popular": self.api_client.get_popular_movies,
            "top_rated": self.api_client.get_top_rated_movies,
            "now_playing": self.api_client.get_now_playing_movies,
            "upcoming": self.api_client.get_upcoming_movies
        }
        
        for category in categories:
            logger.info(f"Extracting {category} movies")
            method = category_methods.get(category)
            
            if not method:
                logger.warning(f"Unknown category: {category}")
                continue
            
            for page in range(1, pages_per_category + 1):
                try:
                    response = method(page)
                    movies = response.get("results", [])
                    
                    # Deduplicate by movie ID
                    for movie in movies:
                        if movie["id"] not in movie_ids:
                            all_movies.append(movie)
                            movie_ids.add(movie["id"])
                    
                    logger.debug(f"Extracted page {page}/{pages_per_category} of {category}")
                    
                except Exception as e:
                    logger.error(f"Failed to extract {category} page {page}: {e}")
                    continue
        
        self.metrics.add_metric("movies_extracted", len(all_movies))
        return all_movies
    
    def extract_movie_details(self, movie_ids: List[int]) -> List[Dict]:
        """
        Extract detailed information for specific movies.
        
        Args:
            movie_ids: List of movie IDs
            
        Returns:
            List of detailed movie records
        """
        logger.info(f"Extracting details for {len(movie_ids)} movies")
        details = []
        
        for i, movie_id in enumerate(movie_ids):
            try:
                detail = self.api_client.get_movie_details(movie_id)
                details.append(detail)
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Extracted {i + 1}/{len(movie_ids)} movie details")
                    
            except Exception as e:
                logger.error(f"Failed to extract details for movie {movie_id}: {e}")
                continue
        
        self.metrics.add_metric("movie_details_extracted", len(details))
        return details
    
    def extract_reviews(self, movie_ids: List[int], max_reviews_per_movie: int = 20) -> List[Dict]:
        """
        Extract reviews for movies.
        
        Args:
            movie_ids: List of movie IDs
            max_reviews_per_movie: Maximum reviews per movie
            
        Returns:
            List of review records
        """
        logger.info(f"Extracting reviews for {len(movie_ids)} movies")
        all_reviews = []
        
        for movie_id in movie_ids:
            try:
                response = self.api_client.get_movie_reviews(movie_id, page=1)
                reviews = response.get("results", [])[:max_reviews_per_movie]
                
                # Add movie_id to each review
                for review in reviews:
                    review["movie_id"] = movie_id
                
                all_reviews.extend(reviews)
                
            except Exception as e:
                logger.error(f"Failed to extract reviews for movie {movie_id}: {e}")
                continue
        
        self.metrics.add_metric("reviews_extracted", len(all_reviews))
        return all_reviews
    
    def run_full_extraction(
        self,
        extraction_time: Optional[datetime] = None,
        pages: int = 20,
        categories: List[str] = None,
        extract_details: bool = True,
        extract_reviews: bool = True
    ) -> Dict[str, int]:
        """
        Run full batch extraction pipeline.
        
        Args:
            extraction_time: Timestamp for partitioning (default: now)
            pages: Pages per category to extract
            categories: Movie categories to extract
            extract_details: Whether to extract detailed movie info
            extract_reviews: Whether to extract reviews
            
        Returns:
            Dictionary with extraction counts
        """
        extraction_time = extraction_time or datetime.now()
        logger.info("Starting full batch extraction", extra={
            "extraction_time": extraction_time.isoformat(),
            "pages": pages,
            "categories": categories or ["popular", "top_rated", "now_playing"]
        })
        
        results = {}
        
        try:
            # Extract movies
            movies = self.extract_movies(categories, pages)
            movie_count = self.bronze_writer.write_to_bronze(movies, "movies", extraction_time)
            results["movies"] = movie_count
            
            # Extract movie details
            if extract_details and movies:
                movie_ids = [m["id"] for m in movies]
                details = self.extract_movie_details(movie_ids)
                details_count = self.bronze_writer.write_to_bronze(details, "movie_details", extraction_time)
                results["movie_details"] = details_count
            
            # Extract reviews
            if extract_reviews and movies:
                # Limit to first 100 movies for reviews
                movie_ids = [m["id"] for m in movies[:100]]
                reviews = self.extract_reviews(movie_ids)
                reviews_count = self.bronze_writer.write_to_bronze(reviews, "reviews", extraction_time)
                results["reviews"] = reviews_count
            
            # Record API metrics
            self.metrics.add_metric("api_requests", self.api_client.request_count)
            self.metrics.add_metric("api_errors", self.api_client.error_count)
            
            logger.info("Batch extraction completed successfully", extra=results)
            self.metrics.finish(success=True)
            
            return results
            
        except Exception as e:
            logger.error(f"Batch extraction failed: {e}", exc_info=True)
            self.metrics.finish(success=False)
            raise
        finally:
            self.bronze_writer.close()


def main():
    """
    CLI entry point for batch ingestion.
    """
    parser = argparse.ArgumentParser(description="TMDB Batch Ingestion to Bronze Layer")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD). Default: 4 hours ago"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD). Default: now"
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=20,
        help="Pages per category to extract (default: 20)"
    )
    parser.add_argument(
        "--categories",
        type=str,
        default="popular,top_rated,now_playing",
        help="Comma-separated categories (default: popular,top_rated,now_playing)"
    )
    parser.add_argument(
        "--no-details",
        action="store_true",
        help="Skip extracting detailed movie info"
    )
    parser.add_argument(
        "--no-reviews",
        action="store_true",
        help="Skip extracting reviews"
    )
    
    args = parser.parse_args()
    
    # Validate API key
    if not config.tmdb_api_key:
        logger.error("TMDB_API_KEY not found in environment")
        sys.exit(1)
    
    # Parse dates
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_date = datetime.now() - timedelta(hours=config.batch_interval_hours)
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        end_date = datetime.now()
    
    # Parse categories
    categories = [c.strip() for c in args.categories.split(",")]
    
    logger.info("Starting TMDB batch ingestion", extra={
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "pages": args.pages,
        "categories": categories
    })
    
    # Run ingestion
    ingestion = TMDBBatchIngestion(config.tmdb_api_key)
    
    try:
        results = ingestion.run_full_extraction(
            extraction_time=end_date,
            pages=args.pages,
            categories=categories,
            extract_details=not args.no_details,
            extract_reviews=not args.no_reviews
        )
        
        print(f"\n✅ Ingestion completed successfully!")
        print(f"   Movies: {results.get('movies', 0)}")
        print(f"   Details: {results.get('movie_details', 0)}")
        print(f"   Reviews: {results.get('reviews', 0)}")
        
    except Exception as e:
        print(f"\n❌ Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

