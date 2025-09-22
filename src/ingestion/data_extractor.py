"""
Data extraction orchestrator for movie data pipeline.
"""
import logging
import time
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import threading
from queue import Queue

from .tmdb_client import TMDBClient
from .kafka_producer import MovieDataProducer

logger = logging.getLogger(__name__)

class DataExtractor:
    """Orchestrates data extraction from TMDB API and streaming to Kafka."""
    
    def __init__(self, tmdb_client: TMDBClient, kafka_producer: MovieDataProducer):
        self.tmdb_client = tmdb_client
        self.kafka_producer = kafka_producer
        self.extraction_stats = {
            'movies_processed': 0,
            'people_processed': 0,
            'credits_processed': 0,
            'reviews_processed': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
        self.is_running = False
        self.stop_event = threading.Event()
    
    def extract_popular_movies(self, max_pages: int = 5) -> List[Dict[str, Any]]:
        """Extract popular movies from TMDB."""
        logger.info(f"Extracting popular movies (max {max_pages} pages)")
        all_movies = []
        
        for page in range(1, max_pages + 1):
            if self.stop_event.is_set():
                break
                
            data = self.tmdb_client.get_popular_movies(page)
            if data and 'results' in data:
                movies = data['results']
                all_movies.extend(movies)
                self.extraction_stats['movies_processed'] += len(movies)
                logger.info(f"Extracted {len(movies)} movies from page {page}")
                
                # Stream to Kafka
                self.kafka_producer.produce_movies(movies)
                
                # Extract additional details for each movie
                self._extract_movie_details(movies)
            else:
                logger.warning(f"No data received for page {page}")
                self.extraction_stats['errors'] += 1
        
        return all_movies
    
    def extract_trending_movies(self, time_window: str = 'day') -> List[Dict[str, Any]]:
        """Extract trending movies."""
        logger.info(f"Extracting trending movies ({time_window})")
        
        data = self.tmdb_client.get_trending_movies(time_window)
        if data and 'results' in data:
            movies = data['results']
            self.extraction_stats['movies_processed'] += len(movies)
            
            # Stream to Kafka
            self.kafka_producer.produce_movies(movies)
            
            # Extract additional details
            self._extract_movie_details(movies)
            
            logger.info(f"Extracted {len(movies)} trending movies")
            return movies
        else:
            logger.warning("No trending movies data received")
            self.extraction_stats['errors'] += 1
            return []
    
    def extract_trending_people(self, time_window: str = 'day') -> List[Dict[str, Any]]:
        """Extract trending people."""
        logger.info(f"Extracting trending people ({time_window})")
        
        data = self.tmdb_client.get_trending_people(time_window)
        if data and 'results' in data:
            people = data['results']
            self.extraction_stats['people_processed'] += len(people)
            
            # Stream to Kafka
            self.kafka_producer.produce_people(people)
            
            # Extract additional details
            self._extract_people_details(people)
            
            logger.info(f"Extracted {len(people)} trending people")
            return people
        else:
            logger.warning("No trending people data received")
            self.extraction_stats['errors'] += 1
            return []
    
    def _extract_movie_details(self, movies: List[Dict[str, Any]]):
        """Extract detailed information for movies."""
        for movie in movies:
            if self.stop_event.is_set():
                break
                
            movie_id = movie.get('id')
            if not movie_id:
                continue
            
            try:
                # Get movie details
                details = self.tmdb_client.get_movie_details(movie_id)
                if details:
                    # Update movie with additional details
                    movie.update(details)
                
                # Get credits
                credits = self.tmdb_client.get_movie_credits(movie_id)
                if credits:
                    self.kafka_producer.produce_credits(credits, movie_id)
                    self.extraction_stats['credits_processed'] += 1
                
                # Get reviews
                reviews_data = self.tmdb_client.get_movie_reviews(movie_id)
                if reviews_data and 'results' in reviews_data:
                    reviews = reviews_data['results']
                    if reviews:
                        self.kafka_producer.produce_reviews(reviews, movie_id)
                        self.extraction_stats['reviews_processed'] += len(reviews)
                
            except Exception as e:
                logger.error(f"Error extracting details for movie {movie_id}: {e}")
                self.extraction_stats['errors'] += 1
    
    def _extract_people_details(self, people: List[Dict[str, Any]]):
        """Extract detailed information for people."""
        for person in people:
            if self.stop_event.is_set():
                break
                
            person_id = person.get('id')
            if not person_id:
                continue
            
            try:
                # Get person details
                details = self.tmdb_client.get_person_details(person_id)
                if details:
                    person.update(details)
                
                # Get movie credits
                credits = self.tmdb_client.get_person_movie_credits(person_id)
                if credits:
                    # Process cast and crew credits
                    for cast_credit in credits.get('cast', []):
                        credit_data = {
                            'person_id': person_id,
                            'movie_id': cast_credit.get('id'),
                            'credit_type': 'cast',
                            **cast_credit
                        }
                        self.kafka_producer.produce_movie_data('credits', credit_data, 
                                                             f"{person_id}_{cast_credit.get('id')}_cast")
                    
                    for crew_credit in credits.get('crew', []):
                        credit_data = {
                            'person_id': person_id,
                            'movie_id': crew_credit.get('id'),
                            'credit_type': 'crew',
                            **crew_credit
                        }
                        self.kafka_producer.produce_movie_data('credits', credit_data, 
                                                             f"{person_id}_{crew_credit.get('id')}_crew")
                
            except Exception as e:
                logger.error(f"Error extracting details for person {person_id}: {e}")
                self.extraction_stats['errors'] += 1
    
    def run_continuous_extraction(self, interval_minutes: int = 60):
        """Run continuous data extraction."""
        logger.info(f"Starting continuous extraction (interval: {interval_minutes} minutes)")
        self.is_running = True
        self.extraction_stats['start_time'] = datetime.now()
        
        try:
            while not self.stop_event.is_set():
                cycle_start = datetime.now()
                
                # Extract trending data
                self.extract_trending_movies('day')
                self.extract_trending_people('day')
                
                # Extract popular movies periodically
                if cycle_start.minute % 30 == 0:  # Every 30 minutes
                    self.extract_popular_movies(max_pages=2)
                
                # Flush Kafka producer
                self.kafka_producer.flush()
                
                # Calculate sleep time
                cycle_duration = (datetime.now() - cycle_start).total_seconds()
                sleep_time = max(0, (interval_minutes * 60) - cycle_duration)
                
                logger.info(f"Extraction cycle completed in {cycle_duration:.2f}s, sleeping for {sleep_time:.2f}s")
                
                if sleep_time > 0:
                    self.stop_event.wait(sleep_time)
                    
        except KeyboardInterrupt:
            logger.info("Extraction interrupted by user")
        except Exception as e:
            logger.error(f"Error in continuous extraction: {e}")
            raise
        finally:
            self.is_running = False
            self.extraction_stats['end_time'] = datetime.now()
            logger.info("Continuous extraction stopped")
    
    def stop_extraction(self):
        """Stop the extraction process."""
        logger.info("Stopping extraction...")
        self.stop_event.set()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get extraction statistics."""
        stats = self.extraction_stats.copy()
        if stats['start_time'] and stats['end_time']:
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
        elif stats['start_time']:
            stats['duration'] = (datetime.now() - stats['start_time']).total_seconds()
        return stats