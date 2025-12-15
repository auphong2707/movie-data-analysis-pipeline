"""
Reddit to TMDB Movie Matching Service

Matches movie titles extracted from Reddit posts/comments to TMDB movie database.
Uses fuzzy string matching and caching for performance.

Usage:
    from movie_matcher import MovieMatcher
    matcher = MovieMatcher()
    tmdb_id = matcher.match_title("The Shawshank Redemption")
"""

import os
import logging
import re
import time
from typing import Optional, List, Dict, Tuple
from difflib import SequenceMatcher
from datetime import datetime, timedelta
import requests

logger = logging.getLogger(__name__)


class MovieMatcher:
    """
    Fuzzy matching service to map Reddit movie mentions to TMDB movie IDs.
    
    Features:
    - Fuzzy string matching with configurable threshold
    - Local cache of TMDB popular movies
    - Automatic cache refresh every 24 hours
    - Year extraction from titles (e.g., "Dune (2021)")
    """
    
    def __init__(self, tmdb_api_key: str = None, similarity_threshold: float = 0.75):
        """
        Initialize movie matcher.
        
        Args:
            tmdb_api_key: TMDB API key (optional, uses env var if not provided)
            similarity_threshold: Minimum similarity score (0.0-1.0) for match
        """
        self.tmdb_api_key = tmdb_api_key or os.getenv('TMDB_API_KEY')
        self.similarity_threshold = similarity_threshold
        self.base_url = "https://api.themoviedb.org/3"
        
        # Movie cache: {normalized_title: (movie_id, original_title, year)}
        self.movie_cache: Dict[str, Tuple[int, str, Optional[int]]] = {}
        self.cache_last_updated: Optional[datetime] = None
        self.cache_ttl = timedelta(hours=24)
        
        # Initialize cache
        self._refresh_cache_if_needed()
        
        logger.info(f"MovieMatcher initialized with {len(self.movie_cache)} movies in cache")
    
    def _normalize_title(self, title: str) -> str:
        """
        Normalize movie title for comparison.
        
        Removes special characters, converts to lowercase, removes articles.
        Preserves numbers for sequel matching.
        
        Args:
            title: Original title
            
        Returns:
            Normalized title
        """
        if not title:
            return ""
        
        # Extract year if present (e.g., "Dune (2021)" -> "dune")
        title = re.sub(r'\s*\(\d{4}\)\s*', '', title)
        
        # Convert to lowercase
        title = title.lower()
        
        # Remove special characters except spaces and numbers (keep numbers for sequels)
        title = re.sub(r'[^\w\s\d]', '', title)
        
        # Remove leading articles (the, a, an)
        title = re.sub(r'^(the|a|an)\s+', '', title)
        
        # Collapse multiple spaces
        title = re.sub(r'\s+', ' ', title).strip()
        
        return title
    
    def _extract_year(self, title: str) -> Optional[int]:
        """
        Extract year from title if present.
        
        Args:
            title: Title string (e.g., "Dune (2021)")
            
        Returns:
            Year as integer or None
        """
        match = re.search(r'\((\d{4})\)', title)
        return int(match.group(1)) if match else None
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate similarity score between two strings.
        
        Args:
            str1: First string
            str2: Second string
            
        Returns:
            Similarity score (0.0-1.0)
        """
        return SequenceMatcher(None, str1, str2).ratio()
    
    def _refresh_cache_if_needed(self):
        """Refresh movie cache if TTL expired."""
        if (self.cache_last_updated is None or 
            datetime.now() - self.cache_last_updated > self.cache_ttl):
            logger.info("Refreshing TMDB movie cache...")
            self._load_tmdb_movies()
            self.cache_last_updated = datetime.now()
    
    def _load_tmdb_movies(self):
        """
        Load popular movies from TMDB API into cache.
        
        Fetches multiple movie categories for comprehensive coverage.
        Uses batch layer's approach with rate limiting.
        """
        if not self.tmdb_api_key:
            logger.warning("TMDB API key not set. Using empty cache.")
            return
        
        self.movie_cache = {}
        
        try:
            # Fetch from multiple categories for better coverage
            categories = [
                ('popular', 10),      # Top 200 popular movies
                ('top_rated', 5),     # Top 100 rated movies
                ('now_playing', 3),   # Current ~60 movies
                ('upcoming', 2),      # Upcoming ~40 movies
            ]
            
            total_movies = 0
            
            for category, max_pages in categories:
                logger.info(f"Fetching {category} movies from TMDB...")
                
                for page in range(1, max_pages + 1):
                    # Rate limiting: 4 requests/second (same as batch layer)
                    time.sleep(0.25)
                    
                    try:
                        response = requests.get(
                            f"{self.base_url}/movie/{category}",
                            params={'api_key': self.tmdb_api_key, 'page': page},
                            timeout=10
                        )
                        response.raise_for_status()
                        data = response.json()
                        
                        for movie in data.get('results', []):
                            movie_id = movie['id']
                            title = movie['title']
                            year = None
                            
                            # Extract year from release_date
                            if movie.get('release_date'):
                                try:
                                    year = int(movie['release_date'][:4])
                                except ValueError:
                                    pass
                            
                            # Store in cache
                            normalized = self._normalize_title(title)
                            self.movie_cache[normalized] = (movie_id, title, year)
                            total_movies += 1
                        
                    except requests.RequestException as e:
                        logger.warning(f"Failed to fetch {category} page {page}: {e}")
                        continue
            
            logger.info(f"Loaded {len(self.movie_cache)} unique movies into cache from {total_movies} total results")
            
        except Exception as e:
            logger.error(f"Failed to load TMDB movies: {e}")
    
    def match_title(self, reddit_title: str) -> Optional[Dict[str, any]]:
        """
        Match Reddit movie mention to TMDB movie.
        
        Args:
            reddit_title: Title extracted from Reddit post/comment
            
        Returns:
            Dictionary with match info or None if no match found:
            {
                'tmdb_id': int,
                'tmdb_title': str,
                'year': int or None,
                'similarity': float,
                'reddit_title': str
            }
        """
        if not reddit_title or len(reddit_title) < 2:
            return None
        
        # Refresh cache if needed
        self._refresh_cache_if_needed()
        
        # Extract year from Reddit title
        reddit_year = self._extract_year(reddit_title)
        normalized_reddit = self._normalize_title(reddit_title)
        
        # Try exact match first
        if normalized_reddit in self.movie_cache:
            movie_id, tmdb_title, year = self.movie_cache[normalized_reddit]
            return {
                'tmdb_id': movie_id,
                'tmdb_title': tmdb_title,
                'year': year,
                'similarity': 1.0,
                'reddit_title': reddit_title
            }
        
        # Fuzzy match - find all candidates above threshold
        candidates = []
        
        for cached_title, (movie_id, tmdb_title, year) in self.movie_cache.items():
            similarity = self._calculate_similarity(normalized_reddit, cached_title)
            
            if similarity >= self.similarity_threshold:
                score = similarity
                
                # Boost score if years match
                if reddit_year and year and reddit_year == year:
                    score += 0.15
                
                # CRITICAL: Penalize if numbers don't match (e.g., "Zootopia" vs "Zootopia 2")
                # Extract trailing numbers from both titles
                reddit_num = re.search(r'(\d+)\s*$', normalized_reddit)
                cached_num = re.search(r'(\d+)\s*$', cached_title)
                
                if reddit_num and not cached_num:
                    # Reddit has number (e.g., "zootopia 2") but cache doesn't (e.g., "zootopia")
                    score -= 0.3  # Heavy penalty
                elif not reddit_num and cached_num:
                    # Reddit doesn't have number but cache does
                    score -= 0.2  # Medium penalty
                elif reddit_num and cached_num and reddit_num.group(1) != cached_num.group(1):
                    # Both have numbers but they don't match
                    score -= 0.4  # Very heavy penalty
                
                # Boost recent releases (within last 2 years)
                if year:
                    years_ago = datetime.now().year - year
                    if years_ago <= 2:
                        score += 0.05
                
                candidates.append({
                    'tmdb_id': movie_id,
                    'tmdb_title': tmdb_title,
                    'year': year,
                    'similarity': score,
                    'reddit_title': reddit_title
                })
        
        if not candidates:
            return None
        
        # Sort by similarity score (highest first)
        candidates.sort(key=lambda x: x['similarity'], reverse=True)
        best_match = candidates[0]
        
        if best_match:
            logger.debug(f"Matched '{reddit_title}' to '{best_match['tmdb_title']}' (score: {best_match['similarity']:.2f})")
        
        return best_match
    
    def batch_match(self, titles: List[str]) -> List[Optional[Dict[str, any]]]:
        """
        Match multiple titles in batch.
        
        Args:
            titles: List of Reddit titles
            
        Returns:
            List of match results (same length as input)
        """
        return [self.match_title(title) for title in titles]
    
    def search_tmdb(self, query: str) -> Optional[Dict[str, any]]:
        """
        Search TMDB API directly if cache matching fails.
        
        Args:
            query: Search query
            
        Returns:
            Match info or None
        """
        if not self.tmdb_api_key:
            return None
        
        try:
            response = requests.get(
                f"{self.base_url}/search/movie",
                params={'api_key': self.tmdb_api_key, 'query': query},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            results = data.get('results', [])
            if not results:
                return None
            
            # Return first result
            movie = results[0]
            year = None
            if movie.get('release_date'):
                try:
                    year = int(movie['release_date'][:4])
                except ValueError:
                    pass
            
            return {
                'tmdb_id': movie['id'],
                'tmdb_title': movie['title'],
                'year': year,
                'similarity': 1.0,
                'reddit_title': query
            }
            
        except Exception as e:
            logger.error(f"TMDB search failed for '{query}': {e}")
            return None


# Example usage
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    matcher = MovieMatcher()
    
    test_titles = [
        "The Shawshank Redemption",
        "Dune (2021)",
        "oppenheimer",
        "The godfather part II",
        "not a real movie title xyz123"
    ]
    
    for title in test_titles:
        match = matcher.match_title(title)
        if match:
            print(f"✓ '{title}' -> {match['tmdb_title']} (ID: {match['tmdb_id']}, score: {match['similarity']:.2f})")
        else:
            print(f"✗ '{title}' -> No match")
