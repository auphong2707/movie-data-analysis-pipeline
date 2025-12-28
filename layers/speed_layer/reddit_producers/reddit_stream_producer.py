"""
Reddit Stream Producer for Speed Layer

Polls Reddit API every 30 seconds for new posts and comments from movie subreddits.
Publishes to Kafka topics for real-time processing.

Subreddits monitored: r/movies, r/boxoffice, r/TrueFilm

Usage:
    python reddit_stream_producer.py --subreddits movies boxoffice TrueFilm
"""

import os
import sys
import time
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import argparse

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from movie_matcher import MovieMatcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedditStreamProducer:
    """
    Reddit JSON scraper that streams posts and comments to Kafka.
    
    Features:
    - 30-second polling interval
    - No authentication required (uses .json trick)
    - Automatic retry on failures
    - Deduplication of already-seen posts
    - Rate limiting to avoid aggressive scraping
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        subreddits: List[str],
        user_agent: str = "MovieAnalytics/1.0"
    ):
        """
        Initialize Reddit stream producer.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            subreddits: List of subreddit names to monitor
            user_agent: User agent for requests
        """
        self.subreddits = subreddits
        self.seen_posts = set()  # Track processed post IDs
        self.seen_comments = set()  # Track processed comment IDs
        self.user_agent = user_agent
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
        self.request_count = 0
        self.last_request_time = time.time()
        
        logger.info(f"Initializing Reddit JSON scraper (no auth required)...")
        
        # Initialize TMDB movie matcher
        self.movie_matcher = MovieMatcher()
        logger.info(f"MovieMatcher initialized with {len(self.movie_matcher.movie_cache)} movies")
        
        # Initialize Kafka producer
        logger.info(f"Connecting to Kafka: {kafka_bootstrap_servers}")
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='gzip',
            acks='all',
            retries=3
        )
        
        logger.info(f"Reddit stream producer initialized for subreddits: {subreddits}")
    
    def _extract_movie_titles(self, text: str) -> List[str]:
        """
        Extract and validate movie titles from text using TMDB matching.
        
        Args:
            text: Post title or comment body
            
        Returns:
            List of validated TMDB movie titles
        """
        if not text:
            return []
        
        # Extract quoted strings (common for movie titles)
        import re
        quoted = re.findall(r'"([^"]+)"', text)
        
        # Extract words in title case (potential movie names)
        title_case = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', text)
        
        # Extract multi-word titles (2-6 words)
        multi_word = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z]+){1,5})\b', text)
        
        # Combine candidates
        candidates = list(set(quoted + title_case + multi_word))
        
        # Validate against TMDB
        validated_titles = []
        for candidate in candidates[:20]:  # Check top 20 candidates
            if len(candidate) < 3 or len(candidate) > 100:
                continue
            
            match = self.movie_matcher.match_title(candidate)
            if match and match['similarity'] >= 0.8:
                # Use TMDB canonical title
                validated_titles.append(match['tmdb_title'])
                logger.debug(f"✓ TMDB match: '{candidate}' → '{match['tmdb_title']}' (score: {match['similarity']:.2f})")
            else:
                logger.debug(f"✗ No TMDB match for: '{candidate}'")
        
        result = list(set(validated_titles))[:5]  # Return up to 5 validated titles
        if result:
            logger.info(f"Extracted {len(result)} valid movie titles: {result}")
        return result
    
    def _rate_limit(self):
        """Implement rate limiting: max 1 request per 2 seconds."""
        elapsed = time.time() - self.last_request_time
        if elapsed < 2:
            time.sleep(2 - elapsed)
        self.last_request_time = time.time()
        self.request_count += 1
    
    def fetch_new_posts(self) -> List[Dict[str, Any]]:
        """
        Fetch new posts from monitored subreddits using JSON scraping.
        
        Returns:
            List of post dictionaries
        """
        posts = []
        
        for subreddit_name in self.subreddits:
            try:
                self._rate_limit()
                
                # Use .json trick to get data without authentication
                url = f"https://www.reddit.com/r/{subreddit_name}/new.json?limit=100"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                children = data.get('data', {}).get('children', [])
                
                for child in children:
                    post = child.get('data', {})
                    post_id = post.get('id')
                    
                    if not post_id or post_id in self.seen_posts:
                        continue
                    
                    self.seen_posts.add(post_id)
                    
                    post_data = {
                        'post_id': post_id,
                        'subreddit': subreddit_name,
                        'title': post.get('title', ''),
                        'selftext': post.get('selftext', ''),
                        'author': post.get('author', '[deleted]'),
                        'created_utc': post.get('created_utc', time.time()),
                        'upvotes': post.get('score', 0),
                        'upvote_ratio': post.get('upvote_ratio', 0.5),
                        'num_comments': post.get('num_comments', 0),
                        'awards': post.get('total_awards_received', 0),
                        'url': post.get('url', ''),
                        'is_self': post.get('is_self', False),
                        'potential_movies': self._extract_movie_titles(
                            post.get('title', '') + ' ' + post.get('selftext', '')
                        ),
                        'fetched_at': datetime.now(timezone.utc).isoformat()
                    }
                    
                    posts.append(post_data)
                
                logger.info(f"Fetched {len([p for p in posts if p['subreddit'] == subreddit_name])} new posts from r/{subreddit_name}")
                
            except Exception as e:
                logger.error(f"Error fetching posts from r/{subreddit_name}: {e}")
        
        return posts
    
    def fetch_new_comments(self) -> List[Dict[str, Any]]:
        """
        Fetch new comments from monitored subreddits using JSON scraping.
        
        Returns:
            List of comment dictionaries
        """
        comments = []
        
        for subreddit_name in self.subreddits:
            try:
                self._rate_limit()
                
                # Use .json trick for comments
                url = f"https://www.reddit.com/r/{subreddit_name}/comments.json?limit=100"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                children = data.get('data', {}).get('children', [])
                
                for child in children:
                    comment = child.get('data', {})
                    comment_id = comment.get('id')
                    
                    if not comment_id or comment_id in self.seen_comments:
                        continue
                    
                    self.seen_comments.add(comment_id)
                    
                    comment_data = {
                        'comment_id': comment_id,
                        'post_id': comment.get('link_id', '').replace('t3_', ''),
                        'subreddit': subreddit_name,
                        'body': comment.get('body', ''),
                        'author': comment.get('author', '[deleted]'),
                        'created_utc': comment.get('created_utc', time.time()),
                        'upvotes': comment.get('score', 0),
                        'awards': comment.get('total_awards_received', 0),
                        'parent_id': comment.get('parent_id', ''),
                        'is_submitter': comment.get('is_submitter', False),
                        'potential_movies': self._extract_movie_titles(comment.get('body', '')),
                        'fetched_at': datetime.now(timezone.utc).isoformat()
                    }
                    
                    comments.append(comment_data)
                
                logger.info(f"Fetched {len([c for c in comments if c['subreddit'] == subreddit_name])} new comments from r/{subreddit_name}")
                
            except Exception as e:
                logger.error(f"Error fetching comments from r/{subreddit_name}: {e}")
        
        return comments
    
    def publish_to_kafka(self, posts: List[Dict], comments: List[Dict]):
        """
        Publish Reddit data to Kafka topics.
        
        Args:
            posts: List of post dictionaries
            comments: List of comment dictionaries
        """
        # Publish posts (async, don't wait)
        for post in posts:
            try:
                self.producer.send('reddit.posts', value=post)
            except Exception as e:
                logger.error(f"Failed to publish post {post['post_id']}: {e}")
        
        # Publish comments (async, don't wait)
        for comment in comments:
            try:
                self.producer.send('reddit.comments', value=comment)
            except Exception as e:
                logger.error(f"Failed to publish comment {comment['comment_id']}: {e}")
        
        # Flush to ensure delivery
        self.producer.flush(timeout=5)
        
        logger.info(f"Published {len(posts)} posts and {len(comments)} comments to Kafka")
    
    def run(self, poll_interval: int = 30):
        """
        Run continuous polling loop.
        
        Args:
            poll_interval: Seconds between polls (default: 30)
        """
        logger.info(f"Starting Reddit stream with {poll_interval}s poll interval")
        
ư


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Reddit Stream Producer for Kafka (JSON scraping)')
    parser.add_argument('--subreddits', nargs='+', default=['movies', 'boxoffice', 'TrueFilm'],
                        help='Subreddits to monitor')
    parser.add_argument('--poll-interval', type=int, default=30,
                        help='Polling interval in seconds')
    args = parser.parse_args()
    
    # Get configuration from environment
    user_agent = os.getenv('REDDIT_USER_AGENT', 'MovieAnalytics/1.0')
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    logger.info("Starting Reddit JSON scraper (no API credentials needed)")
    
    # Create and run producer
    producer = RedditStreamProducer(
        kafka_bootstrap_servers=kafka_bootstrap,
        subreddits=args.subreddits,
        user_agent=user_agent
    )
    
    producer.run(poll_interval=args.poll_interval)


if __name__ == '__main__':
    main()
