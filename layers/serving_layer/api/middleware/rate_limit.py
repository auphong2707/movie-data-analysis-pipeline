"""
Rate Limiting Middleware

Implements token bucket algorithm for rate limiting API requests
"""

from fastapi import HTTPException, Request, status
from typing import Dict, Optional
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Token bucket for rate limiting
    """
    
    def __init__(self, capacity: int, refill_rate: float):
        """
        Initialize token bucket
        
        Args:
            capacity: Maximum tokens in bucket
            refill_rate: Tokens added per second
        """
        self.capacity = capacity
        self.tokens = capacity
        self.refill_rate = refill_rate
        self.last_refill = datetime.utcnow()
    
    def consume(self, tokens: int = 1) -> bool:
        """
        Try to consume tokens from bucket
        
        Args:
            tokens: Number of tokens to consume
        
        Returns:
            True if tokens available, False otherwise
        """
        self._refill()
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        
        return False
    
    def _refill(self):
        """Refill tokens based on elapsed time"""
        now = datetime.utcnow()
        elapsed = (now - self.last_refill).total_seconds()
        
        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now
    
    def time_until_available(self, tokens: int = 1) -> float:
        """
        Calculate seconds until requested tokens are available
        
        Args:
            tokens: Number of tokens needed
        
        Returns:
            Seconds to wait
        """
        self._refill()
        
        if self.tokens >= tokens:
            return 0.0
        
        tokens_needed = tokens - self.tokens
        return tokens_needed / self.refill_rate


class RateLimiter:
    """
    Rate limiter using token bucket algorithm
    """
    
    def __init__(
        self,
        requests_per_minute: int = 100,
        burst_size: Optional[int] = None
    ):
        """
        Initialize rate limiter
        
        Args:
            requests_per_minute: Sustained request rate
            burst_size: Maximum burst size (defaults to requests_per_minute)
        """
        self.requests_per_minute = requests_per_minute
        self.burst_size = burst_size or requests_per_minute
        
        # Token buckets per client (keyed by IP or API key)
        self.buckets: Dict[str, TokenBucket] = {}
        
        # Rate = requests per second
        self.refill_rate = requests_per_minute / 60.0
        
        logger.info(
            f"Rate limiter initialized: {requests_per_minute} req/min, "
            f"burst: {self.burst_size}"
        )
    
    def _get_client_id(self, request: Request) -> str:
        """
        Get unique client identifier
        
        Args:
            request: FastAPI request
        
        Returns:
            Client ID (IP address or API key)
        """
        # Try to get API key first
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return f"api_key:{api_key}"
        
        # Fall back to IP address
        client_ip = request.client.host if request.client else "unknown"
        
        # Check for proxy headers
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            client_ip = forwarded_for.split(",")[0].strip()
        
        return f"ip:{client_ip}"
    
    def _get_or_create_bucket(self, client_id: str) -> TokenBucket:
        """
        Get or create token bucket for client
        
        Args:
            client_id: Client identifier
        
        Returns:
            Token bucket for client
        """
        if client_id not in self.buckets:
            self.buckets[client_id] = TokenBucket(
                capacity=self.burst_size,
                refill_rate=self.refill_rate
            )
        
        return self.buckets[client_id]
    
    async def check_rate_limit(self, request: Request) -> bool:
        """
        Check if request is within rate limit
        
        Args:
            request: FastAPI request
        
        Returns:
            True if allowed
        
        Raises:
            HTTPException: If rate limit exceeded
        """
        # Check if rate limiting is enabled
        rate_limit_enabled = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"
        
        if not rate_limit_enabled:
            return True
        
        client_id = self._get_client_id(request)
        bucket = self._get_or_create_bucket(client_id)
        
        # Try to consume a token
        if bucket.consume(1):
            # Add rate limit headers
            request.state.rate_limit_remaining = int(bucket.tokens)
            request.state.rate_limit_limit = self.burst_size
            return True
        
        # Rate limit exceeded
        retry_after = bucket.time_until_available(1)
        
        logger.warning(
            f"Rate limit exceeded for client {client_id}. "
            f"Retry after {retry_after:.1f} seconds"
        )
        
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "Rate limit exceeded",
                "retry_after": retry_after,
                "limit": self.requests_per_minute,
                "window": "1 minute"
            },
            headers={
                "Retry-After": str(int(retry_after) + 1),
                "X-RateLimit-Limit": str(self.requests_per_minute),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(int(retry_after) + 1)
            }
        )
    
    def cleanup_old_buckets(self, max_age_minutes: int = 60):
        """
        Clean up inactive token buckets
        
        Args:
            max_age_minutes: Remove buckets inactive for this long
        """
        cutoff_time = datetime.utcnow() - timedelta(minutes=max_age_minutes)
        
        clients_to_remove = [
            client_id
            for client_id, bucket in self.buckets.items()
            if bucket.last_refill < cutoff_time
        ]
        
        for client_id in clients_to_remove:
            del self.buckets[client_id]
        
        if clients_to_remove:
            logger.info(f"Cleaned up {len(clients_to_remove)} inactive rate limit buckets")


# Global rate limiter instance
_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> RateLimiter:
    """
    Get or create global rate limiter instance
    
    Returns:
        RateLimiter instance
    """
    global _rate_limiter
    
    if _rate_limiter is None:
        requests_per_minute = int(os.getenv("RATE_LIMIT_RPM", "100"))
        burst_size = int(os.getenv("RATE_LIMIT_BURST", str(requests_per_minute)))
        
        _rate_limiter = RateLimiter(
            requests_per_minute=requests_per_minute,
            burst_size=burst_size
        )
    
    return _rate_limiter


# Dependency for FastAPI routes
async def rate_limit_dependency(request: Request):
    """
    FastAPI dependency for rate limiting
    
    Usage:
        @router.get("/endpoint", dependencies=[Depends(rate_limit_dependency)])
    """
    limiter = get_rate_limiter()
    await limiter.check_rate_limit(request)
