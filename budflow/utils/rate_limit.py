"""Rate limiting utilities for BudFlow.

This module provides rate limiting functionality for workflows,
API endpoints, and other rate-limited operations.
"""

import time
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from collections import defaultdict

import structlog
from redis import Redis

from budflow.config import settings

logger = structlog.get_logger()


class RateLimiter:
    """Rate limiter using Redis for distributed rate limiting."""
    
    def __init__(self, redis_url: Optional[str] = None):
        """Initialize rate limiter.
        
        Args:
            redis_url: Redis connection URL
        """
        self.redis_url = redis_url or settings.celery_broker_url
        self._redis_client = None
    
    @property
    def redis_client(self) -> Redis:
        """Get Redis client (lazy initialization)."""
        if self._redis_client is None:
            self._redis_client = Redis.from_url(self.redis_url)
        return self._redis_client
    
    def check_rate_limit(
        self,
        key: str,
        limit: int,
        window_seconds: int = 60,
        increment: bool = True
    ) -> bool:
        """Check if rate limit is exceeded.
        
        Args:
            key: Rate limit key
            limit: Maximum number of requests
            window_seconds: Time window in seconds
            increment: Whether to increment the counter
        
        Returns:
            True if within rate limit, False if exceeded
        """
        try:
            # Use Redis sliding window algorithm
            now = time.time()
            window_start = now - window_seconds
            
            # Key for the sorted set
            redis_key = f"rate_limit:{key}"
            
            # Remove old entries
            self.redis_client.zremrangebyscore(redis_key, 0, window_start)
            
            # Count current entries
            current_count = self.redis_client.zcard(redis_key)
            
            if current_count >= limit:
                logger.warning(
                    "Rate limit exceeded",
                    key=key,
                    limit=limit,
                    current_count=current_count
                )
                return False
            
            if increment:
                # Add new entry
                self.redis_client.zadd(redis_key, {str(now): now})
                # Set expiry
                self.redis_client.expire(redis_key, window_seconds + 1)
            
            return True
            
        except Exception as e:
            logger.error(f"Rate limit check failed: {str(e)}")
            # Fail open - allow request if rate limiting fails
            return True
    
    def get_remaining(
        self,
        key: str,
        limit: int,
        window_seconds: int = 60
    ) -> Dict[str, Any]:
        """Get remaining rate limit info.
        
        Args:
            key: Rate limit key
            limit: Maximum number of requests
            window_seconds: Time window in seconds
        
        Returns:
            Rate limit info including remaining requests and reset time
        """
        try:
            now = time.time()
            window_start = now - window_seconds
            redis_key = f"rate_limit:{key}"
            
            # Remove old entries
            self.redis_client.zremrangebyscore(redis_key, 0, window_start)
            
            # Count current entries
            current_count = self.redis_client.zcard(redis_key)
            remaining = max(0, limit - current_count)
            
            # Get oldest entry to calculate reset time
            oldest_entries = self.redis_client.zrange(redis_key, 0, 0, withscores=True)
            if oldest_entries:
                oldest_timestamp = oldest_entries[0][1]
                reset_time = oldest_timestamp + window_seconds
            else:
                reset_time = now + window_seconds
            
            return {
                "limit": limit,
                "remaining": remaining,
                "reset": int(reset_time),
                "reset_after": int(reset_time - now)
            }
            
        except Exception as e:
            logger.error(f"Failed to get rate limit info: {str(e)}")
            return {
                "limit": limit,
                "remaining": limit,
                "reset": int(time.time() + window_seconds),
                "reset_after": window_seconds
            }
    
    def reset(self, key: str) -> bool:
        """Reset rate limit for a key.
        
        Args:
            key: Rate limit key
        
        Returns:
            True if reset successful
        """
        try:
            redis_key = f"rate_limit:{key}"
            self.redis_client.delete(redis_key)
            return True
        except Exception as e:
            logger.error(f"Failed to reset rate limit: {str(e)}")
            return False
    
    def close(self):
        """Close Redis connection."""
        if self._redis_client:
            self._redis_client.close()
            self._redis_client = None


class LocalRateLimiter:
    """In-memory rate limiter for single-instance deployments."""
    
    def __init__(self):
        """Initialize local rate limiter."""
        self.requests = defaultdict(list)
    
    def check_rate_limit(
        self,
        key: str,
        limit: int,
        window_seconds: int = 60,
        increment: bool = True
    ) -> bool:
        """Check if rate limit is exceeded.
        
        Args:
            key: Rate limit key
            limit: Maximum number of requests
            window_seconds: Time window in seconds
            increment: Whether to increment the counter
        
        Returns:
            True if within rate limit, False if exceeded
        """
        now = time.time()
        window_start = now - window_seconds
        
        # Clean old requests
        self.requests[key] = [
            timestamp for timestamp in self.requests[key]
            if timestamp > window_start
        ]
        
        # Check limit
        if len(self.requests[key]) >= limit:
            return False
        
        if increment:
            self.requests[key].append(now)
        
        return True
    
    def get_remaining(
        self,
        key: str,
        limit: int,
        window_seconds: int = 60
    ) -> Dict[str, Any]:
        """Get remaining rate limit info.
        
        Args:
            key: Rate limit key
            limit: Maximum number of requests
            window_seconds: Time window in seconds
        
        Returns:
            Rate limit info
        """
        now = time.time()
        window_start = now - window_seconds
        
        # Clean old requests
        self.requests[key] = [
            timestamp for timestamp in self.requests[key]
            if timestamp > window_start
        ]
        
        current_count = len(self.requests[key])
        remaining = max(0, limit - current_count)
        
        if self.requests[key]:
            oldest_timestamp = min(self.requests[key])
            reset_time = oldest_timestamp + window_seconds
        else:
            reset_time = now + window_seconds
        
        return {
            "limit": limit,
            "remaining": remaining,
            "reset": int(reset_time),
            "reset_after": int(reset_time - now)
        }
    
    def reset(self, key: str) -> bool:
        """Reset rate limit for a key.
        
        Args:
            key: Rate limit key
        
        Returns:
            True if reset successful
        """
        self.requests[key] = []
        return True


# Global rate limiter instance
_rate_limiter = None


def get_rate_limiter() -> RateLimiter:
    """Get global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        if settings.redis_url:
            _rate_limiter = RateLimiter(settings.redis_url)
        else:
            _rate_limiter = LocalRateLimiter()
    return _rate_limiter


def check_rate_limit(
    key: str,
    limit: int,
    window_seconds: int = 60,
    increment: bool = True
) -> bool:
    """Check if rate limit is exceeded.
    
    Args:
        key: Rate limit key
        limit: Maximum number of requests
        window_seconds: Time window in seconds
        increment: Whether to increment the counter
    
    Returns:
        True if within rate limit, False if exceeded
    """
    limiter = get_rate_limiter()
    return limiter.check_rate_limit(key, limit, window_seconds, increment)


def get_rate_limit_info(
    key: str,
    limit: int,
    window_seconds: int = 60
) -> Dict[str, Any]:
    """Get rate limit information.
    
    Args:
        key: Rate limit key
        limit: Maximum number of requests
        window_seconds: Time window in seconds
    
    Returns:
        Rate limit info
    """
    limiter = get_rate_limiter()
    return limiter.get_remaining(key, limit, window_seconds)


def reset_rate_limit(key: str) -> bool:
    """Reset rate limit for a key.
    
    Args:
        key: Rate limit key
    
    Returns:
        True if reset successful
    """
    limiter = get_rate_limiter()
    return limiter.reset(key)


class RateLimitDecorator:
    """Decorator for rate limiting functions."""
    
    def __init__(
        self,
        key_prefix: str,
        limit: int,
        window_seconds: int = 60,
        key_func: Optional[callable] = None
    ):
        """Initialize rate limit decorator.
        
        Args:
            key_prefix: Prefix for rate limit keys
            limit: Maximum number of calls
            window_seconds: Time window in seconds
            key_func: Function to generate key suffix from arguments
        """
        self.key_prefix = key_prefix
        self.limit = limit
        self.window_seconds = window_seconds
        self.key_func = key_func
    
    def __call__(self, func):
        """Decorate function with rate limiting."""
        def wrapper(*args, **kwargs):
            # Generate rate limit key
            if self.key_func:
                key_suffix = self.key_func(*args, **kwargs)
                key = f"{self.key_prefix}:{key_suffix}"
            else:
                key = self.key_prefix
            
            # Check rate limit
            if not check_rate_limit(key, self.limit, self.window_seconds):
                raise RateLimitExceeded(
                    f"Rate limit exceeded for {key}: {self.limit} per {self.window_seconds}s"
                )
            
            return func(*args, **kwargs)
        
        return wrapper


class RateLimitExceeded(Exception):
    """Exception raised when rate limit is exceeded."""
    pass


# Convenience decorators
def rate_limit(limit: int, window_seconds: int = 60, key_func: Optional[callable] = None):
    """Convenience decorator for rate limiting.
    
    Args:
        limit: Maximum number of calls
        window_seconds: Time window in seconds
        key_func: Function to generate key from arguments
    
    Example:
        @rate_limit(100, 60)  # 100 requests per minute
        def api_endpoint():
            pass
        
        @rate_limit(10, 60, lambda user_id: user_id)  # 10 per minute per user
        def user_action(user_id):
            pass
    """
    def decorator(func):
        key_prefix = f"{func.__module__}.{func.__name__}"
        return RateLimitDecorator(key_prefix, limit, window_seconds, key_func)(func)
    
    return decorator