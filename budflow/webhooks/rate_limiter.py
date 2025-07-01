"""Webhook rate limiting using Redis."""

import logging
from typing import Optional, Dict, Any

from ..core.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class WebhookRateLimiter:
    """Rate limiter for webhook requests using Redis."""
    
    RATE_LIMIT_PREFIX = "rate_limit"
    CUSTOM_LIMIT_PREFIX = "custom_rate_limit"
    DEFAULT_LIMIT = 100  # Requests per minute
    DEFAULT_WINDOW = 60  # Seconds
    
    def __init__(self, redis_client=None):
        self.redis = redis_client or get_redis_client()
    
    def _get_key(self, webhook_id: str) -> str:
        """Generate rate limit key for webhook."""
        return f"{self.RATE_LIMIT_PREFIX}:{webhook_id}"
    
    def _get_custom_limit_key(self, webhook_id: str) -> str:
        """Generate custom rate limit configuration key."""
        return f"{self.CUSTOM_LIMIT_PREFIX}:{webhook_id}"
    
    async def check(
        self,
        webhook_id: str,
        limit: Optional[int] = None,
        window: Optional[int] = None,
    ) -> bool:
        """Check if request is allowed under rate limit."""
        try:
            # Get custom limits if not provided
            if limit is None or window is None:
                custom = await self.get_custom_limit(webhook_id)
                limit = limit or custom.get("limit", self.DEFAULT_LIMIT)
                window = window or custom.get("window", self.DEFAULT_WINDOW)
            
            key = self._get_key(webhook_id)
            
            # Increment counter
            count = await self.redis.incr(key)
            
            # Set expiration on first request
            if count == 1:
                await self.redis.expire(key, window)
            elif await self.redis.ttl(key) == -1:
                # Key exists but no TTL (shouldn't happen, but handle it)
                await self.redis.expire(key, window)
            
            # Check if limit exceeded
            allowed = count <= limit
            
            if not allowed:
                logger.warning(
                    f"Rate limit exceeded for webhook {webhook_id}: "
                    f"{count}/{limit} in {window}s"
                )
            
            return allowed
            
        except Exception as e:
            logger.error(f"Error checking rate limit: {e}")
            # On error, allow the request (fail open)
            return True
    
    async def get_current_count(self, webhook_id: str) -> int:
        """Get current request count for webhook."""
        try:
            key = self._get_key(webhook_id)
            count = await self.redis.get(key)
            return int(count) if count else 0
            
        except Exception as e:
            logger.error(f"Error getting rate limit count: {e}")
            return 0
    
    async def get_ttl(self, webhook_id: str) -> int:
        """Get remaining TTL for rate limit window."""
        try:
            key = self._get_key(webhook_id)
            ttl = await self.redis.ttl(key)
            return max(0, ttl)
            
        except Exception as e:
            logger.error(f"Error getting rate limit TTL: {e}")
            return 0
    
    async def reset(self, webhook_id: str) -> bool:
        """Reset rate limit counter for webhook."""
        try:
            key = self._get_key(webhook_id)
            result = await self.redis.delete(key)
            
            if result:
                logger.info(f"Reset rate limit for webhook {webhook_id}")
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error resetting rate limit: {e}")
            return False
    
    async def set_custom_limit(
        self,
        webhook_id: str,
        limit: int,
        window: int,
    ) -> bool:
        """Set custom rate limit for webhook."""
        try:
            key = self._get_custom_limit_key(webhook_id)
            data = {"limit": limit, "window": window}
            
            result = await self.redis.hset(key, mapping=data)
            
            logger.info(
                f"Set custom rate limit for webhook {webhook_id}: "
                f"{limit} requests per {window}s"
            )
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error setting custom rate limit: {e}")
            return False
    
    async def get_custom_limit(self, webhook_id: str) -> Dict[str, int]:
        """Get custom rate limit configuration."""
        try:
            key = self._get_custom_limit_key(webhook_id)
            data = await self.redis.hgetall(key)
            
            if data:
                return {
                    "limit": int(data.get(b"limit", self.DEFAULT_LIMIT)),
                    "window": int(data.get(b"window", self.DEFAULT_WINDOW)),
                }
            
            return {"limit": self.DEFAULT_LIMIT, "window": self.DEFAULT_WINDOW}
            
        except Exception as e:
            logger.error(f"Error getting custom rate limit: {e}")
            return {"limit": self.DEFAULT_LIMIT, "window": self.DEFAULT_WINDOW}
    
    async def remove_custom_limit(self, webhook_id: str) -> bool:
        """Remove custom rate limit configuration."""
        try:
            key = self._get_custom_limit_key(webhook_id)
            result = await self.redis.delete(key)
            
            if result:
                logger.info(f"Removed custom rate limit for webhook {webhook_id}")
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error removing custom rate limit: {e}")
            return False
    
    async def get_status(self, webhook_id: str) -> Dict[str, Any]:
        """Get current rate limit status for webhook."""
        try:
            # Get limits
            limits = await self.get_custom_limit(webhook_id)
            
            # Get current count and TTL
            count = await self.get_current_count(webhook_id)
            ttl = await self.get_ttl(webhook_id)
            
            return {
                "webhook_id": webhook_id,
                "current_count": count,
                "limit": limits["limit"],
                "window": limits["window"],
                "remaining": max(0, limits["limit"] - count),
                "reset_in": ttl,
                "is_limited": count >= limits["limit"],
            }
            
        except Exception as e:
            logger.error(f"Error getting rate limit status: {e}")
            return {
                "webhook_id": webhook_id,
                "error": str(e),
            }