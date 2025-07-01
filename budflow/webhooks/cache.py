"""Webhook caching layer using Redis."""

import json
import logging
from typing import Optional

from ..core.redis_client import get_redis_client
from .models import WebhookMethod, WebhookRegistration

logger = logging.getLogger(__name__)


class WebhookCache:
    """Redis-based cache for webhook registrations."""
    
    CACHE_PREFIX = "webhook:cache"
    DEFAULT_TTL = 3600  # 1 hour
    
    def __init__(self, redis_client=None):
        self.redis = redis_client or get_redis_client()
    
    def _get_key(self, path: str, method: WebhookMethod) -> str:
        """Generate cache key for webhook."""
        return f"{self.CACHE_PREFIX}:{method.value}:{path}"
    
    async def get(
        self, path: str, method: WebhookMethod
    ) -> Optional[WebhookRegistration]:
        """Get webhook registration from cache."""
        try:
            key = self._get_key(path, method)
            data = await self.redis.get(key)
            
            if data:
                registration_data = json.loads(data)
                return WebhookRegistration.model_validate(registration_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting webhook from cache: {e}")
            return None
    
    async def set(
        self,
        registration: WebhookRegistration,
        ttl: Optional[int] = None,
    ) -> bool:
        """Cache webhook registration."""
        try:
            key = self._get_key(registration.path, registration.method)
            data = registration.model_dump_json()
            
            # Set with TTL
            ttl = ttl or self.DEFAULT_TTL
            result = await self.redis.set(key, data, ex=ttl)
            
            if result:
                logger.debug(
                    f"Cached webhook {registration.method} {registration.path} "
                    f"with TTL {ttl}s"
                )
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error caching webhook: {e}")
            return False
    
    async def invalidate(self, path: str, method: WebhookMethod) -> bool:
        """Invalidate cached webhook."""
        try:
            key = self._get_key(path, method)
            result = await self.redis.delete(key)
            
            if result:
                logger.debug(f"Invalidated webhook cache for {method} {path}")
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error invalidating webhook cache: {e}")
            return False
    
    async def invalidate_by_workflow(self, workflow_id: str) -> int:
        """Invalidate all cached webhooks for a workflow."""
        # Note: This requires maintaining a separate index of webhooks by workflow
        # For now, we'll rely on TTL expiration
        logger.warning(
            f"Workflow-based cache invalidation not implemented, "
            f"relying on TTL for workflow {workflow_id}"
        )
        return 0
    
    async def clear(self) -> int:
        """Clear all webhook cache entries."""
        try:
            # Find all webhook cache keys
            pattern = f"{self.CACHE_PREFIX}:*"
            
            count = 0
            async for key in self.redis.scan_iter(match=pattern):
                await self.redis.delete(key)
                count += 1
            
            logger.info(f"Cleared {count} webhook cache entries")
            return count
            
        except Exception as e:
            logger.error(f"Error clearing webhook cache: {e}")
            return 0
    
    async def warm_cache(self, registrations: list[WebhookRegistration]) -> int:
        """Warm the cache with multiple registrations."""
        count = 0
        
        for registration in registrations:
            if await self.set(registration):
                count += 1
        
        logger.info(f"Warmed cache with {count} webhook registrations")
        return count