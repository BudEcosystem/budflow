"""Webhook system exceptions."""

from typing import Optional, Dict, Any


class WebhookError(Exception):
    """Base exception for webhook-related errors."""
    
    def __init__(
        self,
        message: str,
        webhook_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.webhook_id = webhook_id
        self.details = details or {}


class DuplicateWebhookError(WebhookError):
    """Raised when attempting to register a duplicate webhook."""
    
    def __init__(
        self,
        path: str,
        method: str,
        existing_workflow_id: str,
        new_workflow_id: str,
    ):
        message = (
            f"Webhook {method} {path} is already registered "
            f"for workflow {existing_workflow_id}"
        )
        super().__init__(
            message,
            details={
                "path": path,
                "method": method,
                "existing_workflow_id": existing_workflow_id,
                "new_workflow_id": new_workflow_id,
            },
        )


class WebhookNotFoundError(WebhookError):
    """Raised when a webhook is not found."""
    
    def __init__(self, path: str, method: str):
        message = f"Webhook {method} {path} not found"
        super().__init__(
            message,
            details={
                "path": path,
                "method": method,
            },
        )


class RateLimitExceededError(WebhookError):
    """Raised when webhook rate limit is exceeded."""
    
    def __init__(
        self,
        webhook_id: str,
        limit: int,
        window: int,
        current_count: int,
    ):
        message = (
            f"Rate limit exceeded for webhook {webhook_id}: "
            f"{current_count}/{limit} requests in {window}s"
        )
        super().__init__(
            message,
            webhook_id=webhook_id,
            details={
                "limit": limit,
                "window": window,
                "current_count": current_count,
            },
        )


class WebhookAuthenticationError(WebhookError):
    """Raised when webhook authentication fails."""
    
    def __init__(
        self,
        webhook_id: str,
        auth_type: str,
        reason: str,
    ):
        message = f"Webhook authentication failed: {reason}"
        super().__init__(
            message,
            webhook_id=webhook_id,
            details={
                "auth_type": auth_type,
                "reason": reason,
            },
        )