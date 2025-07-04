"""Webhook HTTP routes."""

from typing import Any, Dict, Optional, Union

from fastapi import APIRouter, Body, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, PlainTextResponse

from budflow.core.redis_client import get_redis_client
from budflow.webhooks import (
    WebhookCache,
    WebhookRateLimiter,
    WebhookRegistry,
    WebhookService,
)
from budflow.webhooks.exceptions import (
    RateLimitExceededError,
    WebhookError,
    WebhookNotFoundError,
)

# Create router instances
webhook_router = APIRouter()
webhook_test_router = APIRouter()
webhook_waiting_router = APIRouter()


def get_webhook_service() -> WebhookService:
    """Get webhook service instance."""
    redis = get_redis_client()
    cache = WebhookCache(redis)
    rate_limiter = WebhookRateLimiter(redis)
    registry = WebhookRegistry()
    
    return WebhookService(
        registry=registry,
        cache=cache,
        rate_limiter=rate_limiter,
    )


async def handle_webhook_request(
    request: Request,
    path: str,
    method: str,
    service: WebhookService,
    test_mode: bool = False,
) -> Response:
    """Handle incoming webhook request."""
    # Get request body
    body: Any = None
    content_type = request.headers.get("content-type", "").lower()
    
    if request.method != "GET":
        if "application/json" in content_type:
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Invalid JSON payload"
                )
        elif "multipart/form-data" in content_type or "application/x-www-form-urlencoded" in content_type:
            form = await request.form()
            body = dict(form)
        else:
            # Raw body (binary data, text, etc.)
            body = await request.body()
    
    try:
        # Handle the webhook request
        webhook_response = await service.handle_request(
            path=f"/{path}",  # Ensure leading slash
            method=method,
            request=request,
            body=body,
            test_mode=test_mode,
        )
        
        # Build response
        response_class = JSONResponse if isinstance(webhook_response.body, (dict, list)) else PlainTextResponse
        
        return response_class(
            content=webhook_response.body,
            status_code=webhook_response.status_code,
            headers=webhook_response.headers,
        )
        
    except WebhookNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except RateLimitExceededError as e:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(e),
        )
    except WebhookError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


# Production webhook endpoints
@webhook_router.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    tags=["Webhooks"],
    summary="Handle production webhook",
    response_model=None,
)
async def handle_production_webhook(
    request: Request,
    path: str,
    service: WebhookService = Depends(get_webhook_service),
) -> Response:
    """
    Handle production webhook requests.
    
    This endpoint receives webhook calls for active workflows.
    """
    return await handle_webhook_request(
        request=request,
        path=path,
        method=request.method,
        service=service,
        test_mode=False,
    )


# Test webhook endpoints
@webhook_test_router.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    tags=["Webhooks"],
    summary="Handle test webhook",
    response_model=None,
)
async def handle_test_webhook(
    request: Request,
    path: str,
    service: WebhookService = Depends(get_webhook_service),
) -> Response:
    """
    Handle test webhook requests.
    
    This endpoint receives webhook calls during workflow testing.
    Test webhooks have a limited lifetime (typically 120 seconds).
    """
    response = await handle_webhook_request(
        request=request,
        path=path,
        method=request.method,
        service=service,
        test_mode=True,
    )
    
    # Add test mode header
    response.headers["X-Test-Mode"] = "true"
    return response


# Webhook waiting endpoint (for wait nodes)
@webhook_waiting_router.post(
    "/{execution_id}",
    tags=["Webhooks"],
    summary="Resume waiting execution",
    response_model=Dict[str, Any],
)
async def handle_waiting_webhook(
    execution_id: str,
    request: Request,
    body: Optional[Dict[str, Any]] = Body(None),
    service: WebhookService = Depends(get_webhook_service),
) -> Dict[str, Any]:
    """
    Handle webhook for resuming waiting executions.
    
    This endpoint is used by wait nodes to resume workflow execution
    when an external event occurs.
    """
    try:
        webhook_response = await service.handle_waiting_webhook(
            execution_id=execution_id,
            request=request,
            body=body,
        )
        
        if isinstance(webhook_response.body, dict):
            return webhook_response.body
        else:
            return {"status": "resumed", "data": webhook_response.body}
            
    except WebhookNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Execution not found: {str(e)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resume execution: {str(e)}",
        )