"""Webhook processing system for BudFlow.

This module handles incoming webhook requests, processes data,
manages binary content, and triggers workflow executions.
"""

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from ipaddress import ip_address, ip_network
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

import structlog
from multipart import parse_form_data

from budflow.config import settings
from budflow.database import get_db
from budflow.webhooks.service import WebhookService
from budflow.core.binary_data import BinaryDataManager
from budflow.workflows.tasks import execute_workflow_task
from budflow.utils.rate_limit import check_rate_limit
from budflow.metrics import metrics

logger = structlog.get_logger()


class WebhookProcessingError(Exception):
    """Base exception for webhook processing errors."""
    pass


@dataclass
class WebhookRequest:
    """Webhook request data."""
    webhook_id: UUID
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, str]
    body: Any
    raw_body: bytes
    client_ip: str


@dataclass
class WebhookResponse:
    """Webhook response data."""
    status_code: int
    body: Dict[str, Any]
    headers: Dict[str, str]
    
    def __post_init__(self):
        """Ensure headers include content type."""
        if "Content-Type" not in self.headers:
            self.headers["Content-Type"] = "application/json"
    
    @classmethod
    def success(
        cls,
        data: Optional[Dict[str, Any]] = None,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None
    ) -> "WebhookResponse":
        """Create success response."""
        body = {
            "success": True,
            "data": data or {},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        return cls(
            status_code=status_code,
            body=body,
            headers=headers or {}
        )
    
    @classmethod
    def error(
        cls,
        message: str,
        status_code: int = 400,
        details: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> "WebhookResponse":
        """Create error response."""
        body = {
            "success": False,
            "error": message,
            "details": details or {},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        return cls(
            status_code=status_code,
            body=body,
            headers=headers or {}
        )


class BinaryDataHandler:
    """Handles binary data in webhook requests."""
    
    def __init__(self):
        """Initialize binary data handler."""
        self.binary_manager = BinaryDataManager()
    
    async def store_binary_data(
        self,
        data: bytes,
        metadata: Dict[str, Any]
    ) -> UUID:
        """Store binary data and return reference.
        
        Args:
            data: Binary data to store
            metadata: Metadata about the binary data
        
        Returns:
            Binary data ID
        """
        return await self.binary_manager.store(
            data=data,
            filename=metadata.get("filename"),
            content_type=metadata.get("content_type"),
            metadata=metadata
        )
    
    async def extract_binary_parts(
        self,
        body: bytes,
        content_type: str
    ) -> Dict[str, Any]:
        """Extract binary parts from multipart data.
        
        Args:
            body: Request body
            content_type: Content type header
        
        Returns:
            Dictionary mapping field names to binary references
        """
        binary_parts = {}
        
        if not content_type.startswith("multipart/"):
            return binary_parts
        
        try:
            # Parse multipart data
            # In production, use proper multipart parser
            # This is a simplified implementation
            if "boundary=" in content_type:
                boundary = content_type.split("boundary=")[1].strip()
                parts = body.split(f"--{boundary}".encode())
                
                for part in parts[1:-1]:  # Skip first and last
                    if b"\r\n\r\n" in part:
                        headers, content = part.split(b"\r\n\r\n", 1)
                        
                        # Parse headers
                        header_dict = {}
                        for header_line in headers.split(b"\r\n"):
                            if b":" in header_line:
                                key, value = header_line.split(b":", 1)
                                header_dict[key.decode().lower()] = value.decode().strip()
                        
                        # Check if it's a file
                        content_disposition = header_dict.get("content-disposition", "")
                        if "filename=" in content_disposition:
                            # Extract field name and filename
                            field_name = None
                            filename = None
                            
                            for param in content_disposition.split(";"):
                                param = param.strip()
                                if param.startswith("name="):
                                    field_name = param[5:].strip('"')
                                elif param.startswith("filename="):
                                    filename = param[9:].strip('"')
                            
                            if field_name and content:
                                # Store binary data
                                binary_id = await self.store_binary_data(
                                    content.rstrip(b"\r\n"),
                                    {
                                        "filename": filename,
                                        "content_type": header_dict.get("content-type", "application/octet-stream"),
                                        "field_name": field_name
                                    }
                                )
                                
                                binary_parts[field_name] = {
                                    "binary_id": str(binary_id),
                                    "filename": filename,
                                    "size": len(content)
                                }
        
        except Exception as e:
            logger.error(f"Failed to parse multipart data: {str(e)}")
        
        return binary_parts


class WebhookCache:
    """Caches webhook data for performance."""
    
    def __init__(self, ttl_seconds: int = 300):
        """Initialize webhook cache.
        
        Args:
            ttl_seconds: Cache TTL in seconds
        """
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[UUID, tuple[Dict[str, Any], float]] = {}
    
    async def get_webhook(self, webhook_id: UUID) -> Optional[Dict[str, Any]]:
        """Get webhook from cache.
        
        Args:
            webhook_id: Webhook ID
        
        Returns:
            Cached webhook data or None
        """
        if webhook_id in self._cache:
            data, expiry = self._cache[webhook_id]
            if time.time() < expiry:
                return data
            else:
                del self._cache[webhook_id]
        return None
    
    async def set_webhook(self, webhook_id: UUID, data: Dict[str, Any]) -> None:
        """Cache webhook data.
        
        Args:
            webhook_id: Webhook ID
            data: Webhook data to cache
        """
        expiry = time.time() + self.ttl_seconds
        self._cache[webhook_id] = (data, expiry)
    
    async def invalidate_webhook(self, webhook_id: UUID) -> None:
        """Invalidate cached webhook.
        
        Args:
            webhook_id: Webhook ID
        """
        self._cache.pop(webhook_id, None)


class WebhookProcessor:
    """Main webhook processor."""
    
    def __init__(self):
        """Initialize webhook processor."""
        self.webhook_service = None
        self.binary_handler = BinaryDataHandler()
        self.cache = WebhookCache()
    
    async def process_webhook(self, request: WebhookRequest) -> WebhookResponse:
        """Process incoming webhook request.
        
        Args:
            request: Webhook request data
        
        Returns:
            Webhook response
        """
        start_time = time.time()
        
        try:
            # Initialize webhook service if needed
            if not self.webhook_service:
                async for session in get_db():
                    self.webhook_service = WebhookService(session)
                    break
            
            # Get webhook configuration
            webhook = await self._get_webhook(request.webhook_id)
            if not webhook or not webhook.is_active:
                return WebhookResponse.error(
                    "Webhook not found or inactive",
                    status_code=404
                )
            
            # Validate request
            validation_result = await self._validate_request(request, webhook)
            if not validation_result["valid"]:
                return WebhookResponse.error(
                    validation_result["error"],
                    status_code=validation_result["status_code"]
                )
            
            # Process binary data if present
            processed_body = await self._process_binary_data(request)
            
            # Trigger workflow execution
            execution_result = await self._trigger_workflow(
                webhook,
                request,
                processed_body
            )
            
            # Update webhook stats
            await self._update_stats(
                webhook.id,
                success=True,
                response_time=time.time() - start_time
            )
            
            # Build response
            return self._build_response(webhook, execution_result)
            
        except Exception as e:
            logger.error(
                "Webhook processing failed",
                webhook_id=str(request.webhook_id),
                error=str(e),
                exc_info=True
            )
            
            # Update stats for failure
            if hasattr(self, 'webhook_service') and self.webhook_service:
                await self._update_stats(
                    request.webhook_id,
                    success=False,
                    response_time=time.time() - start_time
                )
            
            return WebhookResponse.error(
                "Internal server error",
                status_code=500
            )
    
    async def _get_webhook(self, webhook_id: UUID) -> Optional[Any]:
        """Get webhook configuration.
        
        Args:
            webhook_id: Webhook ID
        
        Returns:
            Webhook object or None
        """
        # Check cache first
        cached = await self.cache.get_webhook(webhook_id)
        if cached:
            return cached
        
        # Get from database
        webhook = await self.webhook_service.get_webhook(webhook_id)
        
        # Cache if found
        if webhook:
            await self.cache.set_webhook(webhook_id, webhook)
        
        return webhook
    
    async def _validate_request(
        self,
        request: WebhookRequest,
        webhook: Any
    ) -> Dict[str, Any]:
        """Validate webhook request.
        
        Args:
            request: Webhook request
            webhook: Webhook configuration
        
        Returns:
            Validation result
        """
        settings = webhook.settings or {}
        
        # Check allowed methods
        allowed_methods = settings.get("allowed_methods", ["POST", "GET"])
        if request.method not in allowed_methods:
            return {
                "valid": False,
                "error": f"Method not allowed. Allowed: {', '.join(allowed_methods)}",
                "status_code": 405
            }
        
        # Check signature if required
        if settings.get("require_signature"):
            secret = settings.get("secret")
            signature = request.headers.get("X-Webhook-Signature", "")
            
            if not self._validate_signature(request.raw_body, signature, secret):
                return {
                    "valid": False,
                    "error": "Invalid signature",
                    "status_code": 401
                }
        
        # Check IP whitelist
        allowed_ips = settings.get("allowed_ips", [])
        if allowed_ips and not self._check_ip_allowed(request.client_ip, allowed_ips):
            return {
                "valid": False,
                "error": "IP not allowed",
                "status_code": 403
            }
        
        # Check rate limit
        rate_limit = settings.get("rate_limit", 1000)
        rate_limit_key = f"webhook:{webhook.id}:requests"
        
        if not check_rate_limit(rate_limit_key, rate_limit, window_seconds=60):
            return {
                "valid": False,
                "error": "Rate limit exceeded",
                "status_code": 429
            }
        
        return {"valid": True}
    
    def _validate_signature(
        self,
        payload: bytes,
        signature: str,
        secret: str
    ) -> bool:
        """Validate webhook signature.
        
        Args:
            payload: Request payload
            signature: Provided signature
            secret: Webhook secret
        
        Returns:
            True if signature is valid
        """
        expected = self._generate_signature(payload, secret)
        return hmac.compare_digest(signature, expected)
    
    def _generate_signature(self, payload: bytes, secret: str) -> str:
        """Generate webhook signature.
        
        Args:
            payload: Request payload
            secret: Webhook secret
        
        Returns:
            HMAC signature
        """
        return hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256
        ).hexdigest()
    
    def _check_ip_allowed(self, client_ip: str, allowed_ips: List[str]) -> bool:
        """Check if client IP is allowed.
        
        Args:
            client_ip: Client IP address
            allowed_ips: List of allowed IPs or CIDR ranges
        
        Returns:
            True if IP is allowed
        """
        try:
            client = ip_address(client_ip)
            
            for allowed in allowed_ips:
                try:
                    # Check if it's a CIDR range
                    if "/" in allowed:
                        network = ip_network(allowed, strict=False)
                        if client in network:
                            return True
                    else:
                        # Direct IP comparison
                        if client == ip_address(allowed):
                            return True
                except ValueError:
                    logger.warning(f"Invalid IP/CIDR in allowed list: {allowed}")
            
            return False
            
        except ValueError:
            logger.error(f"Invalid client IP: {client_ip}")
            return False
    
    async def _process_binary_data(self, request: WebhookRequest) -> Dict[str, Any]:
        """Process binary data in request.
        
        Args:
            request: Webhook request
        
        Returns:
            Processed body with binary references
        """
        content_type = request.headers.get("Content-Type", "")
        
        if content_type.startswith("multipart/"):
            # Extract and store binary parts
            binary_parts = await self.binary_handler.extract_binary_parts(
                request.raw_body,
                content_type
            )
            
            # Merge with regular body
            processed = request.body.copy() if isinstance(request.body, dict) else {}
            processed.update(binary_parts)
            return processed
        
        return request.body
    
    async def _trigger_workflow(
        self,
        webhook: Any,
        request: WebhookRequest,
        processed_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Trigger workflow execution.
        
        Args:
            webhook: Webhook configuration
            request: Webhook request
            processed_body: Processed request body
        
        Returns:
            Execution result
        """
        # Prepare trigger data
        trigger_data = {
            "type": "webhook",
            "webhook_id": str(webhook.id),
            "method": request.method,
            "path": request.path,
            "headers": request.headers,
            "query_params": request.query_params,
            "client_ip": request.client_ip,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Queue workflow execution
        task = execute_workflow_task.apply_async(
            kwargs={
                "workflow_id": str(webhook.workflow_id),
                "trigger_data": trigger_data,
                "initial_data": processed_body,
                "execution_mode": "webhook"
            }
        )
        
        return {
            "workflow_execution_id": task.id,
            "workflow_id": str(webhook.workflow_id),
            "queued_at": datetime.now(timezone.utc).isoformat()
        }
    
    async def _update_stats(
        self,
        webhook_id: UUID,
        success: bool,
        response_time: float
    ) -> None:
        """Update webhook statistics.
        
        Args:
            webhook_id: Webhook ID
            success: Whether processing succeeded
            response_time: Response time in seconds
        """
        try:
            await self.webhook_service.update_webhook_stats(
                webhook_id,
                success=success,
                response_time=response_time
            )
            
            # Update metrics
            metrics.increment(
                "webhook_requests",
                tags={
                    "webhook_id": str(webhook_id),
                    "status": "success" if success else "failure"
                }
            )
            metrics.timing(
                "webhook_response_time",
                response_time * 1000,  # Convert to milliseconds
                tags={"webhook_id": str(webhook_id)}
            )
            
        except Exception as e:
            logger.error(f"Failed to update webhook stats: {str(e)}")
    
    def _build_response(
        self,
        webhook: Any,
        execution_result: Dict[str, Any]
    ) -> WebhookResponse:
        """Build webhook response.
        
        Args:
            webhook: Webhook configuration
            execution_result: Workflow execution result
        
        Returns:
            Webhook response
        """
        settings = webhook.settings or {}
        response_template = settings.get("response_template")
        
        if response_template:
            # Use custom response template
            body = response_template.copy()
            body["workflow_execution_id"] = execution_result["workflow_execution_id"]
        else:
            # Default response
            body = {
                "success": True,
                "message": "Webhook processed successfully",
                "workflow_execution_id": execution_result["workflow_execution_id"]
            }
        
        return WebhookResponse(
            status_code=200,
            body=body,
            headers={}
        )


# Global webhook processor instance
_webhook_processor = None


def get_webhook_processor() -> WebhookProcessor:
    """Get global webhook processor instance."""
    global _webhook_processor
    if _webhook_processor is None:
        _webhook_processor = WebhookProcessor()
    return _webhook_processor