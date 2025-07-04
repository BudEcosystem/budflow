"""Main webhook service for managing webhooks."""

import json
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from ..workflows.models import Workflow
from ..workflows.service import WorkflowService
from ..executions.service import ExecutionService
from ..executions.schemas import ExecutionCreate
from .models import (
    WebhookMethod,
    WebhookRegistration,
    WebhookRequest,
    WebhookResponse,
    WebhookAuthentication,
    WebhookAuthType,
    WebhookResponseMode,
)
from .registry import WebhookRegistry
from .cache import WebhookCache
from .rate_limiter import WebhookRateLimiter
from .exceptions import (
    WebhookNotFoundError,
    RateLimitExceededError,
    WebhookAuthenticationError,
)

logger = logging.getLogger(__name__)


class WebhookService:
    """Service for managing webhooks."""
    
    def __init__(
        self,
        registry: Optional[WebhookRegistry] = None,
        cache: Optional[WebhookCache] = None,
        rate_limiter: Optional[WebhookRateLimiter] = None,
        workflow_service: Optional[WorkflowService] = None,
        execution_service: Optional[ExecutionService] = None,
    ):
        self.registry = registry or WebhookRegistry()
        self.cache = cache or WebhookCache()
        self.rate_limiter = rate_limiter or WebhookRateLimiter()
        self.workflow_service = workflow_service
        self.execution_service = execution_service
    
    async def register_workflow_webhooks(self, workflow: Workflow) -> List[str]:
        """Register all webhook triggers in a workflow."""
        webhook_ids = []
        
        # Find all webhook trigger nodes
        for node in workflow.nodes:
            if node.type != "webhook.trigger":
                continue
            
            # Extract webhook configuration
            params = node.parameters
            path = params.get("path", f"/webhook/{node.id}")
            method = WebhookMethod(params.get("method", "POST"))
            
            # Create authentication if configured
            auth = None
            if "authentication" in params:
                auth_config = params["authentication"]
                auth = WebhookAuthentication(
                    type=WebhookAuthType(auth_config.get("type", "none")),
                    header_name=auth_config.get("header_name"),
                    token=auth_config.get("token"),
                    username=auth_config.get("username"),
                    password=auth_config.get("password"),
                    query_param_name=auth_config.get("query_param_name"),
                )
            
            # Create registration
            registration = WebhookRegistration(
                workflow_id=workflow.id,
                node_id=node.id,
                path=path,
                method=method,
                is_active=workflow.active,
                authentication=auth,
                response_mode=WebhookResponseMode(
                    params.get("response_mode", "last_node")
                ),
                response_data=params.get("response_data"),
                response_headers=params.get("response_headers", {}),
                rate_limit=params.get("options", {}).get("rate_limit"),
            )
            
            # Register webhook
            self.registry.register(registration)
            webhook_ids.append(registration.id)
            
            # Cache the registration
            await self.cache.set(registration)
            
            # Set custom rate limit if specified
            if registration.rate_limit:
                await self.rate_limiter.set_custom_limit(
                    registration.id,
                    limit=registration.rate_limit,
                    window=registration.rate_limit_window,
                )
            
            logger.info(
                f"Registered webhook {method} {path} for workflow {workflow.id}"
            )
        
        return webhook_ids
    
    async def unregister_workflow_webhooks(self, workflow_id: str) -> int:
        """Unregister all webhooks for a workflow."""
        webhooks = self.registry.list_by_workflow(workflow_id)
        count = 0
        
        for webhook in webhooks:
            if self.registry.unregister(webhook.id):
                # Invalidate cache
                await self.cache.invalidate(webhook.path, webhook.method)
                
                # Remove custom rate limit
                await self.rate_limiter.remove_custom_limit(webhook.id)
                
                count += 1
        
        logger.info(f"Unregistered {count} webhooks for workflow {workflow_id}")
        return count
    
    async def update_workflow_webhooks(self, workflow: Workflow) -> None:
        """Update webhooks when workflow changes."""
        # Get existing webhooks
        existing = self.registry.list_by_workflow(workflow.id)
        existing_map = {w.node_id: w for w in existing}
        
        # Find current webhook nodes
        current_nodes = {
            node.id: node
            for node in workflow.nodes
            if node.type == "webhook.trigger"
        }
        
        # Remove webhooks for deleted nodes
        for node_id, webhook in existing_map.items():
            if node_id not in current_nodes:
                self.registry.unregister(webhook.id)
                await self.cache.invalidate(webhook.path, webhook.method)
        
        # Update/add webhooks for current nodes
        for node_id, node in current_nodes.items():
            if node_id in existing_map:
                # Update existing webhook
                await self.update_webhook(workflow.id, node_id, node)
            else:
                # Register new webhook
                await self.register_workflow_webhooks(workflow)
        
        # Update active status based on workflow
        if workflow.active:
            self.registry.activate_by_workflow(workflow.id)
        else:
            self.registry.deactivate_by_workflow(workflow.id)
    
    async def update_webhook(
        self,
        workflow_id: str,
        node_id: str,
        node: Any,
    ) -> Optional[WebhookRegistration]:
        """Update a specific webhook."""
        # Find existing webhook
        webhooks = self.registry.list_by_workflow(workflow_id)
        existing = next((w for w in webhooks if w.node_id == node_id), None)
        
        if not existing:
            return None
        
        # Extract new configuration
        params = node.parameters
        updates = {
            "path": params.get("path", existing.path),
            "method": WebhookMethod(params.get("method", existing.method.value)),
            "response_mode": WebhookResponseMode(
                params.get("response_mode", existing.response_mode.value)
            ),
            "response_data": params.get("response_data", existing.response_data),
            "updated_at": datetime.now(timezone.utc),
        }
        
        # Update registration
        updated = self.registry.update(existing.id, **updates)
        
        if updated:
            # Update cache
            await self.cache.set(updated)
            
            logger.info(f"Updated webhook {existing.id}")
        
        return updated
    
    async def handle_request(
        self,
        path: str,
        method: str,
        request: Request,
        body: Union[Dict[str, Any], bytes, None] = None,
        test_mode: bool = False,
    ) -> WebhookResponse:
        """Handle incoming webhook request."""
        webhook_method = WebhookMethod(method.upper())
        
        # Try cache first
        registration = await self.cache.get(path, webhook_method)
        
        # Fall back to registry
        if not registration:
            registration = self.registry.find(path, webhook_method)
            if not registration:
                raise WebhookNotFoundError(path, method)
            
            # Update cache
            await self.cache.set(registration)
        
        # Check if webhook is active
        if not registration.is_active:
            logger.warning(f"Inactive webhook called: {registration.id}")
            return WebhookResponse(status_code=404)
        
        # Check rate limit
        if registration.rate_limit:
            allowed = await self.rate_limiter.check(
                registration.id,
                limit=registration.rate_limit,
                window=registration.rate_limit_window,
            )
            if not allowed:
                status = await self.rate_limiter.get_status(registration.id)
                raise RateLimitExceededError(
                    webhook_id=registration.id,
                    limit=status["limit"],
                    window=status["window"],
                    current_count=status["current_count"],
                )
        
        # Authenticate request
        if registration.authentication:
            await self._authenticate_request(request, registration)
        
        # Handle different response modes
        if registration.response_mode == WebhookResponseMode.IMMEDIATE:
            # Return immediately with configured response
            return self._create_immediate_response(registration)
        
        elif registration.response_mode == WebhookResponseMode.NO_RESPONSE:
            # Queue execution and return 204
            await self._queue_execution(registration, request, body)
            return WebhookResponse(status_code=204)
        
        else:
            # Execute workflow and return result
            return await self._execute_and_respond(registration, request, body)
    
    async def test_webhook(
        self,
        workflow_id: str,
        node_id: str,
        test_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Test a webhook with sample data."""
        # Find webhook
        webhooks = self.registry.list_by_workflow(workflow_id)
        webhook = next((w for w in webhooks if w.node_id == node_id), None)
        
        if not webhook:
            raise WebhookNotFoundError(
                f"Webhook for node {node_id} in workflow {workflow_id}",
                "TEST"
            )
        
        # Create test request
        test_request = WebhookRequest(
            webhook_id=webhook.id,
            workflow_id=webhook.workflow_id,
            node_id=webhook.node_id,
            path=webhook.path,
            method=webhook.method,
            headers={"content-type": "application/json"},
            query_params={},
            path_params=webhook.path_params or {},
            body=test_data or {"test": True},
            timestamp=datetime.now(timezone.utc),
        )
        
        # Execute workflow
        result = await self._execute_workflow(webhook, test_request)
        
        return {
            "success": True,
            "webhook_id": webhook.id,
            "execution_id": result.get("execution_id"),
            "response": result.get("data"),
        }
    
    def list_webhooks(self, workflow_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List webhooks, optionally filtered by workflow."""
        if workflow_id:
            webhooks = self.registry.list_by_workflow(workflow_id)
        else:
            webhooks = self.registry.list_all()
        
        return [
            {
                "id": w.id,
                "workflow_id": w.workflow_id,
                "node_id": w.node_id,
                "path": w.path,
                "method": w.method.value,
                "is_active": w.is_active,
                "response_mode": w.response_mode.value,
                "rate_limit": w.rate_limit,
                "created_at": w.created_at.isoformat(),
                "updated_at": w.updated_at.isoformat(),
            }
            for w in webhooks
        ]
    
    async def get_webhook_status(self, webhook_id: str) -> Dict[str, Any]:
        """Get webhook status including rate limit info."""
        webhook = self.registry.get_by_id(webhook_id)
        if not webhook:
            raise WebhookNotFoundError(f"Webhook {webhook_id}", "GET")
        
        # Get rate limit status
        rate_limit_status = await self.rate_limiter.get_status(webhook_id)
        
        return {
            "webhook": {
                "id": webhook.id,
                "path": webhook.path,
                "method": webhook.method.value,
                "is_active": webhook.is_active,
            },
            "rate_limit": rate_limit_status,
        }
    
    async def _authenticate_request(
        self,
        request: Request,
        registration: WebhookRegistration,
    ) -> None:
        """Authenticate incoming webhook request."""
        auth = registration.authentication
        
        if auth.type == WebhookAuthType.NONE:
            return
        
        elif auth.type == WebhookAuthType.HEADER:
            # Check header authentication
            header_value = request.headers.get(auth.header_name, "")
            if header_value != auth.token:
                raise WebhookAuthenticationError(
                    webhook_id=registration.id,
                    auth_type=auth.type.value,
                    reason="Invalid header token",
                )
        
        elif auth.type == WebhookAuthType.BASIC_AUTH:
            # Check basic authentication
            import base64
            auth_header = request.headers.get("authorization", "")
            
            if not auth_header.startswith("Basic "):
                raise WebhookAuthenticationError(
                    webhook_id=registration.id,
                    auth_type=auth.type.value,
                    reason="Missing basic auth header",
                )
            
            try:
                credentials = base64.b64decode(auth_header[6:]).decode("utf-8")
                username, password = credentials.split(":", 1)
                
                if username != auth.username or password != auth.password:
                    raise WebhookAuthenticationError(
                        webhook_id=registration.id,
                        auth_type=auth.type.value,
                        reason="Invalid credentials",
                    )
            except Exception:
                raise WebhookAuthenticationError(
                    webhook_id=registration.id,
                    auth_type=auth.type.value,
                    reason="Invalid basic auth format",
                )
        
        elif auth.type == WebhookAuthType.QUERY_PARAM:
            # Check query parameter authentication
            param_value = request.query_params.get(auth.query_param_name, "")
            if param_value != auth.token:
                raise WebhookAuthenticationError(
                    webhook_id=registration.id,
                    auth_type=auth.type.value,
                    reason="Invalid query parameter token",
                )
    
    def _create_immediate_response(
        self,
        registration: WebhookRegistration,
    ) -> WebhookResponse:
        """Create immediate response based on configuration."""
        # Parse response data
        if registration.response_data:
            try:
                body = json.loads(registration.response_data)
            except json.JSONDecodeError:
                body = {"message": registration.response_data}
        else:
            body = {"status": "received"}
        
        return WebhookResponse(
            status_code=200,
            headers=registration.response_headers,
            body=body,
        )
    
    async def _queue_execution(
        self,
        registration: WebhookRegistration,
        request: Request,
        body: Any,
    ) -> None:
        """Queue workflow execution without waiting."""
        # Create webhook request
        webhook_request = await self._create_webhook_request(
            registration, request, body
        )
        
        # Queue execution (fire and forget)
        if self.execution_service:
            # Create execution data
            execution_data = ExecutionCreate(
                workflow_id=registration.workflow_id,
                mode="webhook",
                data=webhook_request.model_dump(),
            )
            # Queue execution (don't wait for result)
            import asyncio
            asyncio.create_task(
                self.execution_service.create_execution(
                    execution_data=execution_data,
                    user=None,  # Webhook executions may not have a user
                    trigger_node_id=registration.node_id,
                )
            )
    
    async def _execute_and_respond(
        self,
        registration: WebhookRegistration,
        request: Request,
        body: Any,
    ) -> WebhookResponse:
        """Execute workflow and return response."""
        # Create webhook request
        webhook_request = await self._create_webhook_request(
            registration, request, body
        )
        
        # Execute workflow
        result = await self._execute_workflow(registration, webhook_request)
        
        # Extract response based on mode
        if registration.response_mode == WebhookResponseMode.LAST_NODE:
            # Return data from last executed node
            if result.get("data"):
                response_data = result["data"][-1].get("json", {})
            else:
                response_data = {"execution_id": result.get("execution_id")}
        else:
            # Response node mode - would need to find specific response node
            response_data = {"execution_id": result.get("execution_id")}
        
        return WebhookResponse(
            status_code=200,
            headers=registration.response_headers,
            body=response_data,
        )
    
    async def _create_webhook_request(
        self,
        registration: WebhookRegistration,
        request: Request,
        body: Any,
    ) -> WebhookRequest:
        """Create webhook request object from incoming request."""
        # Handle binary data
        binary_data = None
        if isinstance(body, bytes):
            binary_data = body
            body = None
        
        # Get remote address
        remote_addr = None
        if request.client:
            remote_addr = request.client.host
        
        return WebhookRequest(
            webhook_id=registration.id,
            workflow_id=registration.workflow_id,
            node_id=registration.node_id,
            path=registration.path,
            method=registration.method,
            headers=dict(request.headers),
            query_params=dict(request.query_params),
            path_params=registration.path_params or {},
            body=body,
            binary_data=binary_data,
            remote_addr=remote_addr,
        )
    
    async def _execute_workflow(
        self,
        registration: WebhookRegistration,
        webhook_request: WebhookRequest,
    ) -> Dict[str, Any]:
        """Execute workflow with webhook data."""
        # Prepare input data
        input_data = {
            "headers": webhook_request.headers,
            "params": webhook_request.path_params,
            "query": webhook_request.query_params,
            "body": webhook_request.body,
        }
        
        # Handle binary data
        if webhook_request.binary_data:
            # Store binary data using the processor
            from budflow.webhooks.processor import BinaryDataHandler
            handler = BinaryDataHandler()
            binary_id = await handler.store_binary_data(
                webhook_request.binary_data,
                {
                    "mime_type": webhook_request.headers.get("content-type", "application/octet-stream"),
                    "webhook_id": str(registration.id),
                    "size": len(webhook_request.binary_data)
                }
            )
            input_data["binary"] = {
                "binary_id": str(binary_id),
                "size": len(webhook_request.binary_data),
                "mime_type": webhook_request.headers.get("content-type", "application/octet-stream"),
            }
        
        # Execute workflow
        if self.execution_service:
            # Create execution data
            execution_data = ExecutionCreate(
                workflow_id=registration.workflow_id,
                mode="webhook",
                data=input_data,
            )
            execution = await self.execution_service.create_execution(
                execution_data=execution_data,
                user=None,  # Webhook executions may not have a user
                trigger_node_id=registration.node_id,
            )
            
            return {
                "execution_id": str(execution.id),
                "status": execution.status.value,
                "data": execution.data,
            }
        else:
            # Fallback for testing
            return {
                "execution_id": str(uuid4()),
                "status": "success",
                "data": [{"json": input_data}],
            }
    
    async def handle_waiting_webhook(
        self,
        execution_id: str,
        request: Request,
        body: Optional[Dict[str, Any]] = None,
    ) -> WebhookResponse:
        """
        Handle webhook for resuming waiting executions.
        
        This is used by wait nodes to resume workflow execution
        when an external event occurs.
        
        Args:
            execution_id: The execution ID to resume
            request: The incoming request
            body: The request body data
            
        Returns:
            WebhookResponse with the result
        """
        # TODO: Implement execution resumption logic
        # For now, return a placeholder response
        
        # In a real implementation, this would:
        # 1. Find the waiting execution by ID
        # 2. Verify the execution is in waiting state
        # 3. Resume the execution with the provided data
        # 4. Return the result
        
        return WebhookResponse(
            status_code=200,
            body={
                "resumed": True,
                "execution_id": execution_id,
                "data": body or {},
                "timestamp": datetime.utcnow().isoformat(),
            },
            headers={"Content-Type": "application/json"},
        )