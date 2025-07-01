"""Test webhook service implementation."""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import Request
from httpx import AsyncClient

from budflow.webhooks import (
    WebhookService,
    WebhookRegistry,
    WebhookPath,
    WebhookMethod,
    WebhookRegistration,
    WebhookResponse,
    WebhookRequest,
    WebhookCache,
    WebhookRateLimiter,
    WebhookError,
    DuplicateWebhookError,
    WebhookNotFoundError,
    RateLimitExceededError,
    WebhookResponseMode,
)
# Workflow models imported but not used - using mocks instead


@pytest.fixture
def redis_client():
    """Create mock Redis client."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.set = AsyncMock(return_value=True)
    client.delete = AsyncMock(return_value=1)
    client.exists = AsyncMock(return_value=False)
    client.expire = AsyncMock(return_value=True)
    client.incr = AsyncMock(return_value=1)
    client.ttl = AsyncMock(return_value=-1)
    client.hset = AsyncMock(return_value=1)
    client.hgetall = AsyncMock(return_value={})
    return client


@pytest.fixture
def webhook_cache(redis_client):
    """Create webhook cache instance."""
    return WebhookCache(redis_client)


@pytest.fixture
def rate_limiter(redis_client):
    """Create rate limiter instance."""
    return WebhookRateLimiter(redis_client)


@pytest.fixture
def webhook_registry():
    """Create webhook registry instance."""
    return WebhookRegistry()


@pytest.fixture
def webhook_service(webhook_registry, webhook_cache, rate_limiter):
    """Create webhook service instance."""
    return WebhookService(
        registry=webhook_registry,
        cache=webhook_cache,
        rate_limiter=rate_limiter,
    )


@pytest.fixture
def sample_workflow():
    """Create sample workflow with webhook trigger."""
    workflow = Mock()
    workflow.id = str(uuid4())
    workflow.name = "Test Webhook Workflow"
    workflow.active = True
    
    # Create webhook node
    webhook_node = Mock()
    webhook_node.id = "webhook_1"
    webhook_node.type = "webhook.trigger"
    webhook_node.name = "Webhook Trigger"
    webhook_node.parameters = {
        "path": "/test-webhook",
        "method": "POST",
        "response_mode": "last_node",
        "response_data": "{{$json}}",
        "options": {
            "binary_property": "data",
            "raw_body": False,
        }
    }
    webhook_node.position = {"x": 100, "y": 100}
    
    # Create code node
    code_node = Mock()
    code_node.id = "code_1"
    code_node.type = "code"
    code_node.name = "Process Data"
    code_node.parameters = {
        "code": "return {processed: true, ...items[0].json}"
    }
    code_node.position = {"x": 300, "y": 100}
    
    workflow.nodes = [webhook_node, code_node]
    workflow.connections = {
        "webhook_1": {
            "main": [[{"node": "code_1", "type": "main", "index": 0}]]
        }
    }
    
    return workflow


@pytest.fixture
def dynamic_workflow():
    """Create workflow with dynamic webhook path."""
    workflow = Mock()
    workflow.id = str(uuid4())
    workflow.name = "Dynamic Webhook Workflow"
    workflow.active = True
    
    # Create webhook node with dynamic path
    webhook_node = Mock()
    webhook_node.id = "webhook_1"
    webhook_node.type = "webhook.trigger"
    webhook_node.name = "Dynamic Webhook"
    webhook_node.parameters = {
        "path": "/users/:userId/webhook",
        "method": "POST",
        "response_mode": "immediate",
        "response_data": '{"status": "ok"}',
    }
    webhook_node.position = {"x": 100, "y": 100}
    
    workflow.nodes = [webhook_node]
    workflow.connections = {}
    
    return workflow


@pytest.mark.unit
class TestWebhookPath:
    """Test webhook path parsing and matching."""
    
    def test_static_path_creation(self):
        """Test creating a static webhook path."""
        path = WebhookPath("/test/webhook")
        
        assert path.path == "/test/webhook"
        assert not path.is_dynamic
        assert path.regex is None
        assert path.parameters == []
    
    def test_dynamic_path_creation(self):
        """Test creating a dynamic webhook path."""
        path = WebhookPath("/users/:userId/items/:itemId")
        
        assert path.path == "/users/:userId/items/:itemId"
        assert path.is_dynamic
        assert path.regex is not None
        assert path.parameters == ["userId", "itemId"]
    
    def test_path_matching(self):
        """Test path matching logic."""
        path = WebhookPath("/users/:userId/items/:itemId")
        
        # Test matching paths
        match = path.match("/users/123/items/456")
        assert match is not None
        assert match["userId"] == "123"
        assert match["itemId"] == "456"
        
        # Test non-matching paths
        assert path.match("/users/123") is None
        assert path.match("/items/456") is None
        assert path.match("/users/123/items/456/extra") is None
    
    def test_uuid_extraction(self):
        """Test UUID extraction from paths."""
        path = WebhookPath("/webhook/:uuid")
        
        uuid_str = str(uuid4())
        match = path.match(f"/webhook/{uuid_str}")
        assert match is not None
        assert match["uuid"] == uuid_str
    
    def test_path_normalization(self):
        """Test path normalization."""
        # Paths should be normalized
        path1 = WebhookPath("/test//webhook/")
        path2 = WebhookPath("test/webhook")
        
        assert path1.path == "/test/webhook"
        assert path2.path == "/test/webhook"


@pytest.mark.unit
class TestWebhookRegistry:
    """Test webhook registry functionality."""
    
    def test_register_static_webhook(self, webhook_registry):
        """Test registering a static webhook."""
        registration = WebhookRegistration(
            id=str(uuid4()),
            workflow_id="workflow_1",
            node_id="node_1",
            path="/test/webhook",
            method=WebhookMethod.POST,
            is_active=True,
        )
        
        webhook_registry.register(registration)
        
        # Check registration exists
        found = webhook_registry.find("/test/webhook", WebhookMethod.POST)
        assert found is not None
        assert found.id == registration.id
        assert found.workflow_id == "workflow_1"
    
    def test_register_duplicate_webhook(self, webhook_registry):
        """Test registering duplicate webhook raises error."""
        registration = WebhookRegistration(
            id=str(uuid4()),
            workflow_id="workflow_1",
            node_id="node_1",
            path="/test/webhook",
            method=WebhookMethod.POST,
            is_active=True,
        )
        
        webhook_registry.register(registration)
        
        # Try to register duplicate
        duplicate = WebhookRegistration(
            id=str(uuid4()),
            workflow_id="workflow_2",
            node_id="node_2",
            path="/test/webhook",
            method=WebhookMethod.POST,
            is_active=True,
        )
        
        with pytest.raises(DuplicateWebhookError):
            webhook_registry.register(duplicate)
    
    def test_unregister_webhook(self, webhook_registry):
        """Test unregistering a webhook."""
        registration = WebhookRegistration(
            id=str(uuid4()),
            workflow_id="workflow_1",
            node_id="node_1",
            path="/test/webhook",
            method=WebhookMethod.POST,
            is_active=True,
        )
        
        webhook_registry.register(registration)
        webhook_registry.unregister(registration.id)
        
        # Check webhook is removed
        found = webhook_registry.find("/test/webhook", WebhookMethod.POST)
        assert found is None
    
    def test_find_dynamic_webhook(self, webhook_registry):
        """Test finding dynamic webhook by path."""
        registration = WebhookRegistration(
            id=str(uuid4()),
            workflow_id="workflow_1",
            node_id="node_1",
            path="/users/:userId/webhook",
            method=WebhookMethod.POST,
            is_active=True,
        )
        
        webhook_registry.register(registration)
        
        # Find by matching path
        found = webhook_registry.find("/users/123/webhook", WebhookMethod.POST)
        assert found is not None
        assert found.id == registration.id
        assert found.path_params == {"userId": "123"}
    
    def test_list_webhooks_by_workflow(self, webhook_registry):
        """Test listing webhooks by workflow ID."""
        # Register multiple webhooks
        registrations = []
        for i in range(3):
            reg = WebhookRegistration(
                id=str(uuid4()),
                workflow_id="workflow_1",
                node_id=f"node_{i}",
                path=f"/webhook/{i}",
                method=WebhookMethod.POST,
                is_active=True,
            )
            webhook_registry.register(reg)
            registrations.append(reg)
        
        # Register webhook for different workflow
        other = WebhookRegistration(
            id=str(uuid4()),
            workflow_id="workflow_2",
            node_id="node_other",
            path="/other/webhook",
            method=WebhookMethod.GET,
            is_active=True,
        )
        webhook_registry.register(other)
        
        # List webhooks for workflow_1
        webhooks = webhook_registry.list_by_workflow("workflow_1")
        assert len(webhooks) == 3
        assert all(w.workflow_id == "workflow_1" for w in webhooks)


@pytest.mark.unit
class TestWebhookCache:
    """Test webhook caching functionality."""
    
    @pytest.mark.asyncio
    async def test_cache_set_and_get(self, webhook_cache, redis_client):
        """Test setting and getting webhook from cache."""
        registration = WebhookRegistration(
            id=str(uuid4()),
            workflow_id="workflow_1",
            node_id="node_1",
            path="/test/webhook",
            method=WebhookMethod.POST,
            is_active=True,
        )
        
        # Cache the registration
        await webhook_cache.set(registration)
        
        # Verify Redis calls
        cache_key = webhook_cache._get_key(registration.path, registration.method)
        redis_client.set.assert_called_once()
        
        # Test getting from cache
        # Use model_dump_json to handle datetime serialization
        redis_client.get.return_value = registration.model_dump_json()
        cached = await webhook_cache.get("/test/webhook", WebhookMethod.POST)
        
        assert cached is not None
        assert cached.id == registration.id
    
    @pytest.mark.asyncio
    async def test_cache_invalidation(self, webhook_cache, redis_client):
        """Test cache invalidation."""
        await webhook_cache.invalidate("/test/webhook", WebhookMethod.POST)
        
        cache_key = webhook_cache._get_key("/test/webhook", WebhookMethod.POST)
        redis_client.delete.assert_called_once_with(cache_key)
    
    @pytest.mark.asyncio
    async def test_cache_ttl(self, webhook_cache, redis_client):
        """Test cache TTL setting."""
        registration = WebhookRegistration(
            id=str(uuid4()),
            workflow_id="workflow_1",
            node_id="node_1",
            path="/test/webhook",
            method=WebhookMethod.POST,
            is_active=True,
        )
        
        await webhook_cache.set(registration, ttl=300)
        
        # Verify set was called with TTL
        redis_client.set.assert_called_once()
        _, call_kwargs = redis_client.set.call_args
        assert call_kwargs['ex'] == 300


@pytest.mark.unit
class TestWebhookRateLimiter:
    """Test webhook rate limiting."""
    
    @pytest.mark.asyncio
    async def test_rate_limit_check(self, rate_limiter, redis_client):
        """Test rate limit checking."""
        webhook_id = str(uuid4())
        
        # First request should pass
        redis_client.incr.return_value = 1
        allowed = await rate_limiter.check(webhook_id, limit=10, window=60)
        assert allowed is True
        
        # Exceeded limit
        redis_client.incr.return_value = 11
        allowed = await rate_limiter.check(webhook_id, limit=10, window=60)
        assert allowed is False
    
    @pytest.mark.asyncio
    async def test_rate_limit_key_expiration(self, rate_limiter, redis_client):
        """Test rate limit key expiration."""
        webhook_id = str(uuid4())
        
        # Key doesn't exist yet
        redis_client.ttl.return_value = -2
        await rate_limiter.check(webhook_id, limit=10, window=60)
        
        # Verify expiration was set
        redis_client.expire.assert_called_with(f"rate_limit:{webhook_id}", 60)
    
    @pytest.mark.asyncio
    async def test_custom_rate_limits(self, rate_limiter, redis_client):
        """Test custom rate limits per webhook."""
        webhook_id = str(uuid4())
        
        # Set custom limit
        await rate_limiter.set_custom_limit(webhook_id, limit=5, window=30)
        
        # Mock the hgetall response
        redis_client.hgetall.return_value = {b"limit": b"5", b"window": b"30"}
        
        # Get custom limit
        limit_info = await rate_limiter.get_custom_limit(webhook_id)
        assert limit_info["limit"] == 5
        assert limit_info["window"] == 30


@pytest.mark.unit
class TestWebhookService:
    """Test main webhook service functionality."""
    
    @pytest.mark.asyncio
    async def test_register_workflow_webhooks(
        self, webhook_service, sample_workflow, redis_client
    ):
        """Test registering webhooks from workflow."""
        await webhook_service.register_workflow_webhooks(sample_workflow)
        
        # Check webhook was registered
        webhook = webhook_service.registry.find("/test-webhook", WebhookMethod.POST)
        assert webhook is not None
        assert webhook.workflow_id == sample_workflow.id
        assert webhook.node_id == "webhook_1"
        
        # Check cache was updated
        redis_client.set.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_unregister_workflow_webhooks(
        self, webhook_service, sample_workflow, redis_client
    ):
        """Test unregistering workflow webhooks."""
        # First register
        await webhook_service.register_workflow_webhooks(sample_workflow)
        
        # Then unregister
        await webhook_service.unregister_workflow_webhooks(sample_workflow.id)
        
        # Check webhook was removed
        webhook = webhook_service.registry.find("/test-webhook", WebhookMethod.POST)
        assert webhook is None
        
        # Check cache was invalidated
        redis_client.delete.assert_called()
    
    @pytest.mark.asyncio
    async def test_handle_webhook_request(
        self, webhook_service, sample_workflow, redis_client
    ):
        """Test handling incoming webhook request."""
        # Register webhook
        await webhook_service.register_workflow_webhooks(sample_workflow)
        
        # Create mock request
        request = Mock(spec=Request)
        request.method = "POST"
        request.url.path = "/test-webhook"
        request.headers = {"content-type": "application/json"}
        request.query_params = {}
        request.client = Mock(host="127.0.0.1")
        
        body = {"test": "data"}
        
        # Update the webhook to use immediate response mode
        # First get the webhook ID
        webhook = webhook_service.registry.find("/test-webhook", WebhookMethod.POST)
        
        # Update it in the registry
        webhook_service.registry.update(
            webhook.id,
            response_mode=WebhookResponseMode.IMMEDIATE,
            response_data='{"status": "received"}',
        )
        
        response = await webhook_service.handle_request(
            path="/test-webhook",
            method="POST",
            request=request,
            body=body,
        )
        
        assert response.status_code == 200
        assert response.body["status"] == "received"
    
    @pytest.mark.asyncio
    async def test_handle_webhook_with_rate_limit(
        self, webhook_service, sample_workflow, rate_limiter
    ):
        """Test webhook rate limiting."""
        # Register webhook
        await webhook_service.register_workflow_webhooks(sample_workflow)
        
        # Mock rate limit exceeded
        rate_limiter.check = AsyncMock(return_value=False)
        
        request = Mock(spec=Request)
        request.method = "POST"
        request.url.path = "/test-webhook"
        
        with pytest.raises(RateLimitExceededError):
            await webhook_service.handle_request(
                path="/test-webhook",
                method="POST",
                request=request,
                body={},
            )
    
    @pytest.mark.asyncio
    async def test_handle_dynamic_webhook(
        self, webhook_service, dynamic_workflow
    ):
        """Test handling dynamic webhook paths."""
        # Register dynamic webhook
        await webhook_service.register_workflow_webhooks(dynamic_workflow)
        
        request = Mock(spec=Request)
        request.method = "POST"
        request.url.path = "/users/123/webhook"
        
        # Mock workflow execution
        with patch(
            "budflow.webhooks.service.execute_workflow"
        ) as mock_execute:
            mock_execute.return_value = {
                "execution_id": str(uuid4()),
                "status": "success",
                "data": [{"json": {"status": "ok"}}],
            }
            
            response = await webhook_service.handle_request(
                path="/users/123/webhook",
                method="POST",
                request=request,
                body={},
            )
            
            # Check path params were passed
            call_args = mock_execute.call_args
            assert call_args[1]["input_data"]["params"]["userId"] == "123"
    
    @pytest.mark.asyncio
    async def test_test_webhook_functionality(
        self, webhook_service, sample_workflow
    ):
        """Test webhook testing functionality."""
        # Register webhook
        await webhook_service.register_workflow_webhooks(sample_workflow)
        
        # Test the webhook
        test_data = {"test": "payload"}
        response = await webhook_service.test_webhook(
            workflow_id=sample_workflow.id,
            node_id="webhook_1",
            test_data=test_data,
        )
        
        assert response["success"] is True
        assert "execution_id" in response
        assert "response" in response
    
    @pytest.mark.asyncio
    async def test_binary_data_handling(
        self, webhook_service, sample_workflow
    ):
        """Test binary data reception in webhooks."""
        # Register webhook
        await webhook_service.register_workflow_webhooks(sample_workflow)
        
        # Create request with binary data
        request = Mock(spec=Request)
        request.method = "POST"
        request.url.path = "/test-webhook"
        request.headers = {"content-type": "application/octet-stream"}
        
        binary_data = b"Binary content here"
        
        # Mock binary data storage
        with patch(
            "budflow.webhooks.service.store_binary_data"
        ) as mock_store:
            mock_store.return_value = "binary_123"
            
            with patch(
                "budflow.webhooks.service.execute_workflow"
            ) as mock_execute:
                mock_execute.return_value = {
                    "execution_id": str(uuid4()),
                    "status": "success",
                    "data": [{"json": {"received": "binary"}}],
                }
                
                response = await webhook_service.handle_request(
                    path="/test-webhook",
                    method="POST",
                    request=request,
                    body=binary_data,
                )
                
                # Check binary data was stored
                mock_store.assert_called_once_with(binary_data)
                
                # Check binary reference was passed to workflow
                call_args = mock_execute.call_args
                assert "binary" in call_args[1]["input_data"]
    
    @pytest.mark.asyncio
    async def test_webhook_authentication(
        self, webhook_service, sample_workflow
    ):
        """Test webhook authentication."""
        # Update workflow with auth settings
        sample_workflow.nodes[0].parameters["authentication"] = {
            "type": "header",
            "header_name": "X-Webhook-Token",
            "token": "secret-token",
        }
        
        # Register webhook
        await webhook_service.register_workflow_webhooks(sample_workflow)
        
        # Test with correct auth
        request = Mock(spec=Request)
        request.method = "POST"
        request.url.path = "/test-webhook"
        request.headers = {"X-Webhook-Token": "secret-token"}
        
        # Should succeed
        with patch("budflow.webhooks.service.execute_workflow"):
            response = await webhook_service.handle_request(
                path="/test-webhook",
                method="POST",
                request=request,
                body={},
            )
            assert response.status_code == 200
        
        # Test with incorrect auth
        request.headers = {"X-Webhook-Token": "wrong-token"}
        response = await webhook_service.handle_request(
            path="/test-webhook",
            method="POST",
            request=request,
            body={},
        )
        assert response.status_code == 401
    
    @pytest.mark.asyncio
    async def test_webhook_response_modes(
        self, webhook_service, sample_workflow
    ):
        """Test different webhook response modes."""
        # Test immediate response mode
        sample_workflow.nodes[0].parameters["response_mode"] = "immediate"
        sample_workflow.nodes[0].parameters["response_data"] = '{"status": "received"}'
        
        await webhook_service.register_workflow_webhooks(sample_workflow)
        
        request = Mock(spec=Request)
        request.method = "POST"
        request.url.path = "/test-webhook"
        
        response = await webhook_service.handle_request(
            path="/test-webhook",
            method="POST",
            request=request,
            body={},
        )
        
        assert response.status_code == 200
        assert response.body == {"status": "received"}
        
        # Test last node response mode
        sample_workflow.nodes[0].parameters["response_mode"] = "last_node"
        await webhook_service.update_webhook(sample_workflow.id, "webhook_1", sample_workflow.nodes[0])
        
        with patch(
            "budflow.webhooks.service.execute_workflow"
        ) as mock_execute:
            mock_execute.return_value = {
                "execution_id": str(uuid4()),
                "status": "success",
                "data": [{"json": {"final": "result"}}],
            }
            
            response = await webhook_service.handle_request(
                path="/test-webhook",
                method="POST",
                request=request,
                body={},
            )
            
            assert response.body == {"final": "result"}


@pytest.mark.integration
class TestWebhookIntegration:
    """Integration tests for webhook system."""
    
    @pytest.mark.asyncio
    async def test_webhook_lifecycle(self, webhook_service):
        """Test complete webhook lifecycle."""
        # Create workflow
        workflow = Workflow(
            id=str(uuid4()),
            name="Integration Test",
            active=True,
            nodes=[
                {
                    "id": "webhook_1",
                    "type": "webhook.trigger",
                    "name": "Test Webhook",
                    "parameters": {
                        "path": "/integration/test",
                        "method": "POST",
                    },
                    "position": {"x": 100, "y": 100},
                }
            ],
            connections={},
        )
        
        # Register webhook
        await webhook_service.register_workflow_webhooks(workflow)
        
        # Verify registration
        webhooks = webhook_service.list_webhooks(workflow.id)
        assert len(webhooks) == 1
        assert webhooks[0]["path"] == "/integration/test"
        
        # Update workflow (deactivate)
        workflow.active = False
        await webhook_service.update_workflow_webhooks(workflow)
        
        # Verify webhook is inactive
        webhook = webhook_service.registry.find("/integration/test", WebhookMethod.POST)
        assert webhook is not None
        assert not webhook.is_active
        
        # Delete workflow webhooks
        await webhook_service.unregister_workflow_webhooks(workflow.id)
        
        # Verify deletion
        webhook = webhook_service.registry.find("/integration/test", WebhookMethod.POST)
        assert webhook is None
    
    @pytest.mark.asyncio
    async def test_concurrent_webhook_handling(self, webhook_service):
        """Test handling concurrent webhook requests."""
        # Create workflow
        workflow = Workflow(
            id=str(uuid4()),
            name="Concurrent Test",
            active=True,
            nodes=[
                {
                    "id": "webhook_1",
                    "type": "webhook.trigger",
                    "name": "Concurrent Webhook",
                    "parameters": {
                        "path": "/concurrent/test",
                        "method": "POST",
                    },
                    "position": {"x": 100, "y": 100},
                }
            ],
            connections={},
        )
        
        await webhook_service.register_workflow_webhooks(workflow)
        
        # Mock workflow execution with delay
        async def delayed_execution(*args, **kwargs):
            await asyncio.sleep(0.1)
            return {
                "execution_id": str(uuid4()),
                "status": "success",
                "data": [{"json": {"processed": True}}],
            }
        
        with patch(
            "budflow.webhooks.service.execute_workflow",
            side_effect=delayed_execution
        ):
            # Send multiple concurrent requests
            request = Mock(spec=Request)
            request.method = "POST"
            request.url.path = "/concurrent/test"
            
            tasks = []
            for i in range(5):
                task = webhook_service.handle_request(
                    path="/concurrent/test",
                    method="POST",
                    request=request,
                    body={"index": i},
                )
                tasks.append(task)
            
            responses = await asyncio.gather(*tasks)
            
            # All should succeed
            assert len(responses) == 5
            assert all(r.status_code == 200 for r in responses)