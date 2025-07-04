"""Test webhook HTTP routes."""

import json
from datetime import datetime
from typing import Dict, Any
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import AsyncClient

from budflow.webhooks import WebhookService, WebhookResponse, WebhookResponseMode
from budflow.webhooks.models import WebhookRegistration, WebhookMethod
from budflow.webhooks.exceptions import WebhookNotFoundError, RateLimitExceededError


@pytest.fixture
def mock_webhook_service():
    """Create mock webhook service."""
    service = AsyncMock(spec=WebhookService)
    return service


@pytest.fixture
def app_with_webhook_routes(mock_webhook_service):
    """Create FastAPI app with webhook routes."""
    from budflow.main import create_app
    from budflow.webhooks.routes import get_webhook_service
    
    app = create_app()
    
    # Override the webhook service dependency
    app.dependency_overrides[get_webhook_service] = lambda: mock_webhook_service
    
    return app


@pytest.fixture
def test_client(app_with_webhook_routes):
    """Create test client."""
    return TestClient(app_with_webhook_routes)


@pytest.fixture
async def async_client(app_with_webhook_routes):
    """Create async test client."""
    async with AsyncClient(app=app_with_webhook_routes, base_url="http://test") as client:
        yield client


@pytest.mark.unit
class TestWebhookRoutes:
    """Test webhook HTTP routes."""
    
    def test_webhook_post_request(self, test_client, mock_webhook_service):
        """Test POST request to webhook endpoint."""
        # Setup mock response
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"status": "success"},
            headers={"Content-Type": "application/json"}
        )
        
        # Make request
        response = test_client.post(
            "/webhook/test-path",
            json={"test": "data"}
        )
        
        # Verify response
        if response.status_code != 200:
            print(f"Error response: {response.json()}")
        assert response.status_code == 200
        assert response.json() == {"status": "success"}
        
        # Verify service was called correctly
        mock_webhook_service.handle_request.assert_called_once()
        call_args = mock_webhook_service.handle_request.call_args
        assert call_args[1]["path"] == "/test-path"
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["body"] == {"test": "data"}
    
    def test_webhook_get_request(self, test_client, mock_webhook_service):
        """Test GET request to webhook endpoint."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"result": "data"}
        )
        
        response = test_client.get("/webhook/get-endpoint?param=value")
        
        assert response.status_code == 200
        assert response.json() == {"result": "data"}
        
        # Verify query params were passed
        call_args = mock_webhook_service.handle_request.call_args
        assert call_args[1]["path"] == "/get-endpoint"
        assert call_args[1]["method"] == "GET"
    
    def test_webhook_put_request(self, test_client, mock_webhook_service):
        """Test PUT request to webhook endpoint."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"updated": True}
        )
        
        response = test_client.put(
            "/webhook/update-resource",
            json={"id": 123, "data": "updated"}
        )
        
        assert response.status_code == 200
        assert response.json() == {"updated": True}
    
    def test_webhook_delete_request(self, test_client, mock_webhook_service):
        """Test DELETE request to webhook endpoint."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=204,
            body=None
        )
        
        response = test_client.delete("/webhook/delete-resource/123")
        
        assert response.status_code == 204
    
    def test_webhook_patch_request(self, test_client, mock_webhook_service):
        """Test PATCH request to webhook endpoint."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"patched": True}
        )
        
        response = test_client.patch(
            "/webhook/patch-resource",
            json={"field": "new_value"}
        )
        
        assert response.status_code == 200
        assert response.json() == {"patched": True}
    
    def test_webhook_not_found(self, test_client, mock_webhook_service):
        """Test webhook not found error."""
        mock_webhook_service.handle_request.side_effect = WebhookNotFoundError(
            path="/non-existent",
            method="POST"
        )
        
        response = test_client.post("/webhook/non-existent")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()
    
    def test_webhook_rate_limit_exceeded(self, test_client, mock_webhook_service):
        """Test rate limit exceeded error."""
        mock_webhook_service.handle_request.side_effect = RateLimitExceededError(
            webhook_id="webhook-123",
            limit=10,
            window=60,
            current_count=11
        )
        
        response = test_client.post("/webhook/rate-limited")
        
        assert response.status_code == 429
        assert "rate limit" in response.json()["detail"].lower()
    
    def test_webhook_with_headers(self, test_client, mock_webhook_service):
        """Test webhook request with custom headers."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"headers": "received"}
        )
        
        response = test_client.post(
            "/webhook/with-headers",
            json={"data": "test"},
            headers={
                "X-Custom-Header": "custom-value",
                "Authorization": "Bearer token123"
            }
        )
        
        assert response.status_code == 200
        
        # Verify headers were passed to service
        call_args = mock_webhook_service.handle_request.call_args
        request = call_args[1]["request"]
        assert request.headers.get("x-custom-header") == "custom-value"
        assert request.headers.get("authorization") == "Bearer token123"
    
    def test_webhook_with_binary_data(self, test_client, mock_webhook_service):
        """Test webhook with binary data."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"binary": "received"}
        )
        
        binary_data = b"Binary content here"
        response = test_client.post(
            "/webhook/binary-upload",
            content=binary_data,
            headers={"Content-Type": "application/octet-stream"}
        )
        
        assert response.status_code == 200
        
        # Verify binary data was passed
        call_args = mock_webhook_service.handle_request.call_args
        assert call_args[1]["body"] == binary_data
    
    def test_webhook_with_form_data(self, test_client, mock_webhook_service):
        """Test webhook with form data."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"form": "processed"}
        )
        
        response = test_client.post(
            "/webhook/form-submit",
            data={"field1": "value1", "field2": "value2"}
        )
        
        assert response.status_code == 200
    
    def test_webhook_custom_response_headers(self, test_client, mock_webhook_service):
        """Test webhook returning custom response headers."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"data": "test"},
            headers={
                "X-Custom-Response": "custom-value",
                "Cache-Control": "no-cache"
            }
        )
        
        response = test_client.post("/webhook/custom-response")
        
        assert response.status_code == 200
        assert response.headers["x-custom-response"] == "custom-value"
        assert response.headers["cache-control"] == "no-cache"


@pytest.mark.unit
class TestWebhookTestRoutes:
    """Test webhook test endpoints."""
    
    def test_webhook_test_post_request(self, test_client, mock_webhook_service):
        """Test POST request to webhook test endpoint."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"test": True, "status": "success"}
        )
        
        response = test_client.post(
            "/webhook-test/test-path",
            json={"test": "data"}
        )
        
        assert response.status_code == 200
        assert response.json()["test"] is True
        
        # Verify test mode was passed
        call_args = mock_webhook_service.handle_request.call_args
        assert call_args[1]["test_mode"] is True
    
    def test_webhook_test_timeout(self, test_client, mock_webhook_service):
        """Test webhook test mode timeout behavior."""
        # Test webhooks should have limited lifetime
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"status": "test_success"},
            headers={"X-Test-Mode": "true"}
        )
        
        response = test_client.get("/webhook-test/timeout-test")
        
        assert response.status_code == 200
        assert response.headers.get("x-test-mode") == "true"


@pytest.mark.unit
class TestWebhookPathParameters:
    """Test webhook path parameter handling."""
    
    def test_dynamic_path_single_param(self, test_client, mock_webhook_service):
        """Test dynamic path with single parameter."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"userId": "123"}
        )
        
        response = test_client.post("/webhook/users/123/webhook")
        
        assert response.status_code == 200
        
        # Path should be normalized
        call_args = mock_webhook_service.handle_request.call_args
        assert call_args[1]["path"] == "/users/123/webhook"
    
    def test_dynamic_path_multiple_params(self, test_client, mock_webhook_service):
        """Test dynamic path with multiple parameters."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"userId": "123", "orderId": "456"}
        )
        
        response = test_client.post("/webhook/users/123/orders/456/status")
        
        assert response.status_code == 200
        
        call_args = mock_webhook_service.handle_request.call_args
        assert call_args[1]["path"] == "/users/123/orders/456/status"
    
    def test_webhook_path_with_special_chars(self, test_client, mock_webhook_service):
        """Test webhook path with special characters."""
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={"received": True}
        )
        
        # Test URL-encoded path
        response = test_client.post("/webhook/path%20with%20spaces")
        
        assert response.status_code == 200
        
        # Path should be decoded
        call_args = mock_webhook_service.handle_request.call_args
        assert call_args[1]["path"] == "/path with spaces"


@pytest.mark.unit
class TestWebhookErrorHandling:
    """Test webhook error handling."""
    
    def test_webhook_internal_error(self, test_client, mock_webhook_service):
        """Test internal server error handling."""
        mock_webhook_service.handle_request.side_effect = Exception(
            "Internal error"
        )
        
        response = test_client.post("/webhook/error-test")
        
        assert response.status_code == 500
        assert "internal" in response.json()["detail"].lower()
    
    def test_webhook_invalid_json(self, test_client, mock_webhook_service):
        """Test invalid JSON payload handling."""
        response = test_client.post(
            "/webhook/invalid-json",
            content=b"Invalid JSON{",
            headers={"Content-Type": "application/json"}
        )
        
        # FastAPI should handle JSON parsing errors
        assert response.status_code == 422
    
    def test_webhook_method_not_allowed(self, test_client, mock_webhook_service):
        """Test method not allowed handling."""
        # If webhook only accepts POST, OPTIONS should fail
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=405,
            body={"error": "Method not allowed"}
        )
        
        response = test_client.options("/webhook/post-only")
        
        assert response.status_code == 405


@pytest.mark.unit
class TestWebhookWaitingEndpoint:
    """Test webhook waiting endpoint for wait nodes."""
    
    def test_webhook_waiting_endpoint(self, test_client, mock_webhook_service):
        """Test webhook waiting endpoint."""
        mock_webhook_service.handle_waiting_webhook.return_value = WebhookResponse(
            status_code=200,
            body={"resumed": True}
        )
        
        response = test_client.post(
            "/webhook-waiting/execution-123",
            json={"resume": "data"}
        )
        
        assert response.status_code == 200
        assert response.json()["resumed"] is True
        
        # Verify execution ID was passed
        mock_webhook_service.handle_waiting_webhook.assert_called_once()
        call_args = mock_webhook_service.handle_waiting_webhook.call_args
        assert call_args.kwargs["execution_id"] == "execution-123"


@pytest.mark.integration
class TestWebhookRoutesIntegration:
    """Integration tests for webhook routes."""
    
    @pytest.mark.asyncio
    async def test_webhook_full_flow(self, async_client, mock_webhook_service):
        """Test complete webhook flow."""
        # Setup workflow execution mock
        execution_id = str(uuid4())
        mock_webhook_service.handle_request.return_value = WebhookResponse(
            status_code=200,
            body={
                "execution_id": execution_id,
                "status": "success",
                "data": {"processed": True}
            }
        )
        
        # Make async request
        response = await async_client.post(
            "/webhook/integration-test",
            json={"input": "data"}
        )
        
        assert response.status_code == 200
        result = response.json()
        assert result["execution_id"] == execution_id
        assert result["status"] == "success"
        assert result["data"]["processed"] is True
    
    @pytest.mark.asyncio
    async def test_concurrent_webhook_requests(self, async_client, mock_webhook_service):
        """Test handling concurrent webhook requests."""
        # Setup mock to return different responses
        call_count = 0
        
        async def mock_handler(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return WebhookResponse(
                status_code=200,
                body={"request_number": call_count}
            )
        
        mock_webhook_service.handle_request.side_effect = mock_handler
        
        # Send multiple concurrent requests
        import asyncio
        tasks = []
        for i in range(5):
            task = async_client.post(
                f"/webhook/concurrent-{i}",
                json={"index": i}
            )
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks)
        
        # All should succeed
        assert len(responses) == 5
        assert all(r.status_code == 200 for r in responses)
        assert call_count == 5