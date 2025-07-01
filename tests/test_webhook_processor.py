"""Tests for webhook processing system."""

import pytest
import json
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone
from uuid import UUID, uuid4

from budflow.webhooks.processor import (
    WebhookProcessor,
    WebhookRequest,
    WebhookResponse,
    WebhookProcessingError,
    BinaryDataHandler,
    WebhookCache,
)


@pytest.fixture
def webhook_processor():
    """Create webhook processor instance."""
    return WebhookProcessor()


@pytest.fixture
def sample_webhook_request():
    """Create sample webhook request."""
    return WebhookRequest(
        webhook_id=uuid4(),
        method="POST",
        path="/webhook/test-webhook",
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Signature": "test-signature",
            "User-Agent": "WebhookTest/1.0"
        },
        query_params={"token": "test-token"},
        body={"event": "test.created", "data": {"id": 123, "name": "Test"}},
        raw_body=b'{"event":"test.created","data":{"id":123,"name":"Test"}}',
        client_ip="192.168.1.100"
    )


@pytest.fixture
def mock_webhook_service():
    """Mock webhook service."""
    service = AsyncMock()
    service.get_webhook = AsyncMock()
    service.update_webhook_stats = AsyncMock()
    return service


@pytest.fixture
def mock_binary_handler():
    """Mock binary data handler."""
    handler = AsyncMock()
    handler.store_binary_data = AsyncMock()
    handler.extract_binary_parts = AsyncMock()
    return handler


class TestWebhookProcessor:
    """Test webhook processor."""
    
    @pytest.mark.asyncio
    async def test_process_webhook_success(
        self,
        webhook_processor,
        sample_webhook_request,
        mock_webhook_service
    ):
        """Test successful webhook processing."""
        # Setup mock webhook
        webhook = Mock(
            id=sample_webhook_request.webhook_id,
            workflow_id=uuid4(),
            is_active=True,
            settings={
                "require_signature": False,
                "allowed_methods": ["POST", "GET"],
                "timeout": 30
            }
        )
        mock_webhook_service.get_webhook.return_value = webhook
        
        with patch.object(webhook_processor, 'webhook_service', mock_webhook_service):
            with patch("budflow.webhooks.processor.execute_workflow_task") as mock_execute:
                mock_execute.apply_async.return_value = Mock(id="task-123")
                
                response = await webhook_processor.process_webhook(sample_webhook_request)
                
                assert response.status_code == 200
                assert response.body["success"] is True
                assert "workflow_execution_id" in response.body
                mock_execute.apply_async.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_webhook_inactive(
        self,
        webhook_processor,
        sample_webhook_request,
        mock_webhook_service
    ):
        """Test processing inactive webhook."""
        webhook = Mock(
            id=sample_webhook_request.webhook_id,
            is_active=False
        )
        mock_webhook_service.get_webhook.return_value = webhook
        
        with patch.object(webhook_processor, 'webhook_service', mock_webhook_service):
            response = await webhook_processor.process_webhook(sample_webhook_request)
            
            assert response.status_code == 404
            assert response.body["error"] == "Webhook not found or inactive"
    
    @pytest.mark.asyncio
    async def test_process_webhook_method_not_allowed(
        self,
        webhook_processor,
        sample_webhook_request,
        mock_webhook_service
    ):
        """Test webhook with disallowed method."""
        webhook = Mock(
            id=sample_webhook_request.webhook_id,
            is_active=True,
            settings={"allowed_methods": ["GET"]}
        )
        mock_webhook_service.get_webhook.return_value = webhook
        sample_webhook_request.method = "POST"
        
        with patch.object(webhook_processor, 'webhook_service', mock_webhook_service):
            response = await webhook_processor.process_webhook(sample_webhook_request)
            
            assert response.status_code == 405
            assert "Method not allowed" in response.body["error"]
    
    @pytest.mark.asyncio
    async def test_validate_signature(self, webhook_processor):
        """Test webhook signature validation."""
        secret = "webhook-secret-123"
        payload = b'{"test": "data"}'
        
        # Generate valid signature
        valid_signature = webhook_processor._generate_signature(payload, secret)
        
        # Test valid signature
        assert webhook_processor._validate_signature(
            payload, valid_signature, secret
        ) is True
        
        # Test invalid signature
        assert webhook_processor._validate_signature(
            payload, "invalid-signature", secret
        ) is False
    
    @pytest.mark.asyncio
    async def test_process_binary_data(
        self,
        webhook_processor,
        mock_binary_handler
    ):
        """Test processing webhook with binary data."""
        # Multipart request
        request = WebhookRequest(
            webhook_id=uuid4(),
            method="POST",
            path="/webhook/test",
            headers={
                "Content-Type": "multipart/form-data; boundary=----WebKitFormBoundary"
            },
            body={
                "text_field": "value",
                "file_field": b"binary file content"
            },
            raw_body=b"multipart data",
            client_ip="192.168.1.100"
        )
        
        mock_binary_handler.extract_binary_parts.return_value = {
            "file_field": {"binary_id": str(uuid4()), "size": 19}
        }
        
        with patch.object(webhook_processor, 'binary_handler', mock_binary_handler):
            processed = await webhook_processor._process_binary_data(request)
            
            assert "file_field" in processed
            assert "binary_id" in processed["file_field"]
            mock_binary_handler.extract_binary_parts.assert_called_once()


class TestBinaryDataHandler:
    """Test binary data handler."""
    
    @pytest.mark.asyncio
    async def test_store_binary_data(self):
        """Test storing binary data."""
        handler = BinaryDataHandler()
        data = b"test binary content"
        metadata = {
            "filename": "test.pdf",
            "content_type": "application/pdf"
        }
        
        with patch("budflow.core.binary_data.BinaryDataManager") as mock_manager:
            mock_manager.return_value.store.return_value = uuid4()
            
            binary_id = await handler.store_binary_data(data, metadata)
            
            assert isinstance(binary_id, UUID)
            mock_manager.return_value.store.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_extract_multipart_data(self):
        """Test extracting multipart form data."""
        handler = BinaryDataHandler()
        
        # Simplified multipart data
        boundary = "----WebKitFormBoundary"
        body = f"""------{boundary}
Content-Disposition: form-data; name="text"

Hello World
------{boundary}
Content-Disposition: form-data; name="file"; filename="test.txt"
Content-Type: text/plain

File content here
------{boundary}--""".encode()
        
        parts = await handler.extract_binary_parts(
            body,
            f"multipart/form-data; boundary=----{boundary}"
        )
        
        # Should extract file parts
        assert len(parts) > 0


class TestWebhookCache:
    """Test webhook caching."""
    
    @pytest.mark.asyncio
    async def test_cache_webhook(self):
        """Test caching webhook data."""
        cache = WebhookCache()
        webhook_id = uuid4()
        webhook_data = {
            "id": str(webhook_id),
            "workflow_id": str(uuid4()),
            "settings": {"timeout": 30}
        }
        
        # Cache webhook
        await cache.set_webhook(webhook_id, webhook_data)
        
        # Retrieve from cache
        cached = await cache.get_webhook(webhook_id)
        assert cached == webhook_data
    
    @pytest.mark.asyncio
    async def test_cache_expiry(self):
        """Test cache expiry."""
        cache = WebhookCache(ttl_seconds=1)
        webhook_id = uuid4()
        
        await cache.set_webhook(webhook_id, {"test": "data"})
        
        # Should exist immediately
        assert await cache.get_webhook(webhook_id) is not None
        
        # Wait for expiry
        import asyncio
        await asyncio.sleep(1.1)
        
        # Should be expired
        assert await cache.get_webhook(webhook_id) is None
    
    @pytest.mark.asyncio
    async def test_invalidate_cache(self):
        """Test cache invalidation."""
        cache = WebhookCache()
        webhook_id = uuid4()
        
        await cache.set_webhook(webhook_id, {"test": "data"})
        assert await cache.get_webhook(webhook_id) is not None
        
        # Invalidate
        await cache.invalidate_webhook(webhook_id)
        assert await cache.get_webhook(webhook_id) is None


class TestWebhookResponse:
    """Test webhook response formatting."""
    
    def test_success_response(self):
        """Test creating success response."""
        response = WebhookResponse.success(
            data={"result": "processed"},
            status_code=201
        )
        
        assert response.status_code == 201
        assert response.body["success"] is True
        assert response.body["data"]["result"] == "processed"
        assert response.headers["Content-Type"] == "application/json"
    
    def test_error_response(self):
        """Test creating error response."""
        response = WebhookResponse.error(
            message="Invalid request",
            status_code=400,
            details={"field": "missing"}
        )
        
        assert response.status_code == 400
        assert response.body["success"] is False
        assert response.body["error"] == "Invalid request"
        assert response.body["details"]["field"] == "missing"
    
    def test_custom_headers(self):
        """Test response with custom headers."""
        response = WebhookResponse(
            status_code=200,
            body={"test": "data"},
            headers={
                "X-Custom-Header": "value",
                "X-Request-ID": "123"
            }
        )
        
        assert response.headers["X-Custom-Header"] == "value"
        assert response.headers["X-Request-ID"] == "123"
        assert response.headers["Content-Type"] == "application/json"


class TestWebhookProcessorIntegration:
    """Integration tests for webhook processor."""
    
    @pytest.mark.asyncio
    async def test_full_webhook_flow(
        self,
        webhook_processor,
        mock_webhook_service
    ):
        """Test complete webhook processing flow."""
        # Create webhook with all features
        webhook = Mock(
            id=uuid4(),
            workflow_id=uuid4(),
            is_active=True,
            settings={
                "require_signature": True,
                "secret": "test-secret",
                "allowed_methods": ["POST"],
                "allowed_ips": ["192.168.1.0/24"],
                "rate_limit": 100,
                "response_template": {
                    "success": True,
                    "message": "Webhook received"
                }
            }
        )
        mock_webhook_service.get_webhook.return_value = webhook
        
        # Create request with signature
        body = b'{"event": "test"}'
        signature = webhook_processor._generate_signature(body, "test-secret")
        
        request = WebhookRequest(
            webhook_id=webhook.id,
            method="POST",
            path=f"/webhook/{webhook.id}",
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Signature": signature
            },
            query_params={},
            body=json.loads(body),
            raw_body=body,
            client_ip="192.168.1.100"
        )
        
        with patch.object(webhook_processor, 'webhook_service', mock_webhook_service):
            with patch("budflow.webhooks.processor.execute_workflow_task") as mock_execute:
                mock_execute.apply_async.return_value = Mock(id="task-123")
                
                response = await webhook_processor.process_webhook(request)
                
                assert response.status_code == 200
                assert response.body["success"] is True
                assert response.body["message"] == "Webhook received"
    
    @pytest.mark.asyncio
    async def test_webhook_error_handling(
        self,
        webhook_processor,
        sample_webhook_request,
        mock_webhook_service
    ):
        """Test webhook error handling."""
        # Webhook service throws error
        mock_webhook_service.get_webhook.side_effect = Exception("Database error")
        
        with patch.object(webhook_processor, 'webhook_service', mock_webhook_service):
            response = await webhook_processor.process_webhook(sample_webhook_request)
            
            assert response.status_code == 500
            assert response.body["success"] is False
            assert "Internal server error" in response.body["error"]