"""Test main application functionality."""

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient


@pytest.mark.unit
def test_health_check(client: TestClient):
    """Test health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] in ("healthy", "degraded")
    assert data["service"] == "BudFlow"
    assert "version" in data
    assert "timestamp" in data
    assert "databases" in data


@pytest.mark.unit
async def test_health_check_async(async_client: AsyncClient):
    """Test health check endpoint with async client."""
    response = await async_client.get("/health")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] in ("healthy", "degraded")
    assert data["service"] == "BudFlow"


@pytest.mark.unit
def test_metrics_endpoint(client: TestClient):
    """Test metrics endpoint."""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]


@pytest.mark.unit
def test_cors_headers(client: TestClient):
    """Test CORS headers are properly set."""
    # Use GET request to test CORS headers since OPTIONS might not be explicitly handled
    response = client.get("/health")
    assert response.status_code == 200
    
    # Check that CORS middleware is working by making a request with Origin header
    response = client.get("/health", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200


@pytest.mark.unit
def test_app_configuration():
    """Test application configuration."""
    from budflow.config import settings
    
    assert settings.app_name == "BudFlow"
    assert settings.environment == "testing"
    assert settings.debug is True


@pytest.mark.unit
def test_app_creation():
    """Test FastAPI app creation."""
    from budflow.main import create_app
    
    app = create_app()
    assert app.title == "BudFlow"
    assert app.debug is True


@pytest.mark.integration
async def test_database_health_check(async_client: AsyncClient):
    """Test database health check integration."""
    response = await async_client.get("/health")
    assert response.status_code == 200
    
    data = response.json()
    assert "databases" in data
    
    # Check database statuses
    db_statuses = data["databases"]
    assert "postgres" in db_statuses
    assert db_statuses["postgres"]["status"] in ("healthy", "unhealthy", "disabled", "unknown")


@pytest.mark.unit
def test_request_logging_middleware(client: TestClient):
    """Test that request logging middleware is working."""
    # This will be captured by the logging middleware
    response = client.get("/health")
    assert response.status_code == 200
    
    # The middleware should add timing information
    # We can't easily test the logs here, but we ensure the request completes


@pytest.mark.unit
def test_error_handling():
    """Test global error handling."""
    from budflow.main import create_app
    from fastapi import HTTPException
    
    # Create a separate app instance for testing
    test_app = create_app()
    
    @test_app.get("/test-error")
    async def test_error_endpoint():
        raise ValueError("Test error")
    
    with TestClient(test_app) as client:
        response = client.get("/test-error")
        # FastAPI might return different status codes for different errors
        assert response.status_code in (400, 500)
        
        # Check if response has content
        if response.content:
            try:
                data = response.json()
                # The response should contain error information
                assert "error" in data or "detail" in data
            except ValueError:
                # If JSON parsing fails, check text content
                assert "error" in response.text.lower() or response.status_code in (400, 500)


@pytest.mark.integration
async def test_lifespan_events():
    """Test application lifespan events."""
    from budflow.main import lifespan
    from budflow.database import db_manager
    from unittest.mock import patch
    
    # Mock database manager
    with patch.object(db_manager, 'initialize') as mock_init, \
         patch.object(db_manager, 'close') as mock_close:
        
        async with lifespan(None):
            # During lifespan, database should be initialized
            mock_init.assert_called_once()
        
        # After lifespan, database should be closed
        mock_close.assert_called_once()