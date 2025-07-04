"""Pytest configuration and fixtures."""

import asyncio
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text

from budflow.main import app
from budflow.config import settings
from budflow.database import Base, get_postgres_session, db_manager


import os

os.environ["ENV_FILE"] = ".env.test"



@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def test_engine():
    """Create test database engine."""
    engine = create_async_engine(
        settings.database_url,
        echo=False,
        pool_pre_ping=True,
    )
    
    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    # Drop all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    await engine.dispose()


@pytest_asyncio.fixture
async def test_session(test_engine):
    """Create test database session."""
    async_session_maker = async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    async with async_session_maker() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def test_app(test_session):
    """Create test FastAPI app with overridden dependencies."""
    
    async def override_get_postgres_session():
        yield test_session
    
    app.dependency_overrides[get_postgres_session] = override_get_postgres_session
    
    yield app
    
    app.dependency_overrides.clear()


@pytest.fixture
def client(test_app):
    """Create test client for synchronous tests."""
    return TestClient(test_app)


@pytest_asyncio.fixture
async def async_client(test_app):
    """Create async test client for async tests."""
    async with AsyncClient(app=test_app, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def test_db_manager():
    """Create test database manager."""
    # Override database URL for testing
    original_url = settings.database_url
    settings.database_url = settings.test_database_url or "sqlite+aiosqlite:///./test_budflow.db"
    
    await db_manager.initialize()
    yield db_manager
    await db_manager.close()
    
    # Restore original URL
    settings.database_url = original_url


# Test data factories
class TestDataFactory:
    """Factory for creating test data."""
    
    @staticmethod
    def user_data(**overrides):
        """Create user test data."""
        data = {
            "email": "test@example.com",
            "password": "testpassword123",
            "first_name": "Test",
            "last_name": "User",
            "is_active": True,
        }
        data.update(overrides)
        return data
    
    @staticmethod
    def workflow_data(**overrides):
        """Create workflow test data."""
        data = {
            "name": "Test Workflow",
            "description": "A test workflow",
            "is_active": True,
            "nodes": [],
            "edges": [],
        }
        data.update(overrides)
        return data
    
    @staticmethod
    def credential_data(**overrides):
        """Create credential test data."""
        data = {
            "name": "Test Credential",
            "type": "api_key",
            "data": {"api_key": "test_key_123"},
        }
        data.update(overrides)
        return data


@pytest.fixture
def test_data():
    """Provide test data factory."""
    return TestDataFactory


# Markers for different test types
pytest.mark.unit = pytest.mark.unit
pytest.mark.integration = pytest.mark.integration
pytest.mark.e2e = pytest.mark.e2e
pytest.mark.slow = pytest.mark.slow
pytest.mark.ai = pytest.mark.ai


# Helper functions for testing
class TestHelpers:
    """Helper functions for tests."""
    
    @staticmethod
    async def create_test_user(session: AsyncSession, **overrides):
        """Create a test user in the database."""
        from budflow.auth.models import User
        
        user_data = TestDataFactory.user_data(**overrides)
        user = User(**user_data)
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user
    
    @staticmethod
    async def create_test_workflow(session: AsyncSession, user_id: int, **overrides):
        """Create a test workflow in the database."""
        from budflow.workflows.models import Workflow
        
        workflow_data = TestDataFactory.workflow_data(**overrides)
        workflow_data["user_id"] = user_id
        workflow = Workflow(**workflow_data)
        session.add(workflow)
        await session.commit()
        await session.refresh(workflow)
        return workflow
    
    @staticmethod
    async def authenticate_user(client: AsyncClient, email: str, password: str):
        """Authenticate a user and return auth token."""
        response = await client.post(
            "/api/v1/auth/login",
            json={"email": email, "password": password}
        )
        assert response.status_code == 200
        return response.json()["access_token"]
    
    @staticmethod
    def auth_headers(token: str):
        """Get authorization headers."""
        return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def helpers():
    """Provide test helpers."""
    return TestHelpers


# Mock external services
@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    from unittest.mock import Mock
    
    mock_client = Mock()
    mock_client.ping.return_value = True
    mock_client.get.return_value = None
    mock_client.set.return_value = True
    mock_client.delete.return_value = 1
    
    return mock_client


@pytest.fixture
def mock_mongodb():
    """Mock MongoDB client."""
    from unittest.mock import Mock
    
    mock_client = Mock()
    mock_db = Mock()
    mock_collection = Mock()
    
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection
    mock_client.admin.command.return_value = {"ok": 1}
    
    return mock_client


@pytest.fixture
def mock_external_apis():
    """Mock external API calls."""
    import httpx
    from unittest.mock import patch
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success"}
        mock_response.text = '{"status": "success"}'
        
        mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
        mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
        
        yield mock_client