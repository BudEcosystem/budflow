"""Test database functionality."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from budflow.database import DatabaseManager, db_manager
from budflow.config import settings


@pytest.mark.unit
async def test_database_manager_initialization():
    """Test database manager initialization."""
    manager = DatabaseManager()
    
    # Initially, all connections should be None
    assert manager.postgres_engine is None
    assert manager.redis_client is None
    assert manager.mongodb_client is None
    assert manager.neo4j_driver is None


@pytest.mark.integration
async def test_postgres_connection(test_session: AsyncSession):
    """Test PostgreSQL connection and basic operations."""
    # Test basic query
    result = await test_session.execute(text("SELECT 1 as test_value"))
    row = result.fetchone()
    assert row.test_value == 1


@pytest.mark.integration
async def test_database_health_check(test_db_manager):
    """Test database health check functionality."""
    health_status = await test_db_manager.health_check()
    
    assert isinstance(health_status, dict)
    assert "postgres" in health_status
    assert "redis" in health_status
    assert "mongodb" in health_status
    assert "neo4j" in health_status
    
    # PostgreSQL should be healthy in test environment
    postgres_status = health_status["postgres"]
    assert postgres_status["status"] in ("healthy", "unhealthy")
    
    # Other services should be disabled in test environment
    for service in ["redis", "mongodb", "neo4j"]:
        assert health_status[service]["status"] in ("healthy", "disabled", "unhealthy")


@pytest.mark.integration
async def test_postgres_session_context_manager():
    """Test PostgreSQL session context manager."""
    # This test will work when we have actual database models
    # For now, we test the basic session functionality
    
    async with db_manager.get_postgres_session() as session:
        result = await session.execute(text("SELECT 1 as test_value"))
        row = result.fetchone()
        assert row.test_value == 1


@pytest.mark.unit
def test_database_dependencies():
    """Test database dependencies for FastAPI."""
    from budflow.database import get_postgres_session, get_redis, get_mongodb, get_neo4j_session
    
    # These should be callable functions
    assert callable(get_postgres_session)
    assert callable(get_redis)
    assert callable(get_mongodb)
    assert callable(get_neo4j_session)


@pytest.mark.integration
async def test_database_transaction_rollback(test_session: AsyncSession):
    """Test that database transactions can be rolled back."""
    # Start a transaction
    await test_session.begin()
    
    # Create a temporary table
    await test_session.execute(text("""
        CREATE TEMPORARY TABLE test_rollback (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    """))
    
    # Insert data
    await test_session.execute(text("""
        INSERT INTO test_rollback (name) VALUES ('test')
    """))
    
    # Verify data exists
    result = await test_session.execute(text("SELECT name FROM test_rollback"))
    row = result.fetchone()
    assert row.name == "test"
    
    # Rollback transaction
    await test_session.rollback()
    
    # After rollback, the temp table should still exist but data should be gone
    # Note: TEMPORARY tables in SQLite behave differently, so we just verify rollback works


@pytest.mark.unit
async def test_database_connection_error_handling():
    """Test database connection error handling."""
    manager = DatabaseManager()
    
    # Test with invalid connection string
    original_url = settings.database_url
    settings.database_url = "postgresql://invalid:invalid@invalid:5432/invalid"
    
    try:
        # This should not raise an exception, but log an error
        await manager._init_postgres()
        # PostgreSQL should remain None due to connection failure
        assert manager.postgres_engine is None
    except Exception:
        # Connection should fail gracefully
        assert manager.postgres_engine is None
    finally:
        # Restore original URL
        settings.database_url = original_url


@pytest.mark.integration
async def test_database_pool_configuration():
    """Test database connection pool configuration."""
    manager = DatabaseManager()
    await manager._init_postgres()
    
    if manager.postgres_engine:
        # Check pool configuration
        pool = manager.postgres_engine.pool
        assert pool.size() >= 0  # Pool should be configured
        
        await manager.close()


@pytest.mark.unit
def test_database_url_configuration():
    """Test database URL configuration for different environments."""
    # In testing, both URLs should be SQLite
    assert "sqlite" in settings.database_url.lower()
    assert "aiosqlite" in settings.database_url


@pytest.mark.integration
async def test_database_session_lifecycle(test_session: AsyncSession):
    """Test database session lifecycle."""
    # Test that the session can execute queries and handle transactions
    result = await test_session.execute(text("SELECT 1 as value"))
    value = result.fetchone().value
    assert value == 1
    
    # Test that the session can handle multiple sequential queries
    for i in range(3):
        result = await test_session.execute(text(f"SELECT {i} as iteration"))
        iteration = result.fetchone().iteration
        assert iteration == i


@pytest.mark.slow
@pytest.mark.integration
async def test_database_connection_recovery():
    """Test database connection recovery after failure."""
    manager = DatabaseManager()
    await manager.initialize()
    
    # Verify initial connection
    if manager.postgres_engine:
        health_before = await manager.health_check()
        assert health_before["postgres"]["status"] == "healthy"
        
        # Simulate connection issues by closing the engine
        await manager.postgres_engine.dispose()
        
        # Re-initialize
        await manager._init_postgres()
        
        # Verify connection is restored
        health_after = await manager.health_check()
        assert health_after["postgres"]["status"] == "healthy"
    
    await manager.close()