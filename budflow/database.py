"""Database configuration and connection management."""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorClient
from neo4j import AsyncGraphDatabase, AsyncDriver
from redis.asyncio import Redis
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from budflow.config import settings

logger = structlog.get_logger()


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class DatabaseManager:
    """Database connection manager for all databases."""

    def __init__(self):
        self.postgres_engine: Optional[AsyncEngine] = None
        self.async_session_maker: Optional[async_sessionmaker[AsyncSession]] = None
        self.redis_client: Optional[Redis] = None
        self.mongodb_client: Optional[AsyncIOMotorClient] = None
        self.neo4j_driver: Optional[AsyncDriver] = None

    async def initialize(self) -> None:
        """Initialize all database connections."""
        logger.info("Initializing database connections...")

        # Initialize PostgreSQL
        await self._init_postgres()

        # Initialize Redis
        await self._init_redis()

        # Initialize MongoDB
        await self._init_mongodb()

        # Initialize Neo4j
        await self._init_neo4j()

        logger.info("All database connections initialized successfully")

    async def _init_postgres(self) -> None:
        """Initialize PostgreSQL connection."""
        try:
            self.postgres_engine = create_async_engine(
                settings.database_url,
                pool_size=settings.database_pool_size,
                max_overflow=settings.database_max_overflow,
                echo=settings.database_echo,
                pool_pre_ping=True,
                pool_recycle=3600,  # Recycle connections every hour
            )

            # Create session maker
            self.async_session_maker = async_sessionmaker(
                self.postgres_engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            # Test connection
            async with self.postgres_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))

            logger.info("PostgreSQL connection established")

        except Exception as e:
            logger.error("Failed to initialize PostgreSQL", error=str(e))
            raise

    async def _init_redis(self) -> None:
        """Initialize Redis connection."""
        try:
            self.redis_client = Redis.from_url(
                settings.redis_url,
                password=settings.redis_password,
                max_connections=settings.redis_max_connections,
                decode_responses=True,
                retry_on_timeout=True,
                health_check_interval=30,
            )

            # Test connection
            await self.redis_client.ping()

            logger.info("Redis connection established")

        except Exception as e:
            logger.warning("Redis not available, skipping initialization", error=str(e))
            self.redis_client = None

    async def _init_mongodb(self) -> None:
        """Initialize MongoDB connection."""
        try:
            self.mongodb_client = AsyncIOMotorClient(
                settings.mongodb_url,
                username=settings.mongodb_username,
                password=settings.mongodb_password,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
            )

            # Test connection
            await self.mongodb_client.admin.command("ping")

            logger.info("MongoDB connection established")

        except Exception as e:
            logger.warning("MongoDB not available, skipping initialization", error=str(e))
            self.mongodb_client = None

    async def _init_neo4j(self) -> None:
        """Initialize Neo4j connection."""
        try:
            self.neo4j_driver = AsyncGraphDatabase.driver(
                settings.neo4j_uri,
                auth=(settings.neo4j_username, settings.neo4j_password),
                max_connection_lifetime=3600,
                max_connection_pool_size=50,
                connection_acquisition_timeout=60,
            )

            # Test connection
            await self.neo4j_driver.verify_connectivity()

            logger.info("Neo4j connection established")

        except Exception as e:
            logger.warning("Neo4j not available, skipping initialization", error=str(e))
            self.neo4j_driver = None

    async def close(self) -> None:
        """Close all database connections."""
        logger.info("Closing database connections...")

        # Close PostgreSQL
        if self.postgres_engine:
            await self.postgres_engine.dispose()
            logger.info("PostgreSQL connection closed")

        # Close Redis
        if self.redis_client:
            await self.redis_client.aclose()
            logger.info("Redis connection closed")

        # Close MongoDB
        if self.mongodb_client:
            self.mongodb_client.close()
            logger.info("MongoDB connection closed")

        # Close Neo4j
        if self.neo4j_driver:
            await self.neo4j_driver.close()
            logger.info("Neo4j connection closed")

        logger.info("All database connections closed")

    async def health_check(self) -> dict:
        """Perform health check on all databases."""
        health_status = {
            "postgres": {"status": "unknown", "error": None},
            "redis": {"status": "unknown", "error": None},
            "mongodb": {"status": "unknown", "error": None},
            "neo4j": {"status": "unknown", "error": None},
        }

        # Check PostgreSQL
        try:
            if self.postgres_engine:
                async with self.postgres_engine.begin() as conn:
                    await conn.execute(text("SELECT 1"))
                health_status["postgres"]["status"] = "healthy"
        except Exception as e:
            health_status["postgres"]["status"] = "unhealthy"
            health_status["postgres"]["error"] = str(e)

        # Check Redis
        try:
            if self.redis_client:
                await self.redis_client.ping()
                health_status["redis"]["status"] = "healthy"
            else:
                health_status["redis"]["status"] = "disabled"
        except Exception as e:
            health_status["redis"]["status"] = "unhealthy"
            health_status["redis"]["error"] = str(e)

        # Check MongoDB
        try:
            if self.mongodb_client:
                await self.mongodb_client.admin.command("ping")
                health_status["mongodb"]["status"] = "healthy"
            else:
                health_status["mongodb"]["status"] = "disabled"
        except Exception as e:
            health_status["mongodb"]["status"] = "unhealthy"
            health_status["mongodb"]["error"] = str(e)

        # Check Neo4j
        try:
            if self.neo4j_driver:
                await self.neo4j_driver.verify_connectivity()
                health_status["neo4j"]["status"] = "healthy"
            else:
                health_status["neo4j"]["status"] = "disabled"
        except Exception as e:
            health_status["neo4j"]["status"] = "unhealthy"
            health_status["neo4j"]["error"] = str(e)

        return health_status

    @asynccontextmanager
    async def get_postgres_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get an async PostgreSQL session."""
        if not self.async_session_maker:
            raise RuntimeError("PostgreSQL not initialized")

        async with self.async_session_maker() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    def get_mongodb_database(self, database_name: Optional[str] = None):
        """Get MongoDB database instance."""
        if not self.mongodb_client:
            raise RuntimeError("MongoDB not initialized")

        db_name = database_name or settings.mongodb_database
        return self.mongodb_client[db_name]

    @asynccontextmanager
    async def get_neo4j_session(self):
        """Get a Neo4j session."""
        if not self.neo4j_driver:
            raise RuntimeError("Neo4j not initialized")

        async with self.neo4j_driver.session() as session:
            yield session

    def get_redis(self) -> Redis:
        """Get Redis client."""
        if not self.redis_client:
            raise RuntimeError("Redis not initialized")
        return self.redis_client
    
    # Alias for convenience
    get_session = get_postgres_session


# Global database manager instance
db_manager = DatabaseManager()


# Dependency for FastAPI
async def get_postgres_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for getting PostgreSQL session."""
    async with db_manager.get_postgres_session() as session:
        yield session


async def get_redis() -> Redis:
    """FastAPI dependency for getting Redis client."""
    return db_manager.get_redis()


async def get_mongodb():
    """FastAPI dependency for getting MongoDB database."""
    return db_manager.get_mongodb_database()


async def get_neo4j_session():
    """FastAPI dependency for getting Neo4j session."""
    async with db_manager.get_neo4j_session() as session:
        yield session


# Event listeners for connection management (disabled for async engines)
# AsyncEngine doesn't support the same event model as sync engines
# We'll implement connection monitoring through the health check system instead

# Import models to ensure they are registered with SQLAlchemy
from budflow.auth.models import User, Role, Permission, UserRole, RolePermission, UserSession  # noqa
from budflow.workflows.models import Workflow, WorkflowNode, WorkflowConnection, WorkflowExecution, NodeExecution, SharedWorkflow, WorkflowTag  # noqa
from budflow.projects.models import Project, Folder  # noqa
from budflow.credentials.models import Credential, CredentialTypeDefinition, SharedCredential, NodeCredential  # noqa