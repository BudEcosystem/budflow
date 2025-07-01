"""Application configuration."""

import os
from functools import lru_cache
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = Field(default="BudFlow", description="Application name")
    app_version: str = Field(default="0.1.0", description="Application version")
    environment: str = Field(default="development", description="Environment")
    debug: bool = Field(default=False, description="Debug mode")
    log_level: str = Field(default="INFO", description="Log level")

    # Server
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8000, description="Server port")
    workers: int = Field(default=1, description="Number of workers")
    reload: bool = Field(default=False, description="Auto-reload on changes")

    # Security
    secret_key: str = Field(description="Secret key for JWT signing")
    algorithm: str = Field(default="HS256", description="JWT algorithm")
    access_token_expire_hours: int = Field(
        default=168, description="Access token expiration in hours"
    )
    refresh_token_expire_days: int = Field(
        default=30, description="Refresh token expiration in days"
    )
    encryption_key: str = Field(description="Encryption key for sensitive data")
    salt_rounds: int = Field(default=12, description="Password salt rounds")

    # Database
    database_url: str = Field(description="PostgreSQL database URL")
    database_pool_size: int = Field(default=10, description="Database pool size")
    database_max_overflow: int = Field(
        default=20, description="Database max overflow"
    )
    database_echo: bool = Field(default=False, description="Echo SQL statements")
    test_database_url: Optional[str] = Field(
        default=None, description="Test database URL"
    )

    # Redis
    redis_url: str = Field(description="Redis URL")
    redis_password: Optional[str] = Field(default=None, description="Redis password")
    redis_max_connections: int = Field(
        default=10, description="Redis max connections"
    )

    # MongoDB
    mongodb_url: str = Field(description="MongoDB URL")
    mongodb_database: str = Field(default="budflow", description="MongoDB database")
    mongodb_username: Optional[str] = Field(
        default=None, description="MongoDB username"
    )
    mongodb_password: Optional[str] = Field(
        default=None, description="MongoDB password"
    )

    # Neo4j
    neo4j_uri: str = Field(description="Neo4j URI")
    neo4j_username: str = Field(default="neo4j", description="Neo4j username")
    neo4j_password: str = Field(description="Neo4j password")

    # Celery
    celery_broker_url: str = Field(description="Celery broker URL")
    celery_result_backend: str = Field(description="Celery result backend")
    celery_task_serializer: str = Field(
        default="json", description="Celery task serializer"
    )
    celery_result_serializer: str = Field(
        default="json", description="Celery result serializer"
    )
    celery_accept_content: List[str] = Field(
        default=["json"], description="Celery accept content"
    )
    celery_timezone: str = Field(default="UTC", description="Celery timezone")

    # Storage
    storage_type: str = Field(default="local", description="Storage type")
    storage_path: str = Field(
        default="./data/storage", description="Local storage path"
    )
    s3_bucket: Optional[str] = Field(default=None, description="S3 bucket name")
    s3_region: Optional[str] = Field(default=None, description="S3 region")
    s3_access_key_id: Optional[str] = Field(default=None, description="S3 access key")
    s3_secret_access_key: Optional[str] = Field(
        default=None, description="S3 secret key"
    )
    s3_endpoint_url: Optional[str] = Field(default=None, description="S3 endpoint URL")

    # Email
    smtp_host: Optional[str] = Field(default=None, description="SMTP host")
    smtp_port: int = Field(default=587, description="SMTP port")
    smtp_username: Optional[str] = Field(default=None, description="SMTP username")
    smtp_password: Optional[str] = Field(default=None, description="SMTP password")
    smtp_from_email: str = Field(
        default="noreply@budflow.com", description="From email"
    )
    smtp_use_tls: bool = Field(default=True, description="Use TLS for SMTP")

    # MFA
    mfa_enabled: bool = Field(default=True, description="Enable MFA")
    mfa_issuer: str = Field(default="BudFlow", description="MFA issuer")
    mfa_service_name: str = Field(
        default="BudFlow Workflow Platform", description="MFA service name"
    )

    # AI
    openai_api_key: Optional[str] = Field(default=None, description="OpenAI API key")
    anthropic_api_key: Optional[str] = Field(
        default=None, description="Anthropic API key"
    )
    google_api_key: Optional[str] = Field(default=None, description="Google API key")

    # Vector DB
    vector_db_type: str = Field(default="mongodb", description="Vector DB type")
    chroma_host: str = Field(default="localhost", description="ChromaDB host")
    chroma_port: int = Field(default=8000, description="ChromaDB port")

    # MCP
    mcp_enabled: bool = Field(default=True, description="Enable MCP")
    mcp_server_port: int = Field(default=3001, description="MCP server port")
    mcp_client_timeout: int = Field(default=30, description="MCP client timeout")

    # Execution
    max_execution_time: int = Field(
        default=3600, description="Max execution time in seconds"
    )
    max_execution_memory: int = Field(
        default=1024, description="Max execution memory in MB"
    )
    max_parallel_executions: int = Field(
        default=10, description="Max parallel executions"
    )
    max_payload_size: int = Field(
        default=104857600, description="Max payload size in bytes"
    )

    # Monitoring
    metrics_enabled: bool = Field(default=True, description="Enable metrics")
    prometheus_port: int = Field(default=8001, description="Prometheus port")

    # CORS
    cors_enabled: bool = Field(default=True, description="Enable CORS")
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080"],
        description="CORS origins",
    )
    cors_credentials: bool = Field(default=True, description="CORS credentials")
    cors_methods: List[str] = Field(
        default=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
        description="CORS methods",
    )
    cors_headers: List[str] = Field(default=["*"], description="CORS headers")

    # Rate limiting
    rate_limit_enabled: bool = Field(default=True, description="Enable rate limiting")
    rate_limit_requests_per_minute: int = Field(
        default=60, description="Rate limit requests per minute"
    )
    rate_limit_burst: int = Field(default=10, description="Rate limit burst")

    # Health check
    health_check_enabled: bool = Field(default=True, description="Enable health check")
    health_check_path: str = Field(default="/health", description="Health check path")
    health_check_timeout: int = Field(
        default=30, description="Health check timeout"
    )

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment.lower() in ("development", "dev")

    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.environment.lower() in ("production", "prod")

    @property
    def is_testing(self) -> bool:
        """Check if running in testing mode."""
        return self.environment.lower() in ("testing", "test")


@lru_cache()
def get_settings() -> Settings:
    """Get application settings (cached)."""
    return Settings()


# Global settings instance
settings = get_settings()