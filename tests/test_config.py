"""Test configuration system."""

import os
import pytest
from unittest.mock import patch

from budflow.config import Settings, get_settings, settings


@pytest.mark.unit
def test_settings_creation():
    """Test settings object creation."""
    test_settings = Settings()
    
    # Check default values
    assert test_settings.app_name == "BudFlow"
    assert test_settings.app_version == "0.1.0"
    assert test_settings.host == "0.0.0.0"
    assert test_settings.port == 8000


@pytest.mark.unit
def test_settings_environment_properties():
    """Test environment detection properties."""
    # Test development environment
    dev_settings = Settings(environment="development")
    assert dev_settings.is_development is True
    assert dev_settings.is_production is False
    assert dev_settings.is_testing is False
    
    # Test production environment
    prod_settings = Settings(environment="production")
    assert prod_settings.is_development is False
    assert prod_settings.is_production is True
    assert prod_settings.is_testing is False
    
    # Test testing environment
    test_settings = Settings(environment="testing")
    assert test_settings.is_development is False
    assert test_settings.is_production is False
    assert test_settings.is_testing is True


@pytest.mark.unit
def test_settings_from_environment():
    """Test loading settings from environment variables."""
    with patch.dict(os.environ, {
        "APP_NAME": "TestApp",
        "PORT": "9000",
        "DEBUG": "true",
        "MAX_EXECUTION_TIME": "7200",
    }):
        test_settings = Settings()
        
        assert test_settings.app_name == "TestApp"
        assert test_settings.port == 9000
        assert test_settings.debug is True
        assert test_settings.max_execution_time == 7200


@pytest.mark.unit
def test_settings_validation():
    """Test settings validation."""
    # Test with valid values
    valid_settings = Settings(
        secret_key="test-secret",
        encryption_key="test-encryption-key",
        database_url="sqlite:///test.db"
    )
    assert valid_settings.secret_key == "test-secret"
    
    # Pydantic will handle basic validation, but we can test type coercion
    settings_with_string_port = Settings(port="8080")
    assert settings_with_string_port.port == 8080


@pytest.mark.unit
def test_cached_settings():
    """Test that get_settings returns cached instance."""
    settings1 = get_settings()
    settings2 = get_settings()
    
    # Should be the same instance due to lru_cache
    assert settings1 is settings2


@pytest.mark.unit
def test_database_configuration():
    """Test database configuration settings."""
    test_settings = Settings(
        database_url="postgresql://user:pass@localhost:5432/testdb",
        database_pool_size=20,
        database_max_overflow=30,
        redis_url="redis://localhost:6379/1",
        mongodb_url="mongodb://localhost:27017/testdb"
    )
    
    assert "postgresql" in test_settings.database_url
    assert test_settings.database_pool_size == 20
    assert test_settings.database_max_overflow == 30
    assert "redis" in test_settings.redis_url
    assert "mongodb" in test_settings.mongodb_url


@pytest.mark.unit
def test_security_configuration():
    """Test security-related configuration."""
    test_settings = Settings(
        secret_key="super-secret-key",
        encryption_key="encryption-key-123",
        access_token_expire_hours=24,
        mfa_enabled=True,
        rate_limit_enabled=True
    )
    
    assert test_settings.secret_key == "super-secret-key"
    assert test_settings.encryption_key == "encryption-key-123"
    assert test_settings.access_token_expire_hours == 24
    assert test_settings.mfa_enabled is True
    assert test_settings.rate_limit_enabled is True


@pytest.mark.unit
def test_cors_configuration():
    """Test CORS configuration."""
    test_settings = Settings(
        cors_enabled=True,
        cors_origins=["http://localhost:3000", "https://app.example.com"],
        cors_credentials=True,
        cors_methods=["GET", "POST", "PUT", "DELETE"]
    )
    
    assert test_settings.cors_enabled is True
    assert "http://localhost:3000" in test_settings.cors_origins
    assert test_settings.cors_credentials is True
    assert "GET" in test_settings.cors_methods


@pytest.mark.unit
def test_ai_configuration():
    """Test AI-related configuration."""
    test_settings = Settings(
        openai_api_key="sk-test-key",
        anthropic_api_key="ant-test-key",
        vector_db_type="mongodb",
        mcp_enabled=True
    )
    
    assert test_settings.openai_api_key == "sk-test-key"
    assert test_settings.anthropic_api_key == "ant-test-key"
    assert test_settings.vector_db_type == "mongodb"
    assert test_settings.mcp_enabled is True


@pytest.mark.unit
def test_execution_configuration():
    """Test workflow execution configuration."""
    test_settings = Settings(
        max_execution_time=7200,
        max_execution_memory=2048,
        max_parallel_executions=20,
        max_payload_size=209715200  # 200MB
    )
    
    assert test_settings.max_execution_time == 7200
    assert test_settings.max_execution_memory == 2048
    assert test_settings.max_parallel_executions == 20
    assert test_settings.max_payload_size == 209715200


@pytest.mark.unit
def test_monitoring_configuration():
    """Test monitoring and metrics configuration."""
    test_settings = Settings(
        metrics_enabled=True,
        prometheus_port=9090,
        health_check_enabled=True,
        health_check_path="/custom-health"
    )
    
    assert test_settings.metrics_enabled is True
    assert test_settings.prometheus_port == 9090
    assert test_settings.health_check_enabled is True
    assert test_settings.health_check_path == "/custom-health"


@pytest.mark.unit
def test_storage_configuration():
    """Test storage configuration."""
    test_settings = Settings(
        storage_type="s3",
        storage_path="/data/storage",
        s3_bucket="test-bucket",
        s3_region="us-west-2"
    )
    
    assert test_settings.storage_type == "s3"
    assert test_settings.storage_path == "/data/storage"
    assert test_settings.s3_bucket == "test-bucket"
    assert test_settings.s3_region == "us-west-2"


@pytest.mark.unit
def test_email_configuration():
    """Test email configuration."""
    test_settings = Settings(
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_username="user@example.com",
        smtp_use_tls=True
    )
    
    assert test_settings.smtp_host == "smtp.example.com"
    assert test_settings.smtp_port == 587
    assert test_settings.smtp_username == "user@example.com"
    assert test_settings.smtp_use_tls is True


@pytest.mark.unit
def test_global_settings_instance():
    """Test global settings instance."""
    # The global settings instance should be properly configured
    assert settings.app_name == "BudFlow"
    assert settings.environment in ("development", "testing", "production")
    
    # In testing environment, these should be set appropriately
    if settings.is_testing:
        assert settings.debug is True


@pytest.mark.unit
def test_settings_case_insensitive():
    """Test that settings are case insensitive."""
    with patch.dict(os.environ, {
        "app_name": "LowerCaseApp",  # lowercase
        "APP_NAME": "UpperCaseApp",  # uppercase
    }):
        test_settings = Settings()
        # Should use the uppercase version (environment variable priority)
        assert test_settings.app_name == "UpperCaseApp"


@pytest.mark.unit
def test_optional_settings():
    """Test optional settings handling."""
    test_settings = Settings(
        openai_api_key=None,
        smtp_host=None,
        s3_bucket=None
    )
    
    # Optional settings should be None when not provided
    assert test_settings.openai_api_key is None
    assert test_settings.smtp_host is None
    assert test_settings.s3_bucket is None