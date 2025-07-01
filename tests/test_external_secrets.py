"""Test External Secrets Management system."""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch, MagicMock, mock_open
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from budflow.security.external_secrets import (
    SecretBackend,
    HashiCorpVaultBackend,
    AWSSecretsManagerBackend,
    AzureKeyVaultBackend,
    KubernetesSecretsBackend,
    LocalFileBackend,
    ExternalSecretsManager,
    SecretReference,
    SecretValue,
    SecretMetadata,
    RotationPolicy,
    SecretBackendType,
    SecretAccessError,
    SecretNotFoundError,
    SecretRotationError,
    SecretConfigurationError,
)


@pytest.fixture
def secret_reference():
    """Create test secret reference."""
    return SecretReference(
        key="database/password",
        backend="vault",
        version="latest",
        metadata={
            "environment": "production",
            "service": "api"
        }
    )


@pytest.fixture
def secret_value():
    """Create test secret value."""
    return SecretValue(
        key="database/password",
        value="secret123",
        version="v1",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(days=30),
        metadata={"rotation_id": "r123"}
    )


@pytest.fixture
def rotation_policy():
    """Create test rotation policy."""
    return RotationPolicy(
        enabled=True,
        interval_days=30,
        auto_rotate=True,
        notification_days=7,
        rotation_function="rotate_database_password"
    )


@pytest.fixture
def vault_backend():
    """Create HashiCorp Vault backend for testing."""
    return HashiCorpVaultBackend(
        url="https://vault.example.com",
        token="test-token",
        mount_point="secret"
    )


@pytest.fixture
def aws_backend():
    """Create AWS Secrets Manager backend for testing."""
    return AWSSecretsManagerBackend(
        region="us-east-1",
        access_key_id="test-key",
        secret_access_key="test-secret"
    )


@pytest.fixture
def azure_backend():
    """Create Azure Key Vault backend for testing."""
    return AzureKeyVaultBackend(
        vault_url="https://vault.vault.azure.net/",
        tenant_id="test-tenant",
        client_id="test-client",
        client_secret="test-secret"
    )


@pytest.fixture
def k8s_backend():
    """Create Kubernetes Secrets backend for testing."""
    return KubernetesSecretsBackend(
        namespace="default",
        kubeconfig_path="/path/to/kubeconfig"
    )


@pytest.fixture
def local_backend():
    """Create Local File backend for testing."""
    # Generate a proper Fernet key for testing
    from cryptography.fernet import Fernet
    test_key = Fernet.generate_key()
    return LocalFileBackend(
        secrets_dir="/tmp/budflow-secrets",
        encryption_key=test_key
    )


@pytest.fixture
def secrets_manager():
    """Create External Secrets Manager for testing."""
    return ExternalSecretsManager()


@pytest.mark.unit
class TestSecretReference:
    """Test SecretReference model."""
    
    def test_secret_reference_creation(self, secret_reference):
        """Test creating secret reference."""
        assert secret_reference.key == "database/password"
        assert secret_reference.backend == "vault"
        assert secret_reference.version == "latest"
        assert secret_reference.metadata["environment"] == "production"
    
    def test_secret_reference_validation(self):
        """Test secret reference validation."""
        # Valid reference
        ref = SecretReference(key="valid/key", backend="vault")
        assert ref.key == "valid/key"
        
        # Test key validation
        with pytest.raises(ValueError):
            SecretReference(key="", backend="vault")
        
        with pytest.raises(ValueError):
            SecretReference(key="invalid key with spaces", backend="vault")
    
    def test_secret_reference_serialization(self, secret_reference):
        """Test secret reference serialization."""
        data = secret_reference.model_dump()
        
        assert "key" in data
        assert "backend" in data
        assert "version" in data
        assert "metadata" in data
        
        # Test deserialization
        restored = SecretReference.model_validate(data)
        assert restored.key == secret_reference.key
        assert restored.backend == secret_reference.backend


@pytest.mark.unit
class TestSecretValue:
    """Test SecretValue model."""
    
    def test_secret_value_creation(self, secret_value):
        """Test creating secret value."""
        assert secret_value.key == "database/password"
        assert secret_value.value == "secret123"
        assert secret_value.version == "v1"
        assert secret_value.created_at is not None
        assert secret_value.expires_at is not None
    
    def test_secret_value_expiration_check(self):
        """Test secret expiration checking."""
        # Expired secret
        expired_secret = SecretValue(
            key="test/key",
            value="value",
            version="v1",
            created_at=datetime.now(timezone.utc) - timedelta(days=2),
            expires_at=datetime.now(timezone.utc) - timedelta(days=1)
        )
        assert expired_secret.is_expired()
        
        # Non-expired secret
        valid_secret = SecretValue(
            key="test/key",
            value="value",
            version="v1",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(days=1)
        )
        assert not valid_secret.is_expired()
        
        # Secret without expiration
        no_expiry_secret = SecretValue(
            key="test/key",
            value="value",
            version="v1",
            created_at=datetime.now(timezone.utc)
        )
        assert not no_expiry_secret.is_expired()
    
    def test_secret_value_masking(self, secret_value):
        """Test secret value masking for logging."""
        masked = secret_value.get_masked_value()
        assert "secret123" not in masked
        assert "*" in masked or "***" in masked


@pytest.mark.unit
class TestRotationPolicy:
    """Test RotationPolicy model."""
    
    def test_rotation_policy_creation(self, rotation_policy):
        """Test creating rotation policy."""
        assert rotation_policy.enabled is True
        assert rotation_policy.interval_days == 30
        assert rotation_policy.auto_rotate is True
        assert rotation_policy.notification_days == 7
    
    def test_rotation_policy_validation(self):
        """Test rotation policy validation."""
        # Valid policy
        policy = RotationPolicy(enabled=True, interval_days=30)
        assert policy.interval_days == 30
        
        # Invalid interval
        with pytest.raises(ValueError):
            RotationPolicy(enabled=True, interval_days=0)
        
        with pytest.raises(ValueError):
            RotationPolicy(enabled=True, interval_days=-1)
    
    def test_next_rotation_calculation(self, rotation_policy):
        """Test next rotation time calculation."""
        last_rotation = datetime.now(timezone.utc)
        next_rotation = rotation_policy.calculate_next_rotation(last_rotation)
        
        expected = last_rotation + timedelta(days=rotation_policy.interval_days)
        assert abs((next_rotation - expected).total_seconds()) < 1
    
    def test_rotation_due_check(self, rotation_policy):
        """Test checking if rotation is due."""
        # Rotation due
        last_rotation = datetime.now(timezone.utc) - timedelta(days=31)
        assert rotation_policy.is_rotation_due(last_rotation)
        
        # Rotation not due
        last_rotation = datetime.now(timezone.utc) - timedelta(days=1)
        assert not rotation_policy.is_rotation_due(last_rotation)


@pytest.mark.unit
class TestHashiCorpVaultBackend:
    """Test HashiCorp Vault backend."""
    
    def test_vault_backend_initialization(self, vault_backend):
        """Test Vault backend initialization."""
        assert vault_backend.url == "https://vault.example.com"
        assert vault_backend.token == "test-token"
        assert vault_backend.mount_point == "secret"
        assert vault_backend.backend_type == SecretBackendType.HASHICORP_VAULT
    
    @pytest.mark.asyncio
    async def test_vault_health_check(self, vault_backend):
        """Test Vault health check."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={"initialized": True, "sealed": False})
            mock_get.return_value.__aenter__.return_value = mock_response
            
            is_healthy = await vault_backend.health_check()
            assert is_healthy is True
            mock_get.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_vault_get_secret(self, vault_backend):
        """Test getting secret from Vault."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={
                "data": {
                    "data": {"password": "secret123"},
                    "metadata": {
                        "version": 1,
                        "created_time": "2024-01-01T00:00:00Z"
                    }
                }
            })
            mock_get.return_value.__aenter__.return_value = mock_response
            
            secret = await vault_backend.get_secret("database/password")
            
            assert secret.key == "database/password"
            assert secret.value == "secret123"
            assert secret.version == "1"
    
    @pytest.mark.asyncio
    async def test_vault_set_secret(self, vault_backend):
        """Test setting secret in Vault."""
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={
                "data": {"version": 2}
            })
            mock_post.return_value.__aenter__.return_value = mock_response
            
            await vault_backend.set_secret("database/password", "newsecret")
            
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert "database/password" in str(call_args)
    
    @pytest.mark.asyncio
    async def test_vault_delete_secret(self, vault_backend):
        """Test deleting secret from Vault."""
        with patch('aiohttp.ClientSession.delete') as mock_delete:
            mock_response = AsyncMock()
            mock_response.status = 204
            mock_delete.return_value.__aenter__.return_value = mock_response
            
            await vault_backend.delete_secret("database/password")
            
            mock_delete.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_vault_list_secrets(self, vault_backend):
        """Test listing secrets from Vault."""
        with patch('aiohttp.ClientSession.request') as mock_request:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={
                "data": {"keys": ["database/", "api/"]}
            })
            mock_request.return_value.__aenter__.return_value = mock_response
            
            secrets = await vault_backend.list_secrets("database/")
            
            assert len(secrets) == 2
            assert "database/" in secrets
            assert "api/" in secrets
    
    @pytest.mark.asyncio
    async def test_vault_secret_not_found(self, vault_backend):
        """Test handling secret not found in Vault."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_get.return_value.__aenter__.return_value = mock_response
            
            with pytest.raises(SecretNotFoundError):
                await vault_backend.get_secret("nonexistent/secret")


@pytest.mark.unit
class TestAWSSecretsManagerBackend:
    """Test AWS Secrets Manager backend."""
    
    def test_aws_backend_initialization(self, aws_backend):
        """Test AWS backend initialization."""
        assert aws_backend.region == "us-east-1"
        assert aws_backend.access_key_id == "test-key"
        assert aws_backend.secret_access_key == "test-secret"
        assert aws_backend.backend_type == SecretBackendType.AWS_SECRETS_MANAGER
    
    @pytest.mark.asyncio
    async def test_aws_health_check(self, aws_backend):
        """Test AWS health check."""
        with patch.object(aws_backend, '_get_client') as mock_get_client:
            mock_client = Mock()
            mock_client.list_secrets.return_value = {"SecretList": []}
            mock_get_client.return_value = mock_client
            
            is_healthy = await aws_backend.health_check()
            assert is_healthy is True
    
    @pytest.mark.asyncio
    async def test_aws_get_secret(self, aws_backend):
        """Test getting secret from AWS Secrets Manager."""
        with patch.object(aws_backend, '_get_client') as mock_get_client:
            mock_client = Mock()
            mock_client.get_secret_value.return_value = {
                "SecretString": "secret123",
                "VersionId": "v1",
                "CreatedDate": datetime.now(timezone.utc)
            }
            mock_get_client.return_value = mock_client
            
            secret = await aws_backend.get_secret("database/password")
            
            assert secret.key == "database/password"
            assert secret.value == "secret123"
            assert secret.version == "v1"
    
    @pytest.mark.asyncio
    async def test_aws_set_secret(self, aws_backend):
        """Test setting secret in AWS Secrets Manager."""
        with patch.object(aws_backend, '_get_client') as mock_get_client:
            mock_client = Mock()
            mock_client.update_secret.return_value = {
                "VersionId": "v2"
            }
            mock_get_client.return_value = mock_client
            
            await aws_backend.set_secret("database/password", "newsecret")
            
            mock_client.update_secret.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_aws_secret_not_found(self, aws_backend):
        """Test handling secret not found in AWS."""
        with patch.object(aws_backend, '_get_client') as mock_get_client:
            mock_client = Mock()
            mock_client.get_secret_value.side_effect = Exception("ResourceNotFoundException")
            mock_get_client.return_value = mock_client
            
            with pytest.raises(SecretNotFoundError):
                await aws_backend.get_secret("nonexistent/secret")


@pytest.mark.unit
class TestAzureKeyVaultBackend:
    """Test Azure Key Vault backend."""
    
    def test_azure_backend_initialization(self, azure_backend):
        """Test Azure backend initialization."""
        assert azure_backend.vault_url == "https://vault.vault.azure.net"
        assert azure_backend.tenant_id == "test-tenant"
        assert azure_backend.client_id == "test-client"
        assert azure_backend.backend_type == SecretBackendType.AZURE_KEY_VAULT
    
    @pytest.mark.asyncio
    async def test_azure_health_check(self, azure_backend):
        """Test Azure health check."""
        with patch.object(azure_backend, '_get_client') as mock_get_client:
            mock_client = Mock()
            mock_client.list_properties_of_secrets.return_value = []
            mock_get_client.return_value = mock_client
            
            is_healthy = await azure_backend.health_check()
            assert is_healthy is True
    
    @pytest.mark.asyncio
    async def test_azure_get_secret(self, azure_backend):
        """Test getting secret from Azure Key Vault."""
        with patch.object(azure_backend, '_get_client') as mock_get_client:
            mock_client = Mock()
            mock_secret = Mock()
            mock_secret.value = "secret123"
            mock_secret.properties.version = "v1"
            mock_secret.properties.created_on = datetime.now(timezone.utc)
            mock_client.get_secret.return_value = mock_secret
            mock_get_client.return_value = mock_client
            
            secret = await azure_backend.get_secret("database-password")
            
            assert secret.key == "database-password"
            assert secret.value == "secret123"
            assert secret.version == "v1"
    
    @pytest.mark.asyncio
    async def test_azure_set_secret(self, azure_backend):
        """Test setting secret in Azure Key Vault."""
        with patch.object(azure_backend, '_get_client') as mock_get_client:
            mock_client = Mock()
            mock_secret = Mock()
            mock_secret.properties.version = "v2"
            mock_client.set_secret.return_value = mock_secret
            mock_get_client.return_value = mock_client
            
            await azure_backend.set_secret("database-password", "newsecret")
            
            mock_client.set_secret.assert_called_once()


@pytest.mark.unit
class TestKubernetesSecretsBackend:
    """Test Kubernetes Secrets backend."""
    
    def test_k8s_backend_initialization(self, k8s_backend):
        """Test Kubernetes backend initialization."""
        assert k8s_backend.namespace == "default"
        assert k8s_backend.kubeconfig_path == "/path/to/kubeconfig"
        assert k8s_backend.backend_type == SecretBackendType.KUBERNETES
    
    @pytest.mark.asyncio
    async def test_k8s_health_check(self, k8s_backend):
        """Test Kubernetes health check."""
        with patch.object(k8s_backend, '_get_client') as mock_get_client:
            mock_client = Mock()
            mock_client.list_namespaced_secret.return_value = Mock(items=[])
            mock_get_client.return_value = mock_client
            
            is_healthy = await k8s_backend.health_check()
            assert is_healthy is True
    
    @pytest.mark.asyncio
    async def test_k8s_get_secret(self, k8s_backend):
        """Test getting secret from Kubernetes."""
        with patch.object(k8s_backend, '_get_client') as mock_get_client:
            mock_client = Mock()
            mock_secret = Mock()
            mock_secret.data = {"password": "c2VjcmV0MTIz"}  # base64 encoded "secret123"
            mock_secret.metadata.resource_version = "12345"
            mock_secret.metadata.creation_timestamp = datetime.now(timezone.utc)
            mock_client.read_namespaced_secret.return_value = mock_secret
            mock_get_client.return_value = mock_client
            
            secret = await k8s_backend.get_secret("database-secret", field="password")
            
            assert secret.key == "database-secret"
            assert secret.value == "secret123"
            assert secret.version == "12345"


@pytest.mark.unit
class TestLocalFileBackend:
    """Test Local File backend."""
    
    def test_local_backend_initialization(self, local_backend):
        """Test Local File backend initialization."""
        from pathlib import Path
        assert local_backend.secrets_dir == Path("/tmp/budflow-secrets")
        assert local_backend.encryption_key is not None
        assert local_backend.backend_type == SecretBackendType.LOCAL_FILE
    
    @pytest.mark.asyncio
    async def test_local_health_check(self, local_backend):
        """Test Local File health check."""
        with patch('pathlib.Path.exists', return_value=True):
            with patch('pathlib.Path.is_dir', return_value=True):
                is_healthy = await local_backend.health_check()
                assert is_healthy is True
    
    @pytest.mark.asyncio
    async def test_local_get_secret(self, local_backend):
        """Test getting secret from local file."""
        secret_data = {
            "value": "secret123",
            "version": "v1",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "metadata": {}
        }
        
        with patch('pathlib.Path.exists', return_value=True):
            with patch('builtins.open', mock_open(read_data=json.dumps(secret_data))):
                with patch.object(local_backend, '_decrypt', return_value=json.dumps(secret_data)):
                    secret = await local_backend.get_secret("database/password")
                    
                    assert secret.key == "database/password"
                    assert secret.value == "secret123"
                    assert secret.version == "v1"
    
    @pytest.mark.asyncio
    async def test_local_set_secret(self, local_backend):
        """Test setting secret in local file."""
        with patch('pathlib.Path.mkdir'):
            with patch('builtins.open', mock_open()) as mock_file:
                with patch.object(local_backend, '_encrypt', return_value="encrypted_data"):
                    await local_backend.set_secret("database/password", "newsecret")
                    
                    mock_file.assert_called()


@pytest.mark.unit
class TestExternalSecretsManager:
    """Test External Secrets Manager."""
    
    def test_manager_initialization(self, secrets_manager):
        """Test manager initialization."""
        assert len(secrets_manager.backends) == 0
        assert len(secrets_manager.rotation_policies) == 0
        assert secrets_manager.cache_ttl == 300  # 5 minutes default
    
    def test_register_backend(self, secrets_manager, vault_backend):
        """Test registering backend."""
        secrets_manager.register_backend("vault", vault_backend)
        
        assert "vault" in secrets_manager.backends
        assert secrets_manager.backends["vault"] == vault_backend
    
    def test_register_rotation_policy(self, secrets_manager, rotation_policy):
        """Test registering rotation policy."""
        secrets_manager.register_rotation_policy("database/*", rotation_policy)
        
        assert "database/*" in secrets_manager.rotation_policies
        assert secrets_manager.rotation_policies["database/*"] == rotation_policy
    
    @pytest.mark.asyncio
    async def test_get_secret_with_cache(self, secrets_manager, vault_backend, secret_value):
        """Test getting secret with caching."""
        secrets_manager.register_backend("vault", vault_backend)
        
        # Mock backend call
        vault_backend.get_secret = AsyncMock(return_value=secret_value)
        
        # First call - should hit backend
        result1 = await secrets_manager.get_secret("database/password", "vault")
        assert result1.value == "secret123"
        vault_backend.get_secret.assert_called_once()
        
        # Second call - should use cache
        vault_backend.get_secret.reset_mock()
        result2 = await secrets_manager.get_secret("database/password", "vault")
        assert result2.value == "secret123"
        vault_backend.get_secret.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_set_secret(self, secrets_manager, vault_backend):
        """Test setting secret."""
        secrets_manager.register_backend("vault", vault_backend)
        vault_backend.set_secret = AsyncMock()
        
        await secrets_manager.set_secret("database/password", "newsecret", "vault")
        
        vault_backend.set_secret.assert_called_once_with("database/password", "newsecret", None)
    
    @pytest.mark.asyncio
    async def test_rotate_secret(self, secrets_manager, vault_backend, rotation_policy):
        """Test secret rotation."""
        secrets_manager.register_backend("vault", vault_backend)
        secrets_manager.register_rotation_policy("database/*", rotation_policy)
        
        # Mock rotation function
        async def mock_rotation_function(secret_key: str) -> str:
            return f"rotated_{secret_key}"
        
        with patch.object(secrets_manager, '_call_rotation_function', return_value="rotated_password"):
            vault_backend.set_secret = AsyncMock()
            
            await secrets_manager.rotate_secret("database/password", "vault")
            
            vault_backend.set_secret.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_rotations_needed(self, secrets_manager, vault_backend, rotation_policy):
        """Test checking for needed rotations."""
        secrets_manager.register_backend("vault", vault_backend)
        secrets_manager.register_rotation_policy("database/*", rotation_policy)
        
        # Mock secret that needs rotation
        old_secret = SecretValue(
            key="database/password",
            value="old_value",
            version="v1",
            created_at=datetime.now(timezone.utc) - timedelta(days=35),
            last_rotated=datetime.now(timezone.utc) - timedelta(days=35)
        )
        
        vault_backend.list_secrets = AsyncMock(return_value=["database/password"])
        vault_backend.get_secret = AsyncMock(return_value=old_secret)
        
        rotations_needed = await secrets_manager.check_rotations_needed()
        
        assert len(rotations_needed) == 1
        assert rotations_needed[0]["key"] == "database/password"
    
    @pytest.mark.asyncio
    async def test_backend_not_found(self, secrets_manager):
        """Test handling backend not found."""
        with pytest.raises(SecretConfigurationError):
            await secrets_manager.get_secret("test/key", "nonexistent_backend")
    
    @pytest.mark.asyncio
    async def test_health_check_all_backends(self, secrets_manager, vault_backend, aws_backend):
        """Test health check for all backends."""
        secrets_manager.register_backend("vault", vault_backend)
        secrets_manager.register_backend("aws", aws_backend)
        
        vault_backend.health_check = AsyncMock(return_value=True)
        aws_backend.health_check = AsyncMock(return_value=False)
        
        health_status = await secrets_manager.health_check()
        
        assert health_status["vault"] is True
        assert health_status["aws"] is False
        assert health_status["overall"] is False  # One backend unhealthy


@pytest.mark.integration
class TestSecretsIntegration:
    """Integration tests for secrets management."""
    
    @pytest.mark.asyncio
    async def test_multi_backend_failover(self, secrets_manager, vault_backend, local_backend):
        """Test failover between backends."""
        secrets_manager.register_backend("vault", vault_backend)
        secrets_manager.register_backend("local", local_backend)
        
        # Vault fails
        vault_backend.get_secret = AsyncMock(side_effect=SecretAccessError("Vault unavailable"))
        
        # Local succeeds
        local_secret = SecretValue(
            key="fallback/secret",
            value="fallback_value",
            version="v1",
            created_at=datetime.now(timezone.utc)
        )
        local_backend.get_secret = AsyncMock(return_value=local_secret)
        
        # Should fall back to local backend
        result = await secrets_manager.get_secret_with_fallback("fallback/secret", ["vault", "local"])
        
        assert result.value == "fallback_value"
    
    @pytest.mark.asyncio
    async def test_secret_synchronization(self, secrets_manager, vault_backend, aws_backend):
        """Test synchronizing secrets between backends."""
        secrets_manager.register_backend("vault", vault_backend)
        secrets_manager.register_backend("aws", aws_backend)
        
        # Mock vault secret
        vault_secret = SecretValue(
            key="sync/secret",
            value="sync_value",
            version="v1",
            created_at=datetime.now(timezone.utc)
        )
        vault_backend.get_secret = AsyncMock(return_value=vault_secret)
        vault_backend.list_secrets = AsyncMock(return_value=["sync/secret"])
        
        # Mock AWS operations
        aws_backend.set_secret = AsyncMock()
        
        # Sync from vault to aws
        await secrets_manager.sync_secrets("vault", "aws", key_pattern="sync/*")
        
        aws_backend.set_secret.assert_called_once_with("sync/secret", "sync_value", {})
    
    @pytest.mark.asyncio
    async def test_bulk_secret_operations(self, secrets_manager, vault_backend):
        """Test bulk secret operations."""
        secrets_manager.register_backend("vault", vault_backend)
        
        # Mock bulk operations
        vault_backend.set_secret = AsyncMock()
        
        secrets_to_set = {
            "app/db_password": "db123",
            "app/api_key": "api456",
            "app/jwt_secret": "jwt789"
        }
        
        await secrets_manager.set_secrets_bulk(secrets_to_set, "vault")
        
        assert vault_backend.set_secret.call_count == 3
    
    @pytest.mark.asyncio
    async def test_secret_audit_trail(self, secrets_manager, vault_backend):
        """Test secret access audit trail."""
        secrets_manager.register_backend("vault", vault_backend)
        
        secret_value = SecretValue(
            key="audit/secret",
            value="audit_value",
            version="v1",
            created_at=datetime.now(timezone.utc)
        )
        vault_backend.get_secret = AsyncMock(return_value=secret_value)
        
        # Enable audit logging
        secrets_manager.enable_audit_logging = True
        
        with patch.object(secrets_manager, '_log_access') as mock_log:
            await secrets_manager.get_secret("audit/secret", "vault", user_id="test_user")
            
            mock_log.assert_called_once()
            # Check that the method was called (the specific arguments depend on implementation)
            assert mock_log.called


@pytest.mark.performance
class TestSecretsPerformance:
    """Performance tests for secrets management."""
    
    @pytest.mark.asyncio
    async def test_concurrent_secret_access(self, secrets_manager, vault_backend):
        """Test concurrent secret access performance."""
        secrets_manager.register_backend("vault", vault_backend)
        
        secret_value = SecretValue(
            key="perf/secret",
            value="perf_value",
            version="v1",
            created_at=datetime.now(timezone.utc)
        )
        vault_backend.get_secret = AsyncMock(return_value=secret_value)
        
        # Test concurrent access
        tasks = []
        for i in range(100):
            task = secrets_manager.get_secret("perf/secret", "vault")
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # All requests should succeed
        assert len(results) == 100
        assert all(r.value == "perf_value" for r in results)
        
        # Due to caching, backend should be called only once
        assert vault_backend.get_secret.call_count == 1
    
    @pytest.mark.asyncio
    async def test_cache_performance(self, secrets_manager, vault_backend):
        """Test cache performance with many secrets."""
        secrets_manager.register_backend("vault", vault_backend)
        
        # Mock many secrets
        def mock_get_secret(key, version="latest"):
            return SecretValue(
                key=key,
                value=f"value_{key}",
                version="v1",
                created_at=datetime.now(timezone.utc)
            )
        
        vault_backend.get_secret = AsyncMock(side_effect=mock_get_secret)
        
        # Access many different secrets
        for i in range(1000):
            await secrets_manager.get_secret(f"perf/secret_{i}", "vault")
        
        # Each unique secret should be fetched once
        assert vault_backend.get_secret.call_count == 1000
        
        # Access cached secrets again
        vault_backend.get_secret.reset_mock()
        for i in range(1000):
            await secrets_manager.get_secret(f"perf/secret_{i}", "vault")
        
        # Should not hit backend due to caching
        assert vault_backend.get_secret.call_count == 0