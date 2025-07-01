"""External Secrets Management system for BudFlow.

This module provides a comprehensive secrets management system with support for
multiple backends including HashiCorp Vault, AWS Secrets Manager, Azure Key Vault,
Kubernetes Secrets, and local file storage.
"""

import asyncio
import base64
import json
import os
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Callable, Set
from urllib.parse import urljoin
import logging
from cryptography.fernet import Fernet
import fnmatch

import structlog
from pydantic import BaseModel, Field, ConfigDict, field_validator
import aiohttp

logger = structlog.get_logger()


class SecretBackendType(str, Enum):
    """Secret backend types."""
    HASHICORP_VAULT = "hashicorp_vault"
    AWS_SECRETS_MANAGER = "aws_secrets_manager"
    AZURE_KEY_VAULT = "azure_key_vault"
    KUBERNETES = "kubernetes"
    LOCAL_FILE = "local_file"
    GOOGLE_SECRET_MANAGER = "google_secret_manager"


class SecretAccessError(Exception):
    """Error accessing secret backend."""
    pass


class SecretNotFoundError(Exception):
    """Secret not found error."""
    pass


class SecretRotationError(Exception):
    """Secret rotation error."""
    pass


class SecretConfigurationError(Exception):
    """Secret configuration error."""
    pass


class SecretReference(BaseModel):
    """Reference to an external secret."""
    
    key: str = Field(..., description="Secret key/path")
    backend: str = Field(..., description="Backend name")
    version: str = Field(default="latest", description="Secret version")
    field: Optional[str] = Field(default=None, description="Specific field in secret")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    model_config = ConfigDict(from_attributes=True)
    
    @field_validator('key')
    @classmethod
    def validate_key(cls, v):
        if not v or not v.strip():
            raise ValueError("Secret key cannot be empty")
        # Only allow alphanumeric, hyphens, underscores, forward slashes
        if not re.match(r'^[a-zA-Z0-9\-_/]+$', v):
            raise ValueError("Secret key contains invalid characters")
        return v


class SecretValue(BaseModel):
    """Secret value with metadata."""
    
    key: str = Field(..., description="Secret key")
    value: str = Field(..., description="Secret value")
    version: str = Field(..., description="Secret version")
    created_at: datetime = Field(..., description="Creation timestamp")
    expires_at: Optional[datetime] = Field(default=None, description="Expiration timestamp")
    last_rotated: Optional[datetime] = Field(default=None, description="Last rotation timestamp")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Secret metadata")
    
    model_config = ConfigDict(from_attributes=True)
    
    def is_expired(self) -> bool:
        """Check if secret is expired."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) > self.expires_at
    
    def get_masked_value(self, show_chars: int = 3) -> str:
        """Get masked version of secret value for logging."""
        if len(self.value) <= show_chars:
            return "*" * len(self.value)
        return self.value[:show_chars] + "*" * (len(self.value) - show_chars)


class SecretMetadata(BaseModel):
    """Secret metadata."""
    
    key: str = Field(..., description="Secret key")
    backend: str = Field(..., description="Backend name")
    description: Optional[str] = Field(default=None, description="Secret description")
    tags: List[str] = Field(default_factory=list, description="Secret tags")
    created_by: Optional[str] = Field(default=None, description="Created by user")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: Optional[datetime] = Field(default=None, description="Last update timestamp")
    access_count: int = Field(default=0, description="Access count")
    last_accessed: Optional[datetime] = Field(default=None, description="Last access timestamp")
    
    model_config = ConfigDict(from_attributes=True)


class RotationPolicy(BaseModel):
    """Secret rotation policy."""
    
    enabled: bool = Field(default=False, description="Whether rotation is enabled")
    interval_days: int = Field(default=30, description="Rotation interval in days")
    auto_rotate: bool = Field(default=False, description="Whether to auto-rotate")
    notification_days: int = Field(default=7, description="Days before expiry to notify")
    rotation_function: Optional[str] = Field(default=None, description="Custom rotation function")
    max_age_days: Optional[int] = Field(default=None, description="Maximum age before forced rotation")
    
    model_config = ConfigDict(from_attributes=True)
    
    @field_validator('interval_days')
    @classmethod
    def validate_interval(cls, v):
        if v <= 0:
            raise ValueError("Rotation interval must be positive")
        return v
    
    def calculate_next_rotation(self, last_rotation: datetime) -> datetime:
        """Calculate next rotation time."""
        return last_rotation + timedelta(days=self.interval_days)
    
    def is_rotation_due(self, last_rotation: Optional[datetime]) -> bool:
        """Check if rotation is due."""
        if not self.enabled or not last_rotation:
            return False
        
        next_rotation = self.calculate_next_rotation(last_rotation)
        return datetime.now(timezone.utc) >= next_rotation
    
    def is_notification_due(self, last_rotation: Optional[datetime]) -> bool:
        """Check if rotation notification is due."""
        if not self.enabled or not last_rotation:
            return False
        
        next_rotation = self.calculate_next_rotation(last_rotation)
        notification_time = next_rotation - timedelta(days=self.notification_days)
        return datetime.now(timezone.utc) >= notification_time


class SecretBackend(ABC):
    """Abstract base class for secret backends."""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.logger = logger.bind(component="secret_backend", backend=name)
    
    @property
    @abstractmethod
    def backend_type(self) -> SecretBackendType:
        """Get backend type."""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check backend health."""
        pass
    
    @abstractmethod
    async def get_secret(self, key: str, version: str = "latest") -> SecretValue:
        """Get secret value."""
        pass
    
    @abstractmethod
    async def set_secret(self, key: str, value: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Set secret value, returns version."""
        pass
    
    @abstractmethod
    async def delete_secret(self, key: str) -> bool:
        """Delete secret."""
        pass
    
    @abstractmethod
    async def list_secrets(self, prefix: str = "") -> List[str]:
        """List secret keys."""
        pass
    
    async def rotate_secret(self, key: str, new_value: str) -> str:
        """Rotate secret value."""
        return await self.set_secret(key, new_value, {"rotated_at": datetime.now(timezone.utc).isoformat()})


class HashiCorpVaultBackend(SecretBackend):
    """HashiCorp Vault backend."""
    
    def __init__(self, url: str, token: str, mount_point: str = "secret", **kwargs):
        super().__init__("vault", kwargs)
        self.url = url.rstrip('/')
        self.token = token
        self.mount_point = mount_point
        self.api_version = kwargs.get('api_version', 'v1')
        self.timeout = kwargs.get('timeout', 30)
    
    @property
    def backend_type(self) -> SecretBackendType:
        return SecretBackendType.HASHICORP_VAULT
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "X-Vault-Token": self.token,
            "Content-Type": "application/json"
        }
    
    def _build_url(self, path: str) -> str:
        """Build full URL for API call."""
        return urljoin(f"{self.url}/", f"{self.api_version}/{self.mount_point}/data/{path}")
    
    async def health_check(self) -> bool:
        """Check Vault health."""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.get(f"{self.url}/v1/sys/health") as response:
                    if response.status == 200:
                        health_data = await response.json()
                        return health_data.get("initialized", False) and not health_data.get("sealed", True)
                    return False
        except Exception as e:
            self.logger.error("Vault health check failed", error=str(e))
            return False
    
    async def get_secret(self, key: str, version: str = "latest") -> SecretValue:
        """Get secret from Vault."""
        try:
            url = self._build_url(key)
            params = {}
            if version != "latest":
                params["version"] = version
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.get(url, headers=self._get_headers(), params=params) as response:
                    if response.status == 404:
                        raise SecretNotFoundError(f"Secret not found: {key}")
                    elif response.status != 200:
                        raise SecretAccessError(f"Vault API error: {response.status}")
                    
                    data = await response.json()
                    secret_data = data["data"]["data"]
                    metadata = data["data"]["metadata"]
                    
                    # If only one field, return its value; otherwise return JSON
                    if len(secret_data) == 1:
                        value = list(secret_data.values())[0]
                    else:
                        value = json.dumps(secret_data)
                    
                    return SecretValue(
                        key=key,
                        value=str(value),
                        version=str(metadata.get("version", "1")),
                        created_at=datetime.fromisoformat(metadata["created_time"].replace('Z', '+00:00')),
                        metadata=metadata
                    )
                    
        except SecretNotFoundError:
            raise
        except Exception as e:
            self.logger.error("Failed to get secret from Vault", key=key, error=str(e))
            raise SecretAccessError(f"Failed to get secret: {str(e)}")
    
    async def set_secret(self, key: str, value: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Set secret in Vault."""
        try:
            url = self._build_url(key)
            
            # Prepare data
            secret_data = {"data": {key.split('/')[-1]: value}}
            if metadata:
                secret_data["metadata"] = metadata
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.post(url, headers=self._get_headers(), json=secret_data) as response:
                    if response.status not in [200, 204]:
                        raise SecretAccessError(f"Vault API error: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        return str(data["data"]["version"])
                    return "1"
                    
        except Exception as e:
            self.logger.error("Failed to set secret in Vault", key=key, error=str(e))
            raise SecretAccessError(f"Failed to set secret: {str(e)}")
    
    async def delete_secret(self, key: str) -> bool:
        """Delete secret from Vault."""
        try:
            url = f"{self.url}/v1/{self.mount_point}/metadata/{key}"
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.delete(url, headers=self._get_headers()) as response:
                    return response.status in [200, 204]
                    
        except Exception as e:
            self.logger.error("Failed to delete secret from Vault", key=key, error=str(e))
            return False
    
    async def list_secrets(self, prefix: str = "") -> List[str]:
        """List secrets in Vault."""
        try:
            url = f"{self.url}/v1/{self.mount_point}/metadata/{prefix}"
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.request("LIST", url, headers=self._get_headers()) as response:
                    if response.status == 404:
                        return []
                    elif response.status != 200:
                        raise SecretAccessError(f"Vault API error: {response.status}")
                    
                    data = await response.json()
                    return data["data"]["keys"]
                    
        except Exception as e:
            self.logger.error("Failed to list secrets from Vault", prefix=prefix, error=str(e))
            return []


class AWSSecretsManagerBackend(SecretBackend):
    """AWS Secrets Manager backend."""
    
    def __init__(self, region: str, access_key_id: str = None, secret_access_key: str = None, **kwargs):
        super().__init__("aws_secrets", kwargs)
        self.region = region
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = kwargs.get('session_token')
        self._client = None
    
    @property
    def backend_type(self) -> SecretBackendType:
        return SecretBackendType.AWS_SECRETS_MANAGER
    
    async def _get_client(self):
        """Get AWS Secrets Manager client."""
        if not self._client:
            try:
                import boto3
                session = boto3.Session(
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.secret_access_key,
                    aws_session_token=self.session_token,
                    region_name=self.region
                )
                self._client = session.client('secretsmanager')
            except ImportError:
                raise SecretConfigurationError("boto3 library required for AWS Secrets Manager")
        return self._client
    
    async def health_check(self) -> bool:
        """Check AWS Secrets Manager health."""
        try:
            client = await self._get_client()
            # Try to list secrets to verify connectivity
            client.list_secrets(MaxResults=1)
            return True
        except Exception as e:
            self.logger.error("AWS Secrets Manager health check failed", error=str(e))
            return False
    
    async def get_secret(self, key: str, version: str = "latest") -> SecretValue:
        """Get secret from AWS Secrets Manager."""
        try:
            client = await self._get_client()
            
            kwargs = {"SecretId": key}
            if version != "latest":
                kwargs["VersionId"] = version
            
            response = client.get_secret_value(**kwargs)
            
            return SecretValue(
                key=key,
                value=response["SecretString"],
                version=response.get("VersionId", "1"),
                created_at=response.get("CreatedDate", datetime.now(timezone.utc)),
                metadata={"arn": response.get("ARN")}
            )
            
        except Exception as e:
            if "ResourceNotFoundException" in str(e):
                raise SecretNotFoundError(f"Secret not found: {key}")
            self.logger.error("Failed to get secret from AWS", key=key, error=str(e))
            raise SecretAccessError(f"Failed to get secret: {str(e)}")
    
    async def set_secret(self, key: str, value: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Set secret in AWS Secrets Manager."""
        try:
            client = await self._get_client()
            
            # Try to update existing secret first
            try:
                response = client.update_secret(
                    SecretId=key,
                    SecretString=value,
                    Description=metadata.get("description", "") if metadata else ""
                )
                return response["VersionId"]
            except client.exceptions.ResourceNotFoundException:
                # Create new secret
                response = client.create_secret(
                    Name=key,
                    SecretString=value,
                    Description=metadata.get("description", "") if metadata else ""
                )
                return response["VersionId"]
                
        except Exception as e:
            self.logger.error("Failed to set secret in AWS", key=key, error=str(e))
            raise SecretAccessError(f"Failed to set secret: {str(e)}")
    
    async def delete_secret(self, key: str) -> bool:
        """Delete secret from AWS Secrets Manager."""
        try:
            client = await self._get_client()
            client.delete_secret(SecretId=key, ForceDeleteWithoutRecovery=True)
            return True
        except Exception as e:
            self.logger.error("Failed to delete secret from AWS", key=key, error=str(e))
            return False
    
    async def list_secrets(self, prefix: str = "") -> List[str]:
        """List secrets in AWS Secrets Manager."""
        try:
            client = await self._get_client()
            
            secrets = []
            paginator = client.get_paginator('list_secrets')
            
            for page in paginator.paginate():
                for secret in page['SecretList']:
                    name = secret['Name']
                    if not prefix or name.startswith(prefix):
                        secrets.append(name)
            
            return secrets
            
        except Exception as e:
            self.logger.error("Failed to list secrets from AWS", prefix=prefix, error=str(e))
            return []


class AzureKeyVaultBackend(SecretBackend):
    """Azure Key Vault backend."""
    
    def __init__(self, vault_url: str, tenant_id: str, client_id: str, client_secret: str, **kwargs):
        super().__init__("azure_keyvault", kwargs)
        self.vault_url = vault_url.rstrip('/')
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._client = None
    
    @property
    def backend_type(self) -> SecretBackendType:
        return SecretBackendType.AZURE_KEY_VAULT
    
    async def _get_client(self):
        """Get Azure Key Vault client."""
        if not self._client:
            try:
                from azure.keyvault.secrets import SecretClient
                from azure.identity import ClientSecretCredential
                
                credential = ClientSecretCredential(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
                self._client = SecretClient(vault_url=self.vault_url, credential=credential)
            except ImportError:
                raise SecretConfigurationError("azure-keyvault-secrets library required for Azure Key Vault")
        return self._client
    
    async def health_check(self) -> bool:
        """Check Azure Key Vault health."""
        try:
            client = await self._get_client()
            # Try to list secrets to verify connectivity
            list(client.list_properties_of_secrets(max_page_size=1))
            return True
        except Exception as e:
            self.logger.error("Azure Key Vault health check failed", error=str(e))
            return False
    
    async def get_secret(self, key: str, version: str = "latest") -> SecretValue:
        """Get secret from Azure Key Vault."""
        try:
            client = await self._get_client()
            
            # Azure Key Vault doesn't allow slashes in secret names
            azure_key = key.replace('/', '-')
            
            if version == "latest":
                secret = client.get_secret(azure_key)
            else:
                secret = client.get_secret(azure_key, version=version)
            
            return SecretValue(
                key=key,
                value=secret.value,
                version=secret.properties.version,
                created_at=secret.properties.created_on,
                metadata={"vault_url": self.vault_url}
            )
            
        except Exception as e:
            if "SecretNotFound" in str(e):
                raise SecretNotFoundError(f"Secret not found: {key}")
            self.logger.error("Failed to get secret from Azure", key=key, error=str(e))
            raise SecretAccessError(f"Failed to get secret: {str(e)}")
    
    async def set_secret(self, key: str, value: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Set secret in Azure Key Vault."""
        try:
            client = await self._get_client()
            
            # Azure Key Vault doesn't allow slashes in secret names
            azure_key = key.replace('/', '-')
            
            secret = client.set_secret(azure_key, value)
            return secret.properties.version
            
        except Exception as e:
            self.logger.error("Failed to set secret in Azure", key=key, error=str(e))
            raise SecretAccessError(f"Failed to set secret: {str(e)}")
    
    async def delete_secret(self, key: str) -> bool:
        """Delete secret from Azure Key Vault."""
        try:
            client = await self._get_client()
            azure_key = key.replace('/', '-')
            client.begin_delete_secret(azure_key)
            return True
        except Exception as e:
            self.logger.error("Failed to delete secret from Azure", key=key, error=str(e))
            return False
    
    async def list_secrets(self, prefix: str = "") -> List[str]:
        """List secrets in Azure Key Vault."""
        try:
            client = await self._get_client()
            
            secrets = []
            for secret_property in client.list_properties_of_secrets():
                name = secret_property.name.replace('-', '/')  # Convert back from Azure format
                if not prefix or name.startswith(prefix):
                    secrets.append(name)
            
            return secrets
            
        except Exception as e:
            self.logger.error("Failed to list secrets from Azure", prefix=prefix, error=str(e))
            return []


class KubernetesSecretsBackend(SecretBackend):
    """Kubernetes Secrets backend."""
    
    def __init__(self, namespace: str = "default", kubeconfig_path: str = None, **kwargs):
        super().__init__("kubernetes", kwargs)
        self.namespace = namespace
        self.kubeconfig_path = kubeconfig_path
        self._client = None
    
    @property
    def backend_type(self) -> SecretBackendType:
        return SecretBackendType.KUBERNETES
    
    async def _get_client(self):
        """Get Kubernetes client."""
        if not self._client:
            try:
                from kubernetes import client, config
                
                if self.kubeconfig_path:
                    config.load_kube_config(config_file=self.kubeconfig_path)
                else:
                    try:
                        config.load_incluster_config()
                    except:
                        config.load_kube_config()
                
                self._client = client.CoreV1Api()
            except ImportError:
                raise SecretConfigurationError("kubernetes library required for Kubernetes backend")
        return self._client
    
    async def health_check(self) -> bool:
        """Check Kubernetes health."""
        try:
            client = await self._get_client()
            client.list_namespaced_secret(namespace=self.namespace, limit=1)
            return True
        except Exception as e:
            self.logger.error("Kubernetes health check failed", error=str(e))
            return False
    
    async def get_secret(self, key: str, version: str = "latest", field: str = None) -> SecretValue:
        """Get secret from Kubernetes."""
        try:
            client = await self._get_client()
            
            # In K8s, key is the secret name, field is the data key
            secret_name = key.replace('/', '-')  # K8s doesn't allow slashes
            
            secret = client.read_namespaced_secret(name=secret_name, namespace=self.namespace)
            
            if field and field in secret.data:
                # Decode base64 value
                value = base64.b64decode(secret.data[field]).decode('utf-8')
            elif len(secret.data) == 1:
                # Single field secret
                value = base64.b64decode(list(secret.data.values())[0]).decode('utf-8')
            else:
                # Multiple fields - return as JSON
                decoded_data = {}
                for k, v in secret.data.items():
                    decoded_data[k] = base64.b64decode(v).decode('utf-8')
                value = json.dumps(decoded_data)
            
            return SecretValue(
                key=key,
                value=value,
                version=secret.metadata.resource_version,
                created_at=secret.metadata.creation_timestamp,
                metadata={"namespace": self.namespace}
            )
            
        except Exception as e:
            if "NotFound" in str(e):
                raise SecretNotFoundError(f"Secret not found: {key}")
            self.logger.error("Failed to get secret from Kubernetes", key=key, error=str(e))
            raise SecretAccessError(f"Failed to get secret: {str(e)}")
    
    async def set_secret(self, key: str, value: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Set secret in Kubernetes."""
        try:
            from kubernetes import client
            
            k8s_client = await self._get_client()
            secret_name = key.replace('/', '-')
            field_name = metadata.get('field', 'value') if metadata else 'value'
            
            # Encode value
            encoded_value = base64.b64encode(value.encode('utf-8')).decode('utf-8')
            
            secret_body = client.V1Secret(
                metadata=client.V1ObjectMeta(name=secret_name),
                data={field_name: encoded_value}
            )
            
            try:
                # Try to update existing secret
                result = k8s_client.patch_namespaced_secret(
                    name=secret_name,
                    namespace=self.namespace,
                    body=secret_body
                )
            except:
                # Create new secret
                result = k8s_client.create_namespaced_secret(
                    namespace=self.namespace,
                    body=secret_body
                )
            
            return result.metadata.resource_version
            
        except Exception as e:
            self.logger.error("Failed to set secret in Kubernetes", key=key, error=str(e))
            raise SecretAccessError(f"Failed to set secret: {str(e)}")
    
    async def delete_secret(self, key: str) -> bool:
        """Delete secret from Kubernetes."""
        try:
            client = await self._get_client()
            secret_name = key.replace('/', '-')
            client.delete_namespaced_secret(name=secret_name, namespace=self.namespace)
            return True
        except Exception as e:
            self.logger.error("Failed to delete secret from Kubernetes", key=key, error=str(e))
            return False
    
    async def list_secrets(self, prefix: str = "") -> List[str]:
        """List secrets in Kubernetes."""
        try:
            client = await self._get_client()
            
            secrets = []
            secret_list = client.list_namespaced_secret(namespace=self.namespace)
            
            for secret in secret_list.items:
                name = secret.metadata.name.replace('-', '/')  # Convert back
                if not prefix or name.startswith(prefix):
                    secrets.append(name)
            
            return secrets
            
        except Exception as e:
            self.logger.error("Failed to list secrets from Kubernetes", prefix=prefix, error=str(e))
            return []


class LocalFileBackend(SecretBackend):
    """Local file-based backend with encryption."""
    
    def __init__(self, secrets_dir: str, encryption_key: str = None, **kwargs):
        super().__init__("local_file", kwargs)
        self.secrets_dir = Path(secrets_dir)
        self.encryption_key = encryption_key or Fernet.generate_key().decode()
        self.fernet = Fernet(self.encryption_key.encode() if isinstance(self.encryption_key, str) else self.encryption_key)
    
    @property
    def backend_type(self) -> SecretBackendType:
        return SecretBackendType.LOCAL_FILE
    
    async def health_check(self) -> bool:
        """Check local file backend health."""
        try:
            return self.secrets_dir.exists() and self.secrets_dir.is_dir()
        except Exception:
            return False
    
    def _get_secret_path(self, key: str) -> Path:
        """Get file path for secret."""
        # Convert key to safe filename
        safe_key = key.replace('/', '_').replace('\\', '_')
        return self.secrets_dir / f"{safe_key}.json"
    
    def _encrypt(self, data: str) -> str:
        """Encrypt data."""
        return self.fernet.encrypt(data.encode()).decode()
    
    def _decrypt(self, encrypted_data: str) -> str:
        """Decrypt data."""
        return self.fernet.decrypt(encrypted_data.encode()).decode()
    
    async def get_secret(self, key: str, version: str = "latest") -> SecretValue:
        """Get secret from local file."""
        try:
            secret_path = self._get_secret_path(key)
            
            if not secret_path.exists():
                raise SecretNotFoundError(f"Secret not found: {key}")
            
            with open(secret_path, 'r') as f:
                encrypted_data = f.read()
            
            decrypted_data = self._decrypt(encrypted_data)
            secret_data = json.loads(decrypted_data)
            
            return SecretValue(
                key=key,
                value=secret_data["value"],
                version=secret_data.get("version", "1"),
                created_at=datetime.fromisoformat(secret_data["created_at"]),
                expires_at=datetime.fromisoformat(secret_data["expires_at"]) if secret_data.get("expires_at") else None,
                metadata=secret_data.get("metadata", {})
            )
            
        except SecretNotFoundError:
            raise
        except Exception as e:
            self.logger.error("Failed to get secret from local file", key=key, error=str(e))
            raise SecretAccessError(f"Failed to get secret: {str(e)}")
    
    async def set_secret(self, key: str, value: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Set secret in local file."""
        try:
            self.secrets_dir.mkdir(parents=True, exist_ok=True)
            
            secret_path = self._get_secret_path(key)
            version = str(int(time.time()))
            
            secret_data = {
                "value": value,
                "version": version,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata or {}
            }
            
            encrypted_data = self._encrypt(json.dumps(secret_data))
            
            with open(secret_path, 'w') as f:
                f.write(encrypted_data)
            
            return version
            
        except Exception as e:
            self.logger.error("Failed to set secret in local file", key=key, error=str(e))
            raise SecretAccessError(f"Failed to set secret: {str(e)}")
    
    async def delete_secret(self, key: str) -> bool:
        """Delete secret from local file."""
        try:
            secret_path = self._get_secret_path(key)
            if secret_path.exists():
                secret_path.unlink()
                return True
            return False
        except Exception as e:
            self.logger.error("Failed to delete secret from local file", key=key, error=str(e))
            return False
    
    async def list_secrets(self, prefix: str = "") -> List[str]:
        """List secrets in local files."""
        try:
            if not self.secrets_dir.exists():
                return []
            
            secrets = []
            for file_path in self.secrets_dir.glob("*.json"):
                # Convert filename back to key
                key = file_path.stem.replace('_', '/')
                if not prefix or key.startswith(prefix):
                    secrets.append(key)
            
            return secrets
            
        except Exception as e:
            self.logger.error("Failed to list secrets from local files", prefix=prefix, error=str(e))
            return []


class ExternalSecretsManager:
    """Main external secrets manager."""
    
    def __init__(self, cache_ttl: int = 300, enable_audit_logging: bool = True):
        self.backends: Dict[str, SecretBackend] = {}
        self.rotation_policies: Dict[str, RotationPolicy] = {}
        self.cache_ttl = cache_ttl
        self.enable_audit_logging = enable_audit_logging
        
        # Cache for secrets
        self._cache: Dict[str, tuple] = {}  # key -> (secret_value, timestamp)
        self._cache_lock = asyncio.Lock()
        
        # Audit log
        self._audit_log: List[Dict[str, Any]] = []
        
        self.logger = logger.bind(component="external_secrets_manager")
    
    def register_backend(self, name: str, backend: SecretBackend):
        """Register a secret backend."""
        self.backends[name] = backend
        self.logger.info("Registered secret backend", backend_name=name, backend_type=backend.backend_type)
    
    def register_rotation_policy(self, key_pattern: str, policy: RotationPolicy):
        """Register a rotation policy for matching keys."""
        self.rotation_policies[key_pattern] = policy
        self.logger.info("Registered rotation policy", pattern=key_pattern, interval=policy.interval_days)
    
    def _get_rotation_policy(self, key: str) -> Optional[RotationPolicy]:
        """Get rotation policy for a key."""
        for pattern, policy in self.rotation_policies.items():
            if fnmatch.fnmatch(key, pattern):
                return policy
        return None
    
    async def _get_from_cache(self, cache_key: str) -> Optional[SecretValue]:
        """Get secret from cache if valid."""
        async with self._cache_lock:
            if cache_key in self._cache:
                secret_value, timestamp = self._cache[cache_key]
                if time.time() - timestamp < self.cache_ttl:
                    return secret_value
                else:
                    # Expired, remove from cache
                    del self._cache[cache_key]
        return None
    
    async def _set_cache(self, cache_key: str, secret_value: SecretValue):
        """Set secret in cache."""
        async with self._cache_lock:
            self._cache[cache_key] = (secret_value, time.time())
    
    def _clear_cache(self, cache_key: str = None):
        """Clear cache entry or all cache."""
        async def _clear():
            async with self._cache_lock:
                if cache_key:
                    self._cache.pop(cache_key, None)
                else:
                    self._cache.clear()
        
        asyncio.create_task(_clear())
    
    async def _log_access(self, secret_key: str, backend: str, user_id: str = None, operation: str = "get"):
        """Log secret access for audit."""
        if not self.enable_audit_logging:
            return
        
        audit_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "secret_key": secret_key,
            "backend": backend,
            "user_id": user_id,
            "operation": operation
        }
        
        self._audit_log.append(audit_entry)
        
        # Keep only last 10000 entries
        if len(self._audit_log) > 10000:
            self._audit_log = self._audit_log[-5000:]
    
    async def get_secret(self, key: str, backend: str, version: str = "latest", user_id: str = None) -> SecretValue:
        """Get secret from specified backend."""
        if backend not in self.backends:
            raise SecretConfigurationError(f"Backend not found: {backend}")
        
        # Check cache first
        cache_key = f"{backend}:{key}:{version}"
        cached_secret = await self._get_from_cache(cache_key)
        if cached_secret:
            await self._log_access(key, backend, user_id, "get_cached")
            return cached_secret
        
        # Get from backend
        backend_instance = self.backends[backend]
        secret_value = await backend_instance.get_secret(key, version)
        
        # Cache the result
        await self._set_cache(cache_key, secret_value)
        
        # Log access
        await self._log_access(key, backend, user_id, "get")
        
        return secret_value
    
    async def set_secret(self, key: str, value: str, backend: str, metadata: Optional[Dict[str, Any]] = None, user_id: str = None) -> str:
        """Set secret in specified backend."""
        if backend not in self.backends:
            raise SecretConfigurationError(f"Backend not found: {backend}")
        
        backend_instance = self.backends[backend]
        version = await backend_instance.set_secret(key, value, metadata)
        
        # Clear cache for this key
        cache_pattern = f"{backend}:{key}:"
        self._clear_cache(cache_pattern)
        
        # Log access
        await self._log_access(key, backend, user_id, "set")
        
        return version
    
    async def delete_secret(self, key: str, backend: str, user_id: str = None) -> bool:
        """Delete secret from specified backend."""
        if backend not in self.backends:
            raise SecretConfigurationError(f"Backend not found: {backend}")
        
        backend_instance = self.backends[backend]
        success = await backend_instance.delete_secret(key)
        
        if success:
            # Clear cache for this key
            cache_pattern = f"{backend}:{key}:"
            self._clear_cache(cache_pattern)
            
            # Log access
            await self._log_access(key, backend, user_id, "delete")
        
        return success
    
    async def list_secrets(self, backend: str, prefix: str = "", user_id: str = None) -> List[str]:
        """List secrets from specified backend."""
        if backend not in self.backends:
            raise SecretConfigurationError(f"Backend not found: {backend}")
        
        backend_instance = self.backends[backend]
        secrets = await backend_instance.list_secrets(prefix)
        
        # Log access
        await self._log_access(f"list:{prefix}", backend, user_id, "list")
        
        return secrets
    
    async def rotate_secret(self, key: str, backend: str, user_id: str = None) -> str:
        """Rotate a secret."""
        if backend not in self.backends:
            raise SecretConfigurationError(f"Backend not found: {backend}")
        
        # Get rotation policy
        policy = self._get_rotation_policy(key)
        if not policy or not policy.enabled:
            raise SecretRotationError(f"No rotation policy configured for key: {key}")
        
        # Generate new secret value
        if policy.rotation_function:
            new_value = await self._call_rotation_function(policy.rotation_function, key)
        else:
            new_value = self._generate_random_secret()
        
        # Set new secret
        backend_instance = self.backends[backend]
        rotation_metadata = {
            "rotated_at": datetime.now(timezone.utc).isoformat(),
            "rotated_by": user_id,
            "rotation_policy": policy.rotation_function
        }
        
        version = await backend_instance.set_secret(key, new_value, rotation_metadata)
        
        # Clear cache
        cache_pattern = f"{backend}:{key}:"
        self._clear_cache(cache_pattern)
        
        # Log rotation
        await self._log_access(key, backend, user_id, "rotate")
        
        return version
    
    async def _call_rotation_function(self, function_name: str, key: str) -> str:
        """Call custom rotation function."""
        # This would integrate with a function registry or plugin system
        # For now, return a mock implementation
        return f"rotated_{key}_{int(time.time())}"
    
    def _generate_random_secret(self, length: int = 32) -> str:
        """Generate random secret."""
        import secrets
        import string
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(length))
    
    async def check_rotations_needed(self) -> List[Dict[str, Any]]:
        """Check which secrets need rotation."""
        rotations_needed = []
        
        for backend_name, backend_instance in self.backends.items():
            try:
                # Get all secrets from backend
                all_secrets = await backend_instance.list_secrets()
                
                for secret_key in all_secrets:
                    policy = self._get_rotation_policy(secret_key)
                    if not policy or not policy.enabled:
                        continue
                    
                    try:
                        secret = await backend_instance.get_secret(secret_key)
                        last_rotated = secret.last_rotated or secret.created_at
                        
                        if policy.is_rotation_due(last_rotated):
                            rotations_needed.append({
                                "key": secret_key,
                                "backend": backend_name,
                                "last_rotated": last_rotated.isoformat(),
                                "next_rotation": policy.calculate_next_rotation(last_rotated).isoformat()
                            })
                    except Exception as e:
                        self.logger.warning("Failed to check rotation for secret", key=secret_key, error=str(e))
                        
            except Exception as e:
                self.logger.error("Failed to check rotations for backend", backend=backend_name, error=str(e))
        
        return rotations_needed
    
    async def get_secret_with_fallback(self, key: str, backends: List[str], version: str = "latest", user_id: str = None) -> SecretValue:
        """Get secret with fallback to other backends."""
        last_error = None
        
        for backend_name in backends:
            try:
                return await self.get_secret(key, backend_name, version, user_id)
            except (SecretNotFoundError, SecretAccessError) as e:
                last_error = e
                continue
        
        raise last_error or SecretNotFoundError(f"Secret not found in any backend: {key}")
    
    async def sync_secrets(self, source_backend: str, target_backend: str, key_pattern: str = "*", user_id: str = None):
        """Synchronize secrets between backends."""
        if source_backend not in self.backends or target_backend not in self.backends:
            raise SecretConfigurationError("Invalid backend specified")
        
        source = self.backends[source_backend]
        target = self.backends[target_backend]
        
        # Get all secrets from source
        all_secrets = await source.list_secrets()
        
        for secret_key in all_secrets:
            if fnmatch.fnmatch(secret_key, key_pattern):
                try:
                    secret = await source.get_secret(secret_key)
                    await target.set_secret(secret_key, secret.value, secret.metadata)
                    self.logger.info("Synced secret", key=secret_key, source=source_backend, target=target_backend)
                except Exception as e:
                    self.logger.error("Failed to sync secret", key=secret_key, error=str(e))
    
    async def set_secrets_bulk(self, secrets: Dict[str, str], backend: str, user_id: str = None):
        """Set multiple secrets in bulk."""
        if backend not in self.backends:
            raise SecretConfigurationError(f"Backend not found: {backend}")
        
        backend_instance = self.backends[backend]
        
        # Set secrets concurrently
        tasks = []
        for key, value in secrets.items():
            task = backend_instance.set_secret(key, value, {"bulk_operation": True})
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log bulk operation
        await self._log_access(f"bulk:{len(secrets)}_secrets", backend, user_id, "set_bulk")
    
    async def health_check(self) -> Dict[str, bool]:
        """Check health of all backends."""
        health_status = {}
        
        # Check all backends concurrently
        tasks = []
        for name, backend in self.backends.items():
            tasks.append((name, backend.health_check()))
        
        results = await asyncio.gather(*[task[1] for task in tasks], return_exceptions=True)
        
        all_healthy = True
        for (name, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                health_status[name] = False
                all_healthy = False
            else:
                health_status[name] = result
                if not result:
                    all_healthy = False
        
        health_status["overall"] = all_healthy
        return health_status
    
    def get_audit_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get audit log entries."""
        return self._audit_log[-limit:]
    
    def clear_cache(self):
        """Clear all cached secrets."""
        self._clear_cache()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "cache_size": len(self._cache),
            "cache_ttl": self.cache_ttl,
            "audit_log_size": len(self._audit_log)
        }


# Global secrets manager instance
secrets_manager = ExternalSecretsManager()