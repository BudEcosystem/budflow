"""Comprehensive test suite for credentials API."""

import json
from typing import Any, Dict
from uuid import uuid4

import pytest
from fastapi import status
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.auth.models import User
from budflow.credentials.models import Credential, CredentialType


class TestCredentialsAPI:
    """Test cases for credential REST API endpoints."""

    @pytest.fixture
    async def test_credential_type_data(self) -> Dict[str, Any]:
        """Sample credential type data."""
        return {
            "name": "httpBasicAuth",
            "displayName": "Basic Auth",
            "properties": [
                {
                    "name": "username",
                    "displayName": "Username",
                    "type": "string",
                    "required": True
                },
                {
                    "name": "password",
                    "displayName": "Password",
                    "type": "password",
                    "required": True
                }
            ]
        }

    @pytest.fixture
    async def test_credential_data(self) -> Dict[str, Any]:
        """Sample credential data for testing."""
        return {
            "name": "Test HTTP Credential",
            "type": "httpBasicAuth",
            "data": {
                "username": "testuser",
                "password": "testpass123"
            }
        }

    @pytest.fixture
    async def test_user(self, test_session: AsyncSession, helpers):
        """Create a test user."""
        from budflow.auth.security import get_password_hash
        user_data = {
            "email": "test@example.com",
            "password_hash": get_password_hash("testpassword123"),
            "first_name": "Test",
            "last_name": "User",
            "is_active": True,
            "is_verified": True,
        }
        user = await helpers.create_test_user(test_session, **user_data)
        return user
    
    @pytest.fixture
    async def test_user2(self, test_session: AsyncSession, helpers):
        """Create a second test user."""
        from budflow.auth.security import get_password_hash
        user_data = {
            "email": "test2@example.com",
            "password_hash": get_password_hash("testpassword123"),
            "first_name": "Test2",
            "last_name": "User2",
            "is_active": True,
            "is_verified": True,
        }
        user = await helpers.create_test_user(test_session, **user_data)
        return user
    
    @pytest.fixture
    async def auth_token(self, async_client: AsyncClient) -> str:
        """Get auth token for test user."""
        # First create user
        register_data = {
            "email": "test@example.com",
            "password": "testpassword123",
            "first_name": "Test",
            "last_name": "User",
        }
        reg_response = await async_client.post("/api/v1/auth/register", json=register_data)
        assert reg_response.status_code == 201, f"Registration failed: {reg_response.json()}"
        
        # Then login
        login_data = {
            "username": "test@example.com",
            "password": "testpassword123",
        }
        response = await async_client.post("/api/v1/auth/login", data=login_data)
        assert response.status_code == 200, f"Login failed: {response.json()}"
        return response.json()["access_token"]
    
    @pytest.fixture
    def auth_headers(self, auth_token: str) -> Dict[str, str]:
        """Get auth headers."""
        return {"Authorization": f"Bearer {auth_token}"}
    
    @pytest.fixture
    async def auth_headers_user2(self, async_client: AsyncClient) -> Dict[str, str]:
        """Get auth headers for second user."""
        # First create user
        register_data = {
            "email": "test2@example.com",
            "password": "testpassword123",
            "first_name": "Test2",
            "last_name": "User2",
        }
        await async_client.post("/api/v1/auth/register", json=register_data)
        
        # Then login
        login_data = {
            "username": "test2@example.com",
            "password": "testpassword123",
        }
        response = await async_client.post("/api/v1/auth/login", data=login_data)
        token = response.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    @pytest.fixture
    async def test_credential(
        self,
        test_session: AsyncSession,
        test_user: User
    ) -> Credential:
        """Create a test credential."""
        credential = Credential(
            name="Test API Key",
            type="apiKey",
            user_id=test_user.id,
            data={"apiKey": "test-key-123"},  # Will be encrypted
            settings={"timeout": 30}
        )
        test_session.add(credential)
        await test_session.commit()
        await test_session.refresh(credential)
        return credential

    @pytest.mark.asyncio
    async def test_create_credential(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_credential_data: Dict[str, Any]
    ):
        """Test creating a new credential."""
        response = await async_client.post(
            "/api/v1/credentials",
            json=test_credential_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        
        assert data["name"] == test_credential_data["name"]
        assert data["type"] == test_credential_data["type"]
        assert "id" in data
        assert "data" not in data  # Encrypted data should not be returned

    @pytest.mark.asyncio
    async def test_create_credential_duplicate_name(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_credential: Credential,
        test_credential_data: Dict[str, Any]
    ):
        """Test creating credential with duplicate name."""
        test_credential_data["name"] = test_credential.name
        
        response = await async_client.post(
            "/api/v1/credentials",
            json=test_credential_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "already exists" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_list_credentials(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_session: AsyncSession,
        test_user: User
    ):
        """Test listing credentials."""
        # Create test credentials
        for i in range(5):
            credential = Credential(
                name=f"Test Credential {i}",
                type="apiKey" if i % 2 == 0 else "oauth2",
                user_id=test_user.id,
                data={"key": f"value_{i}"}
            )
            test_session.add(credential)
        await test_session.commit()

        response = await async_client.get(
            "/api/v1/credentials",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] >= 5
        assert len(data["items"]) >= 5
        
        # Verify no sensitive data is exposed
        for item in data["items"]:
            assert "data" not in item

    @pytest.mark.asyncio
    async def test_get_credential(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_credential: Credential
    ):
        """Test getting a specific credential."""
        response = await async_client.get(
            f"/api/v1/credentials/{test_credential.id}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["id"] == str(test_credential.id)
        assert data["name"] == test_credential.name
        assert "data" not in data  # Encrypted data should not be returned

    @pytest.mark.asyncio
    async def test_update_credential(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_credential: Credential
    ):
        """Test updating a credential."""
        update_data = {
            "name": "Updated Credential Name",
            "data": {
                "apiKey": "new-key-456"
            }
        }
        
        response = await async_client.patch(
            f"/api/v1/credentials/{test_credential.id}",
            json=update_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["name"] == update_data["name"]

    @pytest.mark.asyncio
    async def test_delete_credential(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_credential: Credential
    ):
        """Test deleting a credential."""
        response = await async_client.delete(
            f"/api/v1/credentials/{test_credential.id}",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_204_NO_CONTENT
        
        # Verify credential is deleted
        response = await async_client.get(
            f"/api/v1/credentials/{test_credential.id}",
            headers=auth_headers
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_test_credential(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_credential: Credential
    ):
        """Test testing a credential."""
        response = await async_client.post(
            f"/api/v1/credentials/{test_credential.id}/test",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "success" in data
        assert "message" in data

    @pytest.mark.asyncio
    async def test_share_credential(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_credential: Credential,
        test_user2: User
    ):
        """Test sharing a credential with another user."""
        share_data = {
            "userIds": [str(test_user2.id)],
            "permissions": ["use"]
        }
        
        response = await async_client.put(
            f"/api/v1/credentials/{test_credential.id}/share",
            json=share_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "sharedWith" in data
        assert len(data["sharedWith"]) == 1

    @pytest.mark.asyncio
    async def test_credential_permissions(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        auth_headers_user2: Dict[str, str],
        test_credential: Credential
    ):
        """Test credential permission enforcement."""
        # User 2 should not have access
        response = await async_client.get(
            f"/api/v1/credentials/{test_credential.id}",
            headers=auth_headers_user2
        )
        
        assert response.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_list_credential_types(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str]
    ):
        """Test listing available credential types."""
        response = await async_client.get(
            "/api/v1/credentials/types",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "types" in data
        assert len(data["types"]) > 0
        
        # Check structure of credential types
        for cred_type in data["types"]:
            assert "name" in cred_type
            assert "displayName" in cred_type
            assert "properties" in cred_type

    @pytest.mark.asyncio
    async def test_oauth2_credential_flow(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str]
    ):
        """Test OAuth2 credential creation and authorization flow."""
        oauth_data = {
            "name": "Test OAuth2 Credential",
            "type": "oauth2",
            "data": {
                "clientId": "test-client-id",
                "clientSecret": "test-client-secret",
                "authUrl": "https://example.com/oauth/authorize",
                "accessTokenUrl": "https://example.com/oauth/token"
            }
        }
        
        response = await async_client.post(
            "/api/v1/credentials",
            json=oauth_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        credential_id = data["id"]
        
        # Get OAuth authorization URL
        response = await async_client.get(
            f"/api/v1/credentials/{credential_id}/oauth/auth-url",
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "authUrl" in data
        assert "state" in data

    @pytest.mark.asyncio
    async def test_credential_usage_in_workflow(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_credential: Credential
    ):
        """Test using credential in workflow nodes."""
        workflow_data = {
            "name": "Test Workflow with Credential",
            "nodes": [
                {
                    "id": "http_1",
                    "type": "action.httpRequest",
                    "name": "HTTP Request",
                    "position": [100, 100],
                    "parameters": {
                        "url": "https://api.example.com/data",
                        "method": "GET",
                        "authentication": "credential",
                        "credential": {
                            "id": str(test_credential.id),
                            "name": test_credential.name
                        }
                    }
                }
            ],
            "connections": []
        }
        
        response = await async_client.post(
            "/api/v1/workflows",
            json=workflow_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_201_CREATED

    @pytest.mark.asyncio
    async def test_credential_encryption(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_session: AsyncSession,
        test_user: User
    ):
        """Test that credentials are properly encrypted in database."""
        credential_data = {
            "name": "Encryption Test",
            "type": "apiKey",
            "data": {
                "apiKey": "super-secret-key-12345"
            }
        }
        
        response = await async_client.post(
            "/api/v1/credentials",
            json=credential_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        credential_id = response.json()["id"]
        
        # Check database directly
        db_credential = await test_session.get(Credential, credential_id)
        assert db_credential is not None
        
        # Encrypted data should not contain the plain text
        assert "super-secret-key-12345" not in str(db_credential.data)
        assert db_credential.data != credential_data["data"]

    @pytest.mark.asyncio
    async def test_bulk_delete_credentials(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str],
        test_session: AsyncSession,
        test_user: User
    ):
        """Test bulk deletion of credentials."""
        credential_ids = []
        for i in range(3):
            credential = Credential(
                name=f"Bulk Delete Test {i}",
                type="apiKey",
                user_id=test_user.id,
                data={"key": f"value_{i}"}
            )
            test_session.add(credential)
            await test_session.flush()
            credential_ids.append(str(credential.id))
        await test_session.commit()

        response = await async_client.post(
            "/api/v1/credentials/bulk/delete",
            json={"credentialIds": credential_ids},
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["deleted"] == 3

    @pytest.mark.asyncio
    async def test_credential_with_expiry(
        self,
        async_client: AsyncClient,
        auth_headers: Dict[str, str]
    ):
        """Test credential with expiry date."""
        credential_data = {
            "name": "Expiring Credential",
            "type": "apiKey",
            "data": {
                "apiKey": "temp-key-123"
            },
            "settings": {
                "expiresAt": "2025-12-31T23:59:59Z"
            }
        }
        
        response = await async_client.post(
            "/api/v1/credentials",
            json=credential_data,
            headers=auth_headers
        )
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["settings"]["expiresAt"] == credential_data["settings"]["expiresAt"]