"""Test authentication system."""

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.auth.models import User, Role, Permission
from budflow.auth.schemas import UserCreate, LoginRequest, ChangePasswordRequest
from budflow.auth.service import UserService, AuthenticationService
from budflow.auth.security import password_manager, token_manager, mfa_manager


@pytest.mark.unit
def test_password_manager():
    """Test password hashing and verification."""
    password = "testpassword123"
    
    # Test hashing
    hash1 = password_manager.hash_password(password)
    hash2 = password_manager.hash_password(password)
    
    # Hashes should be different (due to salt)
    assert hash1 != hash2
    
    # Both should verify correctly
    assert password_manager.verify_password(password, hash1)
    assert password_manager.verify_password(password, hash2)
    
    # Wrong password should not verify
    assert not password_manager.verify_password("wrongpassword", hash1)


@pytest.mark.unit
def test_password_strength_validation():
    """Test password strength validation."""
    # Weak passwords
    weak_passwords = [
        "short",
        "password",
        "12345678",
        "UPPERCASE",
        "lowercase",
        "NoSpecial123"
    ]
    
    for pwd in weak_passwords:
        is_strong, issues = password_manager.is_password_strong(pwd)
        assert not is_strong
        assert len(issues) > 0
    
    # Strong password
    strong_password = "StrongP@ssw0rd123"
    is_strong, issues = password_manager.is_password_strong(strong_password)
    assert is_strong
    assert len(issues) == 0


@pytest.mark.unit
def test_token_manager():
    """Test JWT token creation and verification."""
    data = {"user_id": 123, "email": "test@example.com"}
    
    # Create access token
    access_token = token_manager.create_access_token(data)
    assert access_token is not None
    
    # Verify access token
    payload = token_manager.verify_token(access_token, "access")
    assert payload is not None
    assert payload["user_id"] == 123
    assert payload["type"] == "access"
    
    # Create refresh token
    refresh_token = token_manager.create_refresh_token(data)
    assert refresh_token is not None
    
    # Verify refresh token
    payload = token_manager.verify_token(refresh_token, "refresh")
    assert payload is not None
    assert payload["user_id"] == 123
    assert payload["type"] == "refresh"
    
    # Wrong token type should fail
    assert token_manager.verify_token(access_token, "refresh") is None
    assert token_manager.verify_token(refresh_token, "access") is None


@pytest.mark.unit
def test_mfa_manager():
    """Test MFA functionality."""
    # Generate secret
    secret = mfa_manager.generate_secret()
    assert len(secret) > 0
    
    # Generate QR code URL
    qr_url = mfa_manager.generate_qr_code_url(secret, "test@example.com")
    assert "otpauth://" in qr_url
    assert "test@example.com" in qr_url
    
    # Generate recovery codes
    codes = mfa_manager.generate_recovery_codes(5)
    assert len(codes) == 5
    assert all(len(code) == 8 for code in codes)
    
    # Hash recovery codes
    hashed = mfa_manager.hash_recovery_codes(codes)
    assert hashed is not None
    
    # Verify recovery code
    is_valid, new_hash = mfa_manager.verify_recovery_code(hashed, codes[0])
    assert is_valid
    assert new_hash != hashed  # Should be updated after use
    
    # Verify same code again should fail
    is_valid, _ = mfa_manager.verify_recovery_code(new_hash, codes[0])
    assert not is_valid


@pytest.mark.integration
async def test_user_service_create_user(test_session: AsyncSession):
    """Test user creation service."""
    user_service = UserService(test_session)
    
    user_data = UserCreate(
        email="test@example.com",
        password="StrongP@ssw0rd123",
        confirm_password="StrongP@ssw0rd123",
        first_name="Test",
        last_name="User"
    )
    
    user = await user_service.create_user(user_data)
    
    assert user.id is not None
    assert user.email == "test@example.com"
    assert user.first_name == "Test"
    assert user.last_name == "User"
    assert user.is_active is True
    assert user.mfa_enabled is False
    assert password_manager.verify_password("StrongP@ssw0rd123", user.password_hash)


@pytest.mark.integration
async def test_user_service_duplicate_email(test_session: AsyncSession):
    """Test that duplicate email creation fails."""
    user_service = UserService(test_session)
    
    user_data = UserCreate(
        email="duplicate@example.com",
        password="StrongP@ssw0rd123",
        confirm_password="StrongP@ssw0rd123",
        first_name="Test",
        last_name="User"
    )
    
    # Create first user
    await user_service.create_user(user_data)
    
    # Try to create second user with same email
    with pytest.raises(ValueError, match="already exists"):
        await user_service.create_user(user_data)


@pytest.mark.integration
async def test_authentication_service(test_session: AsyncSession):
    """Test authentication service."""
    user_service = UserService(test_session)
    auth_service = AuthenticationService(test_session)
    
    # Create user
    user_data = UserCreate(
        email="auth@example.com",
        password="StrongP@ssw0rd123",
        confirm_password="StrongP@ssw0rd123",
        first_name="Auth",
        last_name="User"
    )
    user = await user_service.create_user(user_data)
    
    # Test login
    login_data = LoginRequest(
        email="auth@example.com",
        password="StrongP@ssw0rd123"
    )
    
    result = await auth_service.login(login_data, "127.0.0.1", "test-agent")
    
    assert "access_token" in result
    assert "refresh_token" in result
    assert result["user"].id == user.id
    assert result["mfa_required"] is False


@pytest.mark.integration
async def test_authentication_api(async_client: AsyncClient):
    """Test authentication API endpoints."""
    # Test user registration
    user_data = {
        "email": "api@example.com",
        "password": "StrongP@ssw0rd123",
        "confirm_password": "StrongP@ssw0rd123",
        "first_name": "API",
        "last_name": "User"
    }
    
    response = await async_client.post("/api/v1/auth/register", json=user_data)
    assert response.status_code == 201
    
    user_response = response.json()
    assert user_response["email"] == "api@example.com"
    assert user_response["first_name"] == "API"
    assert "id" in user_response
    
    # Test login
    login_data = {
        "email": "api@example.com",
        "password": "StrongP@ssw0rd123"
    }
    
    response = await async_client.post("/api/v1/auth/login", json=login_data)
    assert response.status_code == 200
    
    login_response = response.json()
    assert "access_token" in login_response
    assert "refresh_token" in login_response
    assert login_response["token_type"] == "Bearer"
    assert login_response["mfa_required"] is False
    
    # Test protected endpoint
    headers = {"Authorization": f"Bearer {login_response['access_token']}"}
    response = await async_client.get("/api/v1/auth/me", headers=headers)
    assert response.status_code == 200
    
    me_response = response.json()
    assert me_response["email"] == "api@example.com"


@pytest.mark.integration
async def test_login_invalid_credentials(async_client: AsyncClient):
    """Test login with invalid credentials."""
    # Try login with non-existent user
    login_data = {
        "email": "nonexistent@example.com",
        "password": "password123"
    }
    
    response = await async_client.post("/api/v1/auth/login", json=login_data)
    assert response.status_code == 401
    assert "Invalid email or password" in response.json()["detail"]


@pytest.mark.integration
async def test_password_change(async_client: AsyncClient):
    """Test password change functionality."""
    # Register user
    user_data = {
        "email": "changepwd@example.com",
        "password": "OldP@ssw0rd123",
        "confirm_password": "OldP@ssw0rd123",
        "first_name": "Change",
        "last_name": "Password"
    }
    
    await async_client.post("/api/v1/auth/register", json=user_data)
    
    # Login
    login_data = {
        "email": "changepwd@example.com",
        "password": "OldP@ssw0rd123"
    }
    
    login_response = await async_client.post("/api/v1/auth/login", json=login_data)
    token = login_response.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # Change password
    change_data = {
        "current_password": "OldP@ssw0rd123",
        "new_password": "NewP@ssw0rd456",
        "confirm_password": "NewP@ssw0rd456"
    }
    
    response = await async_client.post("/api/v1/auth/change-password", json=change_data, headers=headers)
    assert response.status_code == 200
    
    # Test login with new password
    new_login_data = {
        "email": "changepwd@example.com",
        "password": "NewP@ssw0rd456"
    }
    
    response = await async_client.post("/api/v1/auth/login", json=new_login_data)
    assert response.status_code == 200
    
    # Test login with old password should fail
    response = await async_client.post("/api/v1/auth/login", json=login_data)
    assert response.status_code == 401


@pytest.mark.integration
async def test_token_refresh(async_client: AsyncClient):
    """Test token refresh functionality."""
    # Register and login user
    user_data = {
        "email": "refresh@example.com",
        "password": "RefreshP@ss123",
        "confirm_password": "RefreshP@ss123",
        "first_name": "Refresh",
        "last_name": "User"
    }
    
    await async_client.post("/api/v1/auth/register", json=user_data)
    
    login_data = {
        "email": "refresh@example.com",
        "password": "RefreshP@ss123"
    }
    
    login_response = await async_client.post("/api/v1/auth/login", json=login_data)
    refresh_token = login_response.json()["refresh_token"]
    
    # Refresh token
    refresh_data = {"refresh_token": refresh_token}
    response = await async_client.post("/api/v1/auth/refresh", json=refresh_data)
    assert response.status_code == 200
    
    refresh_response = response.json()
    assert "access_token" in refresh_response
    assert "refresh_token" in refresh_response
    
    # New tokens should be different
    assert refresh_response["access_token"] != login_response.json()["access_token"]


@pytest.mark.integration
async def test_logout(async_client: AsyncClient):
    """Test logout functionality."""
    # Register and login user
    user_data = {
        "email": "logout@example.com",
        "password": "LogoutP@ss123",
        "confirm_password": "LogoutP@ss123",
        "first_name": "Logout",
        "last_name": "User"
    }
    
    await async_client.post("/api/v1/auth/register", json=user_data)
    
    login_data = {
        "email": "logout@example.com",
        "password": "LogoutP@ss123"
    }
    
    login_response = await async_client.post("/api/v1/auth/login", json=login_data)
    token = login_response.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # Test protected endpoint works before logout
    response = await async_client.get("/api/v1/auth/me", headers=headers)
    assert response.status_code == 200
    
    # Logout
    response = await async_client.post("/api/v1/auth/logout", headers=headers)
    assert response.status_code == 200
    
    # Protected endpoint should still work (token is still valid until expiry)
    # Session is invalidated but token itself is stateless
    response = await async_client.get("/api/v1/auth/me", headers=headers)
    assert response.status_code == 200


@pytest.mark.unit
def test_authentication_error_handling():
    """Test authentication error handling."""
    from budflow.auth.service import AuthenticationError
    
    error = AuthenticationError("Test error")
    assert str(error) == "Test error"


@pytest.mark.integration
async def test_user_model_properties(test_session: AsyncSession):
    """Test User model properties and methods."""
    user_service = UserService(test_session)
    
    user_data = UserCreate(
        email="model@example.com",
        password="ModelP@ss123",
        confirm_password="ModelP@ss123",
        first_name="Model",
        last_name="User"
    )
    
    user = await user_service.create_user(user_data)
    
    # Test properties
    assert user.full_name == "Model User"
    assert user.is_admin is False  # No admin roles assigned
    
    # Test repr
    user_repr = repr(user)
    assert "User(" in user_repr
    assert str(user.id) in user_repr
    assert "model@example.com" in user_repr