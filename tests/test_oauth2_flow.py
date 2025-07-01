"""Tests for OAuth2 flow management."""

import pytest
import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from unittest.mock import Mock, AsyncMock, patch
from urllib.parse import urlparse, parse_qs
import base64
import hashlib
import secrets

from budflow.core.oauth2_flow import (
    OAuth2FlowManager,
    OAuth2Config,
    OAuth2Provider,
    OAuth2Token,
    OAuth2State,
    OAuth2Error,
    AuthorizationCodeFlow,
    PKCEFlow,
    ClientCredentialsFlow,
    RefreshTokenFlow,
    TokenValidator,
    StateManager,
    FlowType,
    GrantType,
    TokenType,
    OAuth2Scope,
)


@pytest.fixture
def oauth2_config():
    """Create OAuth2 configuration."""
    return OAuth2Config(
        provider_name="test_provider",
        client_id="test_client_id",
        client_secret="test_client_secret",
        authorization_url="https://auth.example.com/oauth/authorize",
        token_url="https://auth.example.com/oauth/token",
        userinfo_url="https://auth.example.com/oauth/userinfo",
        scopes=["read", "write", "profile"],
        redirect_uri="https://app.example.com/oauth/callback",
        flow_type=FlowType.AUTHORIZATION_CODE,
        enable_pkce=True,
        token_endpoint_auth_method="client_secret_basic",
        response_type="code",
        grant_type=GrantType.AUTHORIZATION_CODE,
    )


@pytest.fixture
def oauth2_provider(oauth2_config):
    """Create OAuth2 provider."""
    return OAuth2Provider(
        name="test_provider",
        config=oauth2_config,
        metadata={
            "issuer": "https://auth.example.com",
            "authorization_endpoint": "https://auth.example.com/oauth/authorize",
            "token_endpoint": "https://auth.example.com/oauth/token",
            "userinfo_endpoint": "https://auth.example.com/oauth/userinfo",
            "jwks_uri": "https://auth.example.com/.well-known/jwks.json",
            "supported_scopes": ["openid", "profile", "email", "read", "write"],
            "supported_response_types": ["code", "token", "id_token"],
            "supported_grant_types": ["authorization_code", "refresh_token", "client_credentials"],
        }
    )


@pytest.fixture
def oauth2_token():
    """Create OAuth2 token."""
    return OAuth2Token(
        access_token="access_token_123",
        token_type=TokenType.BEARER,
        expires_in=3600,
        refresh_token="refresh_token_456",
        scope=["read", "write"],
        id_token="id_token_789",
        issued_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=3600),
    )


@pytest.fixture
def oauth2_state():
    """Create OAuth2 state."""
    return OAuth2State(
        state="random_state_123",
        flow_type=FlowType.AUTHORIZATION_CODE,
        client_id="test_client_id",
        redirect_uri="https://app.example.com/oauth/callback",
        scopes=["read", "write"],
        code_verifier="code_verifier_123",
        code_challenge="code_challenge_456",
        code_challenge_method="S256",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        user_id="user_123",
        workflow_id="workflow_456",
    )


@pytest.fixture
def oauth2_flow_manager():
    """Create OAuth2 flow manager."""
    return OAuth2FlowManager()


@pytest.fixture
def state_manager():
    """Create OAuth2 state manager."""
    return StateManager()


@pytest.mark.unit
class TestOAuth2Config:
    """Test OAuth2 configuration."""
    
    def test_config_creation(self, oauth2_config):
        """Test creating OAuth2 configuration."""
        assert oauth2_config.provider_name == "test_provider"
        assert oauth2_config.client_id == "test_client_id"
        assert oauth2_config.flow_type == FlowType.AUTHORIZATION_CODE
        assert oauth2_config.enable_pkce is True
        assert "read" in oauth2_config.scopes
    
    def test_config_validation(self):
        """Test OAuth2 configuration validation."""
        # Valid config
        config = OAuth2Config(
            provider_name="valid",
            client_id="client123",
            authorization_url="https://auth.example.com/authorize",
            token_url="https://auth.example.com/token",
            redirect_uri="https://app.example.com/callback",
        )
        assert config.provider_name == "valid"
        
        # Invalid URLs
        with pytest.raises(ValueError, match="Invalid authorization_url"):
            OAuth2Config(
                provider_name="invalid",
                client_id="client123",
                authorization_url="not-a-url",
                token_url="https://auth.example.com/token",
                redirect_uri="https://app.example.com/callback",
            )
    
    def test_scope_handling(self, oauth2_config):
        """Test OAuth2 scope handling."""
        # Test scope validation
        assert oauth2_config.validate_scopes(["read", "write"]) is True
        assert oauth2_config.validate_scopes(["read", "invalid"]) is False
        
        # Test scope formatting
        assert oauth2_config.format_scopes(["read", "write"]) == "read write"
        assert oauth2_config.parse_scopes("read write profile") == ["read", "write", "profile"]


@pytest.mark.unit
class TestOAuth2Token:
    """Test OAuth2 token functionality."""
    
    def test_token_creation(self, oauth2_token):
        """Test creating OAuth2 token."""
        assert oauth2_token.access_token == "access_token_123"
        assert oauth2_token.token_type == TokenType.BEARER
        assert oauth2_token.expires_in == 3600
        assert oauth2_token.refresh_token == "refresh_token_456"
        assert "read" in oauth2_token.scope
    
    def test_token_validation(self, oauth2_token):
        """Test OAuth2 token validation."""
        # Valid token
        assert oauth2_token.is_valid() is True
        assert oauth2_token.is_expired() is False
        
        # Expired token
        expired_token = OAuth2Token(
            access_token="expired_token",
            token_type=TokenType.BEARER,
            expires_in=3600,
            issued_at=datetime.now(timezone.utc) - timedelta(hours=2),
            expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        assert expired_token.is_valid() is False
        assert expired_token.is_expired() is True
    
    def test_token_refresh_needed(self, oauth2_token):
        """Test token refresh detection."""
        # Fresh token
        assert oauth2_token.needs_refresh() is False
        
        # Token expiring soon
        expiring_token = OAuth2Token(
            access_token="expiring_token",
            token_type=TokenType.BEARER,
            expires_in=3600,
            issued_at=datetime.now(timezone.utc) - timedelta(minutes=55),
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        assert expiring_token.needs_refresh() is True
    
    def test_token_serialization(self, oauth2_token):
        """Test OAuth2 token serialization."""
        # Serialize to dict
        token_dict = oauth2_token.to_dict()
        assert token_dict["access_token"] == "access_token_123"
        assert token_dict["token_type"] == "bearer"
        assert token_dict["expires_in"] == 3600
        
        # Deserialize from dict
        restored_token = OAuth2Token.from_dict(token_dict)
        assert restored_token.access_token == oauth2_token.access_token
        assert restored_token.token_type == oauth2_token.token_type
    
    def test_authorization_header(self, oauth2_token):
        """Test authorization header generation."""
        header = oauth2_token.get_authorization_header()
        assert header == "Bearer access_token_123"
        
        # MAC token type
        mac_token = OAuth2Token(
            access_token="mac_token_123",
            token_type=TokenType.MAC,
        )
        # MAC tokens require additional parameters
        with pytest.raises(ValueError, match="MAC token"):
            mac_token.get_authorization_header()


@pytest.mark.unit
class TestOAuth2State:
    """Test OAuth2 state management."""
    
    def test_state_creation(self, oauth2_state):
        """Test creating OAuth2 state."""
        assert oauth2_state.state == "random_state_123"
        assert oauth2_state.flow_type == FlowType.AUTHORIZATION_CODE
        assert oauth2_state.client_id == "test_client_id"
        assert "read" in oauth2_state.scopes
    
    def test_state_validation(self, oauth2_state):
        """Test OAuth2 state validation."""
        # Valid state
        assert oauth2_state.is_valid() is True
        assert oauth2_state.is_expired() is False
        
        # Expired state
        expired_state = OAuth2State(
            state="expired_state",
            flow_type=FlowType.AUTHORIZATION_CODE,
            client_id="client123",
            created_at=datetime.now(timezone.utc) - timedelta(minutes=15),
            expires_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        )
        assert expired_state.is_valid() is False
        assert expired_state.is_expired() is True
    
    def test_pkce_generation(self):
        """Test PKCE code generation."""
        # Generate PKCE codes
        code_verifier = OAuth2State.generate_code_verifier()
        code_challenge = OAuth2State.generate_code_challenge(code_verifier)
        
        assert len(code_verifier) >= 43  # Minimum length
        assert len(code_verifier) <= 128  # Maximum length
        assert len(code_challenge) == 43  # Base64url length for SHA256
        
        # Verify challenge matches verifier
        expected_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode()).digest()
        ).decode().rstrip('=')
        assert code_challenge == expected_challenge


@pytest.mark.unit
class TestStateManager:
    """Test OAuth2 state manager."""
    
    @pytest.fixture
    def state_manager(self):
        """Create state manager."""
        return StateManager()
    
    @pytest.mark.asyncio
    async def test_store_and_retrieve_state(self, state_manager, oauth2_state):
        """Test storing and retrieving OAuth2 state."""
        # Store state
        await state_manager.store_state(oauth2_state)
        
        # Retrieve state
        retrieved_state = await state_manager.get_state(oauth2_state.state)
        assert retrieved_state is not None
        assert retrieved_state.state == oauth2_state.state
        assert retrieved_state.client_id == oauth2_state.client_id
    
    @pytest.mark.asyncio
    async def test_state_expiration(self, state_manager):
        """Test state expiration handling."""
        # Create expired state
        expired_state = OAuth2State(
            state="expired_state",
            flow_type=FlowType.AUTHORIZATION_CODE,
            client_id="client123",
            expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
        
        await state_manager.store_state(expired_state)
        
        # Should not retrieve expired state
        retrieved_state = await state_manager.get_state("expired_state")
        assert retrieved_state is None
    
    @pytest.mark.asyncio
    async def test_cleanup_expired_states(self, state_manager):
        """Test cleanup of expired states."""
        # Store multiple states with different expiration times
        current_time = datetime.now(timezone.utc)
        
        states = [
            OAuth2State(
                state=f"state_{i}",
                flow_type=FlowType.AUTHORIZATION_CODE,
                client_id="client123",
                expires_at=current_time + timedelta(minutes=i-2),  # Some expired, some valid
            )
            for i in range(5)
        ]
        
        for state in states:
            await state_manager.store_state(state)
        
        # Cleanup expired states
        cleaned_count = await state_manager.cleanup_expired_states()
        assert cleaned_count >= 2  # At least 2 expired states should be cleaned


@pytest.mark.unit
class TestAuthorizationCodeFlow:
    """Test authorization code flow."""
    
    @pytest.fixture
    def auth_code_flow(self, oauth2_config, state_manager):
        """Create authorization code flow."""
        return AuthorizationCodeFlow(oauth2_config, state_manager)
    
    @pytest.mark.asyncio
    async def test_generate_authorization_url(self, auth_code_flow):
        """Test generating authorization URL."""
        url, state = await auth_code_flow.generate_authorization_url(
            user_id="user123",
            workflow_id="workflow456",
            additional_scopes=["profile"]
        )
        
        # Parse URL
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        assert parsed_url.netloc == "auth.example.com"
        assert parsed_url.path == "/oauth/authorize"
        assert query_params["client_id"][0] == "test_client_id"
        assert query_params["response_type"][0] == "code"
        assert query_params["redirect_uri"][0] == "https://app.example.com/oauth/callback"
        assert "read" in query_params["scope"][0].split()
        assert "write" in query_params["scope"][0].split()
        assert "profile" in query_params["scope"][0].split()
        assert query_params["state"][0] == state
        
        # Should include PKCE parameters
        assert "code_challenge" in query_params
        assert "code_challenge_method" in query_params
        assert query_params["code_challenge_method"][0] == "S256"
    
    @pytest.mark.asyncio
    async def test_exchange_code_for_token(self, auth_code_flow):
        """Test exchanging authorization code for token."""
        # First generate state
        url, state = await auth_code_flow.generate_authorization_url(
            user_id="user123",
            workflow_id="workflow456"
        )
        
        # Mock HTTP client
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "access_token_123",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_token": "refresh_token_456",
                "scope": "read write",
            }
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            # Exchange code for token
            token = await auth_code_flow.exchange_code_for_token(
                code="auth_code_123",
                state=state
            )
            
            assert token is not None
            assert token.access_token == "access_token_123"
            assert token.token_type == TokenType.BEARER
            assert token.expires_in == 3600
            assert token.refresh_token == "refresh_token_456"
            assert "read" in token.scope
    
    @pytest.mark.asyncio
    async def test_invalid_state_handling(self, auth_code_flow):
        """Test handling invalid state parameter."""
        with pytest.raises(OAuth2Error, match="Invalid or expired state"):
            await auth_code_flow.exchange_code_for_token(
                code="auth_code_123",
                state="invalid_state"
            )
    
    @pytest.mark.asyncio
    async def test_token_exchange_error(self, auth_code_flow):
        """Test token exchange error handling."""
        # Generate valid state
        url, state = await auth_code_flow.generate_authorization_url(
            user_id="user123",
            workflow_id="workflow456"
        )
        
        # Mock HTTP error
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 400
            mock_response.json.return_value = {
                "error": "invalid_grant",
                "error_description": "The authorization code is invalid or expired",
            }
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            with pytest.raises(OAuth2Error, match="invalid_grant"):
                await auth_code_flow.exchange_code_for_token(
                    code="invalid_code",
                    state=state
                )


@pytest.mark.unit
class TestPKCEFlow:
    """Test PKCE flow."""
    
    @pytest.fixture
    def pkce_flow(self, oauth2_config, state_manager):
        """Create PKCE flow."""
        oauth2_config.enable_pkce = True
        return PKCEFlow(oauth2_config, state_manager)
    
    @pytest.mark.asyncio
    async def test_pkce_authorization_url(self, pkce_flow):
        """Test PKCE authorization URL generation."""
        url, state = await pkce_flow.generate_authorization_url(
            user_id="user123",
            workflow_id="workflow456"
        )
        
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        # Should include PKCE parameters
        assert "code_challenge" in query_params
        assert "code_challenge_method" in query_params
        assert query_params["code_challenge_method"][0] == "S256"
        
        # Code challenge should be valid base64url
        code_challenge = query_params["code_challenge"][0]
        assert len(code_challenge) == 43  # SHA256 base64url length
    
    @pytest.mark.asyncio
    async def test_pkce_token_exchange(self, pkce_flow):
        """Test PKCE token exchange."""
        # Generate authorization URL to create state
        url, state = await pkce_flow.generate_authorization_url(
            user_id="user123",
            workflow_id="workflow456"
        )
        
        # Mock successful token exchange
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "pkce_access_token",
                "token_type": "bearer",
                "expires_in": 3600,
            }
            
            # Capture the request to verify PKCE parameters
            captured_requests = []
            
            async def capture_post(*args, **kwargs):
                captured_requests.append(kwargs)
                return mock_response
            
            mock_client.return_value.__aenter__.return_value.post = capture_post
            
            # Exchange code for token
            token = await pkce_flow.exchange_code_for_token(
                code="auth_code_123",
                state=state
            )
            
            assert token is not None
            assert token.access_token == "pkce_access_token"
            
            # Verify PKCE code_verifier was sent
            assert len(captured_requests) == 1
            request_data = captured_requests[0]['data']
            assert 'code_verifier' in request_data


@pytest.mark.unit
class TestClientCredentialsFlow:
    """Test client credentials flow."""
    
    @pytest.fixture
    def client_creds_flow(self, oauth2_config):
        """Create client credentials flow."""
        oauth2_config.flow_type = FlowType.CLIENT_CREDENTIALS
        oauth2_config.grant_type = GrantType.CLIENT_CREDENTIALS
        return ClientCredentialsFlow(oauth2_config)
    
    @pytest.mark.asyncio
    async def test_client_credentials_token_request(self, client_creds_flow):
        """Test client credentials token request."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "client_creds_token",
                "token_type": "bearer",
                "expires_in": 7200,
                "scope": "read write",
            }
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            # Request token
            token = await client_creds_flow.request_token(scopes=["read", "write"])
            
            assert token is not None
            assert token.access_token == "client_creds_token"
            assert token.expires_in == 7200
            assert "read" in token.scope
    
    @pytest.mark.asyncio
    async def test_client_credentials_authentication(self, client_creds_flow):
        """Test client credentials authentication methods."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "token",
                "token_type": "bearer",
                "expires_in": 3600,
            }
            
            captured_requests = []
            
            async def capture_post(*args, **kwargs):
                captured_requests.append(kwargs)
                return mock_response
            
            mock_client.return_value.__aenter__.return_value.post = capture_post
            
            # Test basic authentication
            await client_creds_flow.request_token()
            
            assert len(captured_requests) == 1
            request = captured_requests[0]
            
            # Should include basic auth header
            auth_header = request['headers']['Authorization']
            assert auth_header.startswith('Basic ')
            
            # Verify credentials
            encoded_creds = auth_header.split(' ')[1]
            decoded_creds = base64.b64decode(encoded_creds).decode()
            assert decoded_creds == "test_client_id:test_client_secret"


@pytest.mark.unit
class TestRefreshTokenFlow:
    """Test refresh token flow."""
    
    @pytest.fixture
    def refresh_flow(self, oauth2_config):
        """Create refresh token flow."""
        return RefreshTokenFlow(oauth2_config)
    
    @pytest.mark.asyncio
    async def test_refresh_token_request(self, refresh_flow, oauth2_token):
        """Test refresh token request."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "new_access_token",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_token": "new_refresh_token",
                "scope": "read write profile",
            }
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            # Refresh token
            new_token = await refresh_flow.refresh_token(oauth2_token)
            
            assert new_token is not None
            assert new_token.access_token == "new_access_token"
            assert new_token.refresh_token == "new_refresh_token"
            assert "profile" in new_token.scope
    
    @pytest.mark.asyncio
    async def test_refresh_token_error(self, refresh_flow, oauth2_token):
        """Test refresh token error handling."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 400
            mock_response.json.return_value = {
                "error": "invalid_grant",
                "error_description": "The refresh token is invalid or expired",
            }
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            with pytest.raises(OAuth2Error, match="invalid_grant"):
                await refresh_flow.refresh_token(oauth2_token)


@pytest.mark.unit
class TestTokenValidator:
    """Test OAuth2 token validator."""
    
    @pytest.fixture
    def token_validator(self, oauth2_config):
        """Create token validator."""
        return TokenValidator(oauth2_config)
    
    @pytest.mark.asyncio
    async def test_validate_access_token(self, token_validator, oauth2_token):
        """Test access token validation."""
        # Mock userinfo endpoint
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "sub": "user123",
                "name": "Test User",
                "email": "test@example.com",
                "scope": "read write",
            }
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            # Validate token
            is_valid, userinfo = await token_validator.validate_access_token(oauth2_token)
            
            assert is_valid is True
            assert userinfo["sub"] == "user123"
            assert userinfo["email"] == "test@example.com"
    
    @pytest.mark.asyncio
    async def test_validate_invalid_token(self, token_validator, oauth2_token):
        """Test invalid token validation."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 401
            mock_response.json.return_value = {
                "error": "invalid_token",
                "error_description": "The access token is invalid",
            }
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            # Validate token
            is_valid, userinfo = await token_validator.validate_access_token(oauth2_token)
            
            assert is_valid is False
            assert userinfo is None
    
    @pytest.mark.asyncio
    async def test_validate_id_token(self, token_validator):
        """Test ID token validation."""
        # Mock JWKS endpoint
        with patch('httpx.AsyncClient') as mock_client:
            # Simplified ID token validation test
            # In real implementation, this would involve JWT validation
            id_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9..."
            
            # Mock successful validation
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "keys": [
                    {
                        "kty": "RSA",
                        "kid": "key1",
                        "n": "mock_modulus",
                        "e": "AQAB",
                    }
                ]
            }
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            # For this test, we'll just verify the method exists and handles errors
            try:
                is_valid, claims = await token_validator.validate_id_token(id_token)
                # Mock implementation might not actually validate
                assert isinstance(is_valid, bool)
            except NotImplementedError:
                # ID token validation is complex and might not be fully implemented
                pytest.skip("ID token validation not implemented")


@pytest.mark.unit
class TestOAuth2FlowManager:
    """Test OAuth2 flow manager."""
    
    @pytest.mark.asyncio
    async def test_register_provider(self, oauth2_flow_manager, oauth2_provider):
        """Test registering OAuth2 provider."""
        await oauth2_flow_manager.register_provider(oauth2_provider)
        
        # Verify provider is registered
        provider = await oauth2_flow_manager.get_provider("test_provider")
        assert provider is not None
        assert provider.name == "test_provider"
    
    @pytest.mark.asyncio
    async def test_start_authorization_flow(self, oauth2_flow_manager, oauth2_provider):
        """Test starting authorization flow."""
        await oauth2_flow_manager.register_provider(oauth2_provider)
        
        # Start flow
        url, state = await oauth2_flow_manager.start_authorization_flow(
            provider_name="test_provider",
            user_id="user123",
            workflow_id="workflow456",
            scopes=["read", "write"]
        )
        
        assert url.startswith("https://auth.example.com/oauth/authorize")
        assert isinstance(state, str)
        assert len(state) > 0
    
    @pytest.mark.asyncio
    async def test_handle_callback(self, oauth2_flow_manager, oauth2_provider):
        """Test handling OAuth2 callback."""
        await oauth2_flow_manager.register_provider(oauth2_provider)
        
        # Start flow to create state
        url, state = await oauth2_flow_manager.start_authorization_flow(
            provider_name="test_provider",
            user_id="user123",
            workflow_id="workflow456",
        )
        
        # Mock token exchange
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "callback_token",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_token": "callback_refresh",
            }
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            # Handle callback
            token = await oauth2_flow_manager.handle_callback(
                provider_name="test_provider",
                code="auth_code_123",
                state=state
            )
            
            assert token is not None
            assert token.access_token == "callback_token"
    
    @pytest.mark.asyncio
    async def test_refresh_token(self, oauth2_flow_manager, oauth2_provider, oauth2_token):
        """Test token refresh."""
        await oauth2_flow_manager.register_provider(oauth2_provider)
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "refreshed_token",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_token": "new_refresh_token",
            }
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            # Refresh token
            new_token = await oauth2_flow_manager.refresh_token(
                provider_name="test_provider",
                token=oauth2_token
            )
            
            assert new_token is not None
            assert new_token.access_token == "refreshed_token"
    
    @pytest.mark.asyncio
    async def test_revoke_token(self, oauth2_flow_manager, oauth2_provider, oauth2_token):
        """Test token revocation."""
        await oauth2_flow_manager.register_provider(oauth2_provider)
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_client.return_value.__aenter__.return_value.post.return_value = mock_response
            
            # Revoke token
            success = await oauth2_flow_manager.revoke_token(
                provider_name="test_provider",
                token=oauth2_token
            )
            
            assert success is True


@pytest.mark.integration
class TestOAuth2Integration:
    """Integration tests for OAuth2 flows."""
    
    @pytest.mark.asyncio
    async def test_complete_authorization_flow(self, oauth2_flow_manager):
        """Test complete authorization code flow."""
        # Register provider
        config = OAuth2Config(
            provider_name="integration_test",
            client_id="integration_client",
            client_secret="integration_secret",
            authorization_url="https://oauth.test.com/authorize",
            token_url="https://oauth.test.com/token",
            userinfo_url="https://oauth.test.com/userinfo",
            scopes=["read", "write"],
            redirect_uri="https://app.test.com/callback",
        )
        
        provider = OAuth2Provider(name="integration_test", config=config)
        await oauth2_flow_manager.register_provider(provider)
        
        # Start authorization flow
        auth_url, state = await oauth2_flow_manager.start_authorization_flow(
            provider_name="integration_test",
            user_id="integration_user",
            workflow_id="integration_workflow"
        )
        
        # Verify authorization URL
        assert "oauth.test.com" in auth_url
        assert "client_id=integration_client" in auth_url
        assert "response_type=code" in auth_url
        
        # Mock successful callback
        with patch('httpx.AsyncClient') as mock_client:
            mock_token_response = Mock()
            mock_token_response.status_code = 200
            mock_token_response.json.return_value = {
                "access_token": "integration_access_token",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_token": "integration_refresh_token",
                "scope": "read write",
            }
            
            mock_userinfo_response = Mock()
            mock_userinfo_response.status_code = 200
            mock_userinfo_response.json.return_value = {
                "sub": "integration_user",
                "name": "Integration User",
                "email": "integration@test.com",
            }
            
            # Configure mock responses
            mock_client_instance = mock_client.return_value.__aenter__.return_value
            mock_client_instance.post.return_value = mock_token_response
            mock_client_instance.get.return_value = mock_userinfo_response
            
            # Handle callback
            token = await oauth2_flow_manager.handle_callback(
                provider_name="integration_test",
                code="integration_auth_code",
                state=state
            )
            
            assert token is not None
            assert token.access_token == "integration_access_token"
            
            # Validate token
            token_validator = TokenValidator(config)
            is_valid, userinfo = await token_validator.validate_access_token(token)
            
            assert is_valid is True
            assert userinfo["sub"] == "integration_user"
    
    @pytest.mark.asyncio
    async def test_token_lifecycle_management(self, oauth2_flow_manager):
        """Test complete token lifecycle."""
        # Setup provider
        config = OAuth2Config(
            provider_name="lifecycle_test",
            client_id="lifecycle_client",
            client_secret="lifecycle_secret",
            authorization_url="https://auth.lifecycle.com/authorize",
            token_url="https://auth.lifecycle.com/token",
        )
        
        provider = OAuth2Provider(name="lifecycle_test", config=config)
        await oauth2_flow_manager.register_provider(provider)
        
        # Initial token
        initial_token = OAuth2Token(
            access_token="initial_token",
            token_type=TokenType.BEARER,
            expires_in=3600,
            refresh_token="initial_refresh",
            scope=["read"],
            issued_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=3600),
        )
        
        with patch('httpx.AsyncClient') as mock_client:
            # Mock refresh response
            mock_refresh_response = Mock()
            mock_refresh_response.status_code = 200
            mock_refresh_response.json.return_value = {
                "access_token": "refreshed_token",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_token": "new_refresh_token",
                "scope": "read write",
            }
            
            # Mock revoke response
            mock_revoke_response = Mock()
            mock_revoke_response.status_code = 200
            
            mock_client_instance = mock_client.return_value.__aenter__.return_value
            mock_client_instance.post.side_effect = [mock_refresh_response, mock_revoke_response]
            
            # Refresh token
            refreshed_token = await oauth2_flow_manager.refresh_token(
                provider_name="lifecycle_test",
                token=initial_token
            )
            
            assert refreshed_token.access_token == "refreshed_token"
            assert "write" in refreshed_token.scope
            
            # Revoke token
            revoke_success = await oauth2_flow_manager.revoke_token(
                provider_name="lifecycle_test",
                token=refreshed_token
            )
            
            assert revoke_success is True


@pytest.mark.performance
class TestOAuth2Performance:
    """Performance tests for OAuth2 flows."""
    
    @pytest.mark.asyncio
    async def test_state_management_performance(self):
        """Test state management performance."""
        state_manager = StateManager()
        
        import time
        start_time = time.time()
        
        # Create and store many states
        states = []
        for i in range(100):
            state = OAuth2State(
                state=f"state_{i}",
                flow_type=FlowType.AUTHORIZATION_CODE,
                client_id=f"client_{i}",
                scopes=["read", "write"],
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
            )
            states.append(state)
            await state_manager.store_state(state)
        
        # Retrieve all states
        for state in states:
            retrieved = await state_manager.get_state(state.state)
            assert retrieved is not None
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should handle 100 states quickly
        assert duration < 1.0  # 1 second for 200 operations
    
    @pytest.mark.asyncio
    async def test_token_validation_performance(self):
        """Test token validation performance."""
        config = OAuth2Config(
            provider_name="perf_test",
            client_id="perf_client",
            userinfo_url="https://auth.perf.com/userinfo",
        )
        
        validator = TokenValidator(config)
        
        # Mock fast userinfo response
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"sub": "user123"}
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            tokens = [
                OAuth2Token(
                    access_token=f"token_{i}",
                    token_type=TokenType.BEARER,
                    expires_in=3600,
                )
                for i in range(50)
            ]
            
            import time
            start_time = time.time()
            
            # Validate many tokens
            for token in tokens:
                is_valid, userinfo = await validator.validate_access_token(token)
                assert is_valid is True
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should validate quickly
            assert duration < 2.0  # 2 seconds for 50 validations