"""OAuth2 flow management for BudFlow."""

import asyncio
import base64
import hashlib
import json
import secrets
import time
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, Any, Optional, List, Union
from urllib.parse import urlencode, urlparse
import re

import httpx
import structlog
from pydantic import BaseModel, Field, field_validator

logger = structlog.get_logger()


class FlowType(str, Enum):
    """OAuth2 flow types."""
    AUTHORIZATION_CODE = "authorization_code"
    CLIENT_CREDENTIALS = "client_credentials"
    DEVICE_CODE = "device_code"
    REFRESH_TOKEN = "refresh_token"


class GrantType(str, Enum):
    """OAuth2 grant types."""
    AUTHORIZATION_CODE = "authorization_code"
    CLIENT_CREDENTIALS = "client_credentials"
    REFRESH_TOKEN = "refresh_token"
    DEVICE_CODE = "urn:ietf:params:oauth:grant-type:device_code"


class TokenType(str, Enum):
    """OAuth2 token types."""
    BEARER = "bearer"
    MAC = "mac"


class OAuth2Scope(str, Enum):
    """Common OAuth2 scopes."""
    OPENID = "openid"
    PROFILE = "profile"
    EMAIL = "email"
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


class OAuth2Error(Exception):
    """OAuth2 flow error."""
    pass


class OAuth2Config(BaseModel):
    """OAuth2 provider configuration."""
    provider_name: str
    client_id: str
    client_secret: Optional[str] = None
    authorization_url: str
    token_url: str
    userinfo_url: Optional[str] = None
    revocation_url: Optional[str] = None
    scopes: List[str] = Field(default_factory=list)
    redirect_uri: str
    flow_type: FlowType = FlowType.AUTHORIZATION_CODE
    enable_pkce: bool = True
    token_endpoint_auth_method: str = "client_secret_basic"
    response_type: str = "code"
    grant_type: GrantType = GrantType.AUTHORIZATION_CODE
    
    @field_validator('authorization_url', 'token_url')
    @classmethod
    def validate_urls(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid authorization_url: {v}")
        return v
    
    def validate_scopes(self, scopes: List[str]) -> bool:
        """Validate if scopes are supported."""
        return all(scope in self.scopes for scope in scopes)
    
    def format_scopes(self, scopes: List[str]) -> str:
        """Format scopes for OAuth2 request."""
        return " ".join(scopes)
    
    def parse_scopes(self, scope_string: str) -> List[str]:
        """Parse scope string into list."""
        return scope_string.split() if scope_string else []


class OAuth2Token(BaseModel):
    """OAuth2 token representation."""
    access_token: str
    token_type: TokenType = TokenType.BEARER
    expires_in: Optional[int] = None
    refresh_token: Optional[str] = None
    scope: List[str] = Field(default_factory=list)
    id_token: Optional[str] = None
    issued_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None
    
    def __init__(self, **data):
        super().__init__(**data)
        # Calculate expires_at if not provided
        if self.expires_in and not self.expires_at:
            self.expires_at = self.issued_at + timedelta(seconds=self.expires_in)
    
    def is_valid(self) -> bool:
        """Check if token is valid."""
        return not self.is_expired()
    
    def is_expired(self) -> bool:
        """Check if token is expired."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) >= self.expires_at
    
    def needs_refresh(self, buffer_seconds: int = 300) -> bool:
        """Check if token needs refresh (expires within buffer)."""
        if not self.expires_at:
            return False
        buffer_time = datetime.now(timezone.utc) + timedelta(seconds=buffer_seconds)
        return buffer_time >= self.expires_at
    
    def get_authorization_header(self) -> str:
        """Get authorization header value."""
        if self.token_type == TokenType.BEARER:
            return f"Bearer {self.access_token}"
        elif self.token_type == TokenType.MAC:
            raise ValueError("MAC token authorization header requires additional parameters")
        else:
            return f"{self.token_type.value.title()} {self.access_token}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "access_token": self.access_token,
            "token_type": self.token_type.value,
            "expires_in": self.expires_in,
            "refresh_token": self.refresh_token,
            "scope": " ".join(self.scope) if self.scope else None,
            "id_token": self.id_token,
            "issued_at": self.issued_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OAuth2Token':
        """Create from dictionary."""
        # Parse datetime fields
        if 'issued_at' in data and isinstance(data['issued_at'], str):
            data['issued_at'] = datetime.fromisoformat(data['issued_at'])
        if 'expires_at' in data and isinstance(data['expires_at'], str):
            data['expires_at'] = datetime.fromisoformat(data['expires_at'])
        
        # Parse scope
        if 'scope' in data and isinstance(data['scope'], str):
            data['scope'] = data['scope'].split() if data['scope'] else []
        
        # Parse token_type
        if 'token_type' in data and isinstance(data['token_type'], str):
            data['token_type'] = TokenType(data['token_type'].lower())
        
        return cls(**data)


class OAuth2State(BaseModel):
    """OAuth2 state for security and flow tracking."""
    state: str
    flow_type: FlowType
    client_id: str
    redirect_uri: Optional[str] = None
    scopes: List[str] = Field(default_factory=list)
    code_verifier: Optional[str] = None
    code_challenge: Optional[str] = None
    code_challenge_method: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc) + timedelta(minutes=10))
    user_id: Optional[str] = None
    workflow_id: Optional[str] = None
    additional_data: Dict[str, Any] = Field(default_factory=dict)
    
    def is_valid(self) -> bool:
        """Check if state is valid."""
        return not self.is_expired()
    
    def is_expired(self) -> bool:
        """Check if state is expired."""
        return datetime.now(timezone.utc) >= self.expires_at
    
    @staticmethod
    def generate_state() -> str:
        """Generate cryptographically secure state."""
        return secrets.token_urlsafe(32)
    
    @staticmethod
    def generate_code_verifier() -> str:
        """Generate PKCE code verifier."""
        return secrets.token_urlsafe(96)  # 128 chars base64url
    
    @staticmethod
    def generate_code_challenge(code_verifier: str, method: str = "S256") -> str:
        """Generate PKCE code challenge."""
        if method == "S256":
            digest = hashlib.sha256(code_verifier.encode()).digest()
            return base64.urlsafe_b64encode(digest).decode().rstrip('=')
        elif method == "plain":
            return code_verifier
        else:
            raise ValueError(f"Unsupported code challenge method: {method}")


class OAuth2Provider(BaseModel):
    """OAuth2 provider definition."""
    name: str
    config: OAuth2Config
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def supports_pkce(self) -> bool:
        """Check if provider supports PKCE."""
        return self.config.enable_pkce or "code_challenge_methods_supported" in self.metadata


class StateManager:
    """Manages OAuth2 state storage."""
    
    def __init__(self):
        self._states: Dict[str, OAuth2State] = {}
    
    async def store_state(self, state: OAuth2State):
        """Store OAuth2 state."""
        self._states[state.state] = state
    
    async def get_state(self, state_id: str) -> Optional[OAuth2State]:
        """Get OAuth2 state."""
        state = self._states.get(state_id)
        if state and not state.is_expired():
            return state
        elif state:
            # Remove expired state
            del self._states[state_id]
        return None
    
    async def remove_state(self, state_id: str):
        """Remove OAuth2 state."""
        self._states.pop(state_id, None)
    
    async def cleanup_expired_states(self) -> int:
        """Clean up expired states."""
        expired_states = [
            state_id for state_id, state in self._states.items()
            if state.is_expired()
        ]
        
        for state_id in expired_states:
            del self._states[state_id]
        
        return len(expired_states)


class TokenValidator:
    """Validates OAuth2 tokens."""
    
    def __init__(self, config: OAuth2Config):
        self.config = config
    
    async def validate_access_token(self, token: OAuth2Token) -> tuple[bool, Optional[Dict[str, Any]]]:
        """Validate access token by calling userinfo endpoint."""
        if not self.config.userinfo_url:
            return True, None
        
        try:
            async with httpx.AsyncClient() as client:
                headers = {"Authorization": token.get_authorization_header()}
                response = await client.get(self.config.userinfo_url, headers=headers)
                
                if response.status_code == 200:
                    return True, response.json()
                else:
                    return False, None
                    
        except Exception as e:
            logger.error(f"Token validation failed: {str(e)}")
            return False, None
    
    async def validate_id_token(self, id_token: str) -> tuple[bool, Optional[Dict[str, Any]]]:
        """Validate ID token (simplified implementation)."""
        # In a real implementation, this would involve:
        # 1. Fetching JWKS from provider
        # 2. Validating JWT signature
        # 3. Validating claims (iss, aud, exp, etc.)
        
        try:
            # For now, just decode without verification (for testing)
            # This is NOT secure and should be replaced with proper JWT validation
            parts = id_token.split('.')
            if len(parts) != 3:
                return False, None
            
            # Decode payload (add padding if needed)
            payload = parts[1]
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding
            
            decoded = base64.urlsafe_b64decode(payload)
            claims = json.loads(decoded)
            
            # Basic validation
            now = time.time()
            if claims.get('exp', 0) < now:
                return False, None
            
            return True, claims
            
        except Exception as e:
            logger.error(f"ID token validation failed: {str(e)}")
            return False, None


class AuthorizationCodeFlow:
    """Authorization Code flow implementation."""
    
    def __init__(self, config: OAuth2Config, state_manager: StateManager):
        self.config = config
        self.state_manager = state_manager
    
    async def generate_authorization_url(
        self,
        user_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
        additional_scopes: Optional[List[str]] = None,
    ) -> tuple[str, str]:
        """Generate authorization URL and state."""
        
        # Generate state
        state_id = OAuth2State.generate_state()
        
        # Prepare scopes
        scopes = self.config.scopes.copy()
        if additional_scopes:
            scopes.extend(additional_scopes)
        
        # Generate PKCE if enabled
        code_verifier = None
        code_challenge = None
        code_challenge_method = None
        
        if self.config.enable_pkce:
            code_verifier = OAuth2State.generate_code_verifier()
            code_challenge = OAuth2State.generate_code_challenge(code_verifier)
            code_challenge_method = "S256"
        
        # Create state object
        oauth_state = OAuth2State(
            state=state_id,
            flow_type=self.config.flow_type,
            client_id=self.config.client_id,
            redirect_uri=self.config.redirect_uri,
            scopes=scopes,
            code_verifier=code_verifier,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            user_id=user_id,
            workflow_id=workflow_id,
        )
        
        # Store state
        await self.state_manager.store_state(oauth_state)
        
        # Build authorization URL
        params = {
            "response_type": self.config.response_type,
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "scope": self.config.format_scopes(scopes),
            "state": state_id,
        }
        
        if code_challenge:
            params["code_challenge"] = code_challenge
            params["code_challenge_method"] = code_challenge_method
        
        query_string = urlencode(params)
        auth_url = f"{self.config.authorization_url}?{query_string}"
        
        return auth_url, state_id
    
    async def exchange_code_for_token(self, code: str, state: str) -> OAuth2Token:
        """Exchange authorization code for access token."""
        
        # Validate state
        oauth_state = await self.state_manager.get_state(state)
        if not oauth_state:
            raise OAuth2Error("Invalid or expired state")
        
        # Prepare token request
        token_data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.config.redirect_uri,
        }
        
        # Add PKCE if used
        if oauth_state.code_verifier:
            token_data["code_verifier"] = oauth_state.code_verifier
        
        # Add client credentials
        headers = {}
        if self.config.token_endpoint_auth_method == "client_secret_basic":
            credentials = f"{self.config.client_id}:{self.config.client_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_credentials}"
        else:
            token_data["client_id"] = self.config.client_id
            if self.config.client_secret:
                token_data["client_secret"] = self.config.client_secret
        
        # Make token request
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.config.token_url,
                data=token_data,
                headers=headers
            )
            
            if response.status_code != 200:
                error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
                error = error_data.get("error", f"HTTP {response.status_code}")
                raise OAuth2Error(f"Token exchange failed: {error}")
            
            token_response = response.json()
        
        # Create token object
        token = OAuth2Token(
            access_token=token_response["access_token"],
            token_type=TokenType(token_response.get("token_type", "bearer").lower()),
            expires_in=token_response.get("expires_in"),
            refresh_token=token_response.get("refresh_token"),
            scope=self.config.parse_scopes(token_response.get("scope", "")),
            id_token=token_response.get("id_token"),
        )
        
        # Clean up state
        await self.state_manager.remove_state(state)
        
        return token


class PKCEFlow(AuthorizationCodeFlow):
    """PKCE-enhanced Authorization Code flow."""
    
    def __init__(self, config: OAuth2Config, state_manager: StateManager):
        # Force PKCE enabled
        config.enable_pkce = True
        super().__init__(config, state_manager)


class ClientCredentialsFlow:
    """Client Credentials flow implementation."""
    
    def __init__(self, config: OAuth2Config):
        self.config = config
    
    async def request_token(self, scopes: Optional[List[str]] = None) -> OAuth2Token:
        """Request access token using client credentials."""
        
        # Prepare token request
        token_data = {
            "grant_type": "client_credentials",
        }
        
        if scopes:
            token_data["scope"] = self.config.format_scopes(scopes)
        
        # Add client credentials
        headers = {}
        if self.config.token_endpoint_auth_method == "client_secret_basic":
            credentials = f"{self.config.client_id}:{self.config.client_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_credentials}"
        else:
            token_data["client_id"] = self.config.client_id
            if self.config.client_secret:
                token_data["client_secret"] = self.config.client_secret
        
        # Make token request
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.config.token_url,
                data=token_data,
                headers=headers
            )
            
            if response.status_code != 200:
                error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
                error = error_data.get("error", f"HTTP {response.status_code}")
                raise OAuth2Error(f"Token request failed: {error}")
            
            token_response = response.json()
        
        # Create token object
        return OAuth2Token(
            access_token=token_response["access_token"],
            token_type=TokenType(token_response.get("token_type", "bearer").lower()),
            expires_in=token_response.get("expires_in"),
            scope=self.config.parse_scopes(token_response.get("scope", "")),
        )


class RefreshTokenFlow:
    """Refresh Token flow implementation."""
    
    def __init__(self, config: OAuth2Config):
        self.config = config
    
    async def refresh_token(self, token: OAuth2Token) -> OAuth2Token:
        """Refresh access token using refresh token."""
        
        if not token.refresh_token:
            raise OAuth2Error("No refresh token available")
        
        # Prepare refresh request
        token_data = {
            "grant_type": "refresh_token",
            "refresh_token": token.refresh_token,
        }
        
        # Add client credentials
        headers = {}
        if self.config.token_endpoint_auth_method == "client_secret_basic":
            credentials = f"{self.config.client_id}:{self.config.client_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_credentials}"
        else:
            token_data["client_id"] = self.config.client_id
            if self.config.client_secret:
                token_data["client_secret"] = self.config.client_secret
        
        # Make refresh request
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.config.token_url,
                data=token_data,
                headers=headers
            )
            
            if response.status_code != 200:
                error_data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
                error = error_data.get("error", f"HTTP {response.status_code}")
                raise OAuth2Error(f"Token refresh failed: {error}")
            
            token_response = response.json()
        
        # Create new token object
        return OAuth2Token(
            access_token=token_response["access_token"],
            token_type=TokenType(token_response.get("token_type", "bearer").lower()),
            expires_in=token_response.get("expires_in"),
            refresh_token=token_response.get("refresh_token", token.refresh_token),
            scope=self.config.parse_scopes(token_response.get("scope", "")),
            id_token=token_response.get("id_token"),
        )


class OAuth2FlowManager:
    """Manages OAuth2 flows and providers."""
    
    def __init__(self):
        self.providers: Dict[str, OAuth2Provider] = {}
        self.state_manager = StateManager()
    
    async def register_provider(self, provider: OAuth2Provider):
        """Register OAuth2 provider."""
        self.providers[provider.name] = provider
        logger.info(f"Registered OAuth2 provider: {provider.name}")
    
    async def get_provider(self, provider_name: str) -> Optional[OAuth2Provider]:
        """Get OAuth2 provider."""
        return self.providers.get(provider_name)
    
    async def start_authorization_flow(
        self,
        provider_name: str,
        user_id: Optional[str] = None,
        workflow_id: Optional[str] = None,
        scopes: Optional[List[str]] = None,
    ) -> tuple[str, str]:
        """Start authorization flow."""
        
        provider = await self.get_provider(provider_name)
        if not provider:
            raise OAuth2Error(f"Provider '{provider_name}' not found")
        
        if provider.config.flow_type == FlowType.AUTHORIZATION_CODE:
            if provider.supports_pkce():
                flow = PKCEFlow(provider.config, self.state_manager)
            else:
                flow = AuthorizationCodeFlow(provider.config, self.state_manager)
            
            return await flow.generate_authorization_url(
                user_id=user_id,
                workflow_id=workflow_id,
                additional_scopes=scopes
            )
        else:
            raise OAuth2Error(f"Flow type '{provider.config.flow_type}' not supported for authorization")
    
    async def handle_callback(
        self,
        provider_name: str,
        code: str,
        state: str,
    ) -> OAuth2Token:
        """Handle OAuth2 callback."""
        
        provider = await self.get_provider(provider_name)
        if not provider:
            raise OAuth2Error(f"Provider '{provider_name}' not found")
        
        if provider.config.flow_type == FlowType.AUTHORIZATION_CODE:
            if provider.supports_pkce():
                flow = PKCEFlow(provider.config, self.state_manager)
            else:
                flow = AuthorizationCodeFlow(provider.config, self.state_manager)
            
            return await flow.exchange_code_for_token(code, state)
        else:
            raise OAuth2Error(f"Flow type '{provider.config.flow_type}' not supported for callback")
    
    async def refresh_token(
        self,
        provider_name: str,
        token: OAuth2Token,
    ) -> OAuth2Token:
        """Refresh access token."""
        
        provider = await self.get_provider(provider_name)
        if not provider:
            raise OAuth2Error(f"Provider '{provider_name}' not found")
        
        refresh_flow = RefreshTokenFlow(provider.config)
        return await refresh_flow.refresh_token(token)
    
    async def revoke_token(
        self,
        provider_name: str,
        token: OAuth2Token,
    ) -> bool:
        """Revoke access token."""
        
        provider = await self.get_provider(provider_name)
        if not provider:
            raise OAuth2Error(f"Provider '{provider_name}' not found")
        
        if not provider.config.revocation_url:
            return True  # No revocation endpoint, consider it successful
        
        try:
            # Prepare revocation request
            revoke_data = {
                "token": token.access_token,
                "token_type_hint": "access_token",
            }
            
            # Add client credentials
            headers = {}
            if provider.config.token_endpoint_auth_method == "client_secret_basic":
                credentials = f"{provider.config.client_id}:{provider.config.client_secret}"
                encoded_credentials = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded_credentials}"
            else:
                revoke_data["client_id"] = provider.config.client_id
                if provider.config.client_secret:
                    revoke_data["client_secret"] = provider.config.client_secret
            
            # Make revocation request
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    provider.config.revocation_url,
                    data=revoke_data,
                    headers=headers
                )
                
                # RFC 7009 says successful revocation should return 200
                return response.status_code == 200
                
        except Exception as e:
            logger.error(f"Token revocation failed: {str(e)}")
            return False


# Export all classes
__all__ = [
    'OAuth2FlowManager',
    'OAuth2Config',
    'OAuth2Provider',
    'OAuth2Flow',
    'OAuth2Token',
    'OAuth2State',
    'OAuth2Error',
    'AuthorizationCodeFlow',
    'PKCEFlow',
    'ClientCredentialsFlow',
    'RefreshTokenFlow',
    'TokenValidator',
    'StateManager',
    'FlowType',
    'GrantType',
    'TokenType',
    'OAuth2Scope',
]