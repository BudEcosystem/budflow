"""Webhook data models."""

import re
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any, List, Pattern
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, ConfigDict


class WebhookMethod(str, Enum):
    """HTTP methods supported for webhooks."""
    
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class WebhookAuthType(str, Enum):
    """Webhook authentication types."""
    
    NONE = "none"
    HEADER = "header"
    BASIC_AUTH = "basic_auth"
    QUERY_PARAM = "query_param"


class WebhookResponseMode(str, Enum):
    """Webhook response modes."""
    
    IMMEDIATE = "immediate"  # Return immediately with static response
    LAST_NODE = "last_node"  # Wait for workflow execution and return last node data
    RESPONSE_NODE = "response_node"  # Return data from specific response node
    NO_RESPONSE = "no_response"  # Return 204 No Content


class WebhookPath:
    """Represents a webhook path with support for dynamic parameters."""
    
    PARAM_PATTERN = re.compile(r':([a-zA-Z_][a-zA-Z0-9_]*)')
    
    def __init__(self, path: str):
        self.path = self._normalize_path(path)
        self.is_dynamic = ':' in self.path
        self.parameters = self._extract_parameters()
        self.regex = self._create_regex() if self.is_dynamic else None
    
    def _normalize_path(self, path: str) -> str:
        """Normalize the path to have consistent format."""
        # Remove duplicate slashes
        path = re.sub(r'/+', '/', path)
        
        # Ensure path starts with /
        if not path.startswith('/'):
            path = '/' + path
        
        # Remove trailing slash unless it's root
        if path != '/' and path.endswith('/'):
            path = path[:-1]
        
        return path
    
    def _extract_parameters(self) -> List[str]:
        """Extract parameter names from dynamic path."""
        return self.PARAM_PATTERN.findall(self.path)
    
    def _create_regex(self) -> Pattern:
        """Create regex pattern for matching dynamic paths."""
        pattern = self.path
        
        # Replace parameters with regex groups
        for param in self.parameters:
            pattern = pattern.replace(f':{param}', f'(?P<{param}>[^/]+)')
        
        # Ensure exact match
        pattern = f'^{pattern}$'
        
        return re.compile(pattern)
    
    def match(self, path: str) -> Optional[Dict[str, str]]:
        """Match a path against this webhook path."""
        normalized = self._normalize_path(path)
        
        if not self.is_dynamic:
            return {} if normalized == self.path else None
        
        match = self.regex.match(normalized)
        if not match:
            return None
        
        return match.groupdict()
    
    def __str__(self) -> str:
        return self.path
    
    def __repr__(self) -> str:
        return f"WebhookPath({self.path!r})"


class WebhookAuthentication(BaseModel):
    """Webhook authentication configuration."""
    
    type: WebhookAuthType = WebhookAuthType.NONE
    header_name: Optional[str] = None
    token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    query_param_name: Optional[str] = None
    
    @field_validator("header_name")
    def validate_header_name(cls, v, info):
        if info.data.get("type") == WebhookAuthType.HEADER and not v:
            raise ValueError("header_name required for header authentication")
        return v
    
    @field_validator("username")
    def validate_username(cls, v, info):
        if info.data.get("type") == WebhookAuthType.BASIC_AUTH and not v:
            raise ValueError("username required for basic authentication")
        return v
    
    @field_validator("query_param_name")
    def validate_query_param(cls, v, info):
        if info.data.get("type") == WebhookAuthType.QUERY_PARAM and not v:
            raise ValueError("query_param_name required for query parameter authentication")
        return v


class WebhookRegistration(BaseModel):
    """Represents a registered webhook."""
    
    id: str = Field(default_factory=lambda: str(uuid4()))
    workflow_id: str
    node_id: str
    path: str
    method: WebhookMethod
    is_active: bool = True
    authentication: Optional[WebhookAuthentication] = None
    response_mode: WebhookResponseMode = WebhookResponseMode.LAST_NODE
    response_data: Optional[str] = None
    response_headers: Dict[str, str] = Field(default_factory=dict)
    rate_limit: Optional[int] = None  # Requests per minute
    rate_limit_window: int = 60  # Seconds
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Runtime fields (not persisted)
    path_params: Optional[Dict[str, str]] = None
    
    model_config = ConfigDict(exclude={"path_params"})


class WebhookRequest(BaseModel):
    """Incoming webhook request data."""
    
    webhook_id: str
    workflow_id: str
    node_id: str
    path: str
    method: WebhookMethod
    headers: Dict[str, str]
    query_params: Dict[str, str]
    path_params: Dict[str, str] = Field(default_factory=dict)
    body: Any = None
    binary_data: Optional[bytes] = None
    remote_addr: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class WebhookResponse(BaseModel):
    """Webhook response data."""
    
    status_code: int = 200
    headers: Dict[str, str] = Field(default_factory=dict)
    body: Any = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response."""
        return {
            "status_code": self.status_code,
            "headers": self.headers,
            "body": self.body,
        }