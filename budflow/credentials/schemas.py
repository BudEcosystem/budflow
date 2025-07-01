"""Credential API schemas."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class OAuthTokenData(BaseModel):
    """OAuth token data schema."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: Optional[int] = None
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "access_token": "ya29.a0AfH6SMBx...",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": "1//0gBq7...",
                "scope": "read write"
            }
        }


class CredentialBase(BaseModel):
    """Base credential schema."""
    name: str = Field(..., min_length=1, max_length=255, description="Credential name")
    type: str = Field(..., min_length=1, max_length=50, description="Credential type")
    description: Optional[str] = Field(None, description="Credential description")
    data: Dict[str, Any] = Field(..., description="Credential data (will be encrypted)")
    settings: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional settings")
    no_data_expression: Optional[bool] = Field(False, description="Disable expression resolution")
    oauth_token_data: Optional[OAuthTokenData] = Field(None, description="OAuth token data")


class CredentialCreate(CredentialBase):
    """Schema for creating a credential."""
    pass


class CredentialUpdate(BaseModel):
    """Schema for updating a credential."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    settings: Optional[Dict[str, Any]] = None
    no_data_expression: Optional[bool] = None
    oauth_token_data: Optional[OAuthTokenData] = None


class CredentialResponse(BaseModel):
    """Schema for credential response (without sensitive data)."""
    id: int
    name: str
    type: str
    description: Optional[str] = None
    settings: Dict[str, Any] = Field(default_factory=dict)
    no_data_expression: bool = False
    user_id: int
    created_at: datetime
    updated_at: datetime
    last_used_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    usage_count: int = 0
    is_expired: bool = False
    is_oauth: bool = False
    shared_with: List[Dict[str, Any]] = Field(default_factory=list)
    
    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "name": "My API Key",
                "type": "apiKey",
                "description": "API key for external service",
                "settings": {"timeout": 30},
                "no_data_expression": False,
                "user_id": 1,
                "created_at": "2024-01-15T10:00:00Z",
                "updated_at": "2024-01-15T10:00:00Z",
                "last_used_at": "2024-01-16T15:30:00Z",
                "expires_at": None,
                "usage_count": 5,
                "is_expired": False,
                "is_oauth": False,
                "shared_with": []
            }
        }


class CredentialListResponse(BaseModel):
    """Schema for credential list response."""
    items: List[CredentialResponse]
    total: int
    skip: int = 0
    limit: int = 100
    
    class Config:
        schema_extra = {
            "example": {
                "items": [
                    {
                        "id": 1,
                        "name": "My API Key",
                        "type": "apiKey",
                        "user_id": 1,
                        "created_at": "2024-01-15T10:00:00Z",
                        "updated_at": "2024-01-15T10:00:00Z",
                        "usage_count": 5
                    }
                ],
                "total": 1,
                "skip": 0,
                "limit": 100
            }
        }


class CredentialShare(BaseModel):
    """Schema for sharing credentials."""
    user_ids: List[int] = Field(..., min_items=1, description="User IDs to share with")
    permissions: List[str] = Field(
        default=["use"],
        description="Permissions to grant (use, edit)"
    )
    
    @validator("permissions")
    def validate_permissions(cls, v):
        valid_permissions = {"use", "edit"}
        for perm in v:
            if perm not in valid_permissions:
                raise ValueError(f"Invalid permission: {perm}")
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "user_ids": [2, 3],
                "permissions": ["use"]
            }
        }


class CredentialTestResponse(BaseModel):
    """Schema for credential test response."""
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Credential tested successfully",
                "details": {
                    "response_time": 150,
                    "status_code": 200
                }
            }
        }


class CredentialTypeProperty(BaseModel):
    """Schema for credential type property definition."""
    name: str
    displayName: str
    type: str  # string, number, boolean, options
    required: bool = False
    default: Optional[Any] = None
    placeholder: Optional[str] = None
    typeOptions: Optional[Dict[str, Any]] = None
    displayOptions: Optional[Dict[str, Any]] = None
    
    class Config:
        schema_extra = {
            "example": {
                "name": "apiKey",
                "displayName": "API Key",
                "type": "string",
                "required": True,
                "placeholder": "Enter your API key",
                "typeOptions": {
                    "password": True
                }
            }
        }


class CredentialTypeDefinition(BaseModel):
    """Schema for credential type definition."""
    name: str
    displayName: str
    icon: Optional[str] = None
    documentationUrl: Optional[str] = None
    properties: List[CredentialTypeProperty]
    oauthConfig: Optional[Dict[str, Any]] = None
    testFunction: Optional[str] = None
    generic: bool = False
    
    class Config:
        schema_extra = {
            "example": {
                "name": "apiKey",
                "displayName": "API Key",
                "icon": "fa:key",
                "properties": [
                    {
                        "name": "apiKey",
                        "displayName": "API Key",
                        "type": "string",
                        "required": True,
                        "typeOptions": {
                            "password": True
                        }
                    }
                ]
            }
        }


class CredentialTypesResponse(BaseModel):
    """Schema for credential types list response."""
    types: List[CredentialTypeDefinition]
    
    class Config:
        schema_extra = {
            "example": {
                "types": [
                    {
                        "name": "apiKey",
                        "displayName": "API Key",
                        "properties": [
                            {
                                "name": "apiKey",
                                "displayName": "API Key",
                                "type": "string",
                                "required": True
                            }
                        ]
                    }
                ]
            }
        }


class OAuthAuthUrlResponse(BaseModel):
    """Schema for OAuth authorization URL response."""
    authUrl: str
    state: str
    
    class Config:
        schema_extra = {
            "example": {
                "authUrl": "https://accounts.google.com/o/oauth2/v2/auth?client_id=...",
                "state": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
            }
        }


class OAuthCallbackRequest(BaseModel):
    """Schema for OAuth callback request."""
    code: str = Field(..., description="Authorization code from OAuth provider")
    state: str = Field(..., description="State parameter for CSRF protection")
    
    class Config:
        schema_extra = {
            "example": {
                "code": "4/0AX4XfWj...",
                "state": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
            }
        }


class BulkDeleteRequest(BaseModel):
    """Schema for bulk delete request."""
    credentialIds: List[int] = Field(..., min_items=1, description="Credential IDs to delete")
    
    class Config:
        schema_extra = {
            "example": {
                "credentialIds": [1, 2, 3]
            }
        }


class BulkDeleteResponse(BaseModel):
    """Schema for bulk delete response."""
    deleted: int = Field(..., description="Number of credentials deleted")
    
    class Config:
        schema_extra = {
            "example": {
                "deleted": 3
            }
        }