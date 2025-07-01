"""Authentication Pydantic schemas for API validation."""

import uuid
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field, ConfigDict


# User schemas
class UserBase(BaseModel):
    """Base user schema."""
    
    email: EmailStr
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)


class UserCreate(UserBase):
    """Schema for user creation."""
    
    password: str = Field(..., min_length=8, max_length=128)
    confirm_password: str = Field(..., min_length=8, max_length=128)
    
    def validate_passwords_match(self) -> 'UserCreate':
        """Validate that passwords match."""
        if self.password != self.confirm_password:
            raise ValueError('Passwords do not match')
        return self


class UserUpdate(BaseModel):
    """Schema for user update."""
    
    first_name: Optional[str] = Field(None, min_length=1, max_length=100)
    last_name: Optional[str] = Field(None, min_length=1, max_length=100)
    is_active: Optional[bool] = None


class UserInDB(UserBase):
    """Schema for user in database."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    uuid: uuid.UUID
    is_active: bool
    is_verified: bool
    is_super_admin: bool
    mfa_enabled: bool
    created_at: datetime
    updated_at: datetime
    last_login_at: Optional[datetime] = None


class UserPublic(UserBase):
    """Public user schema (safe for API responses)."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    uuid: uuid.UUID
    is_active: bool
    is_verified: bool
    created_at: datetime
    
    @property
    def full_name(self) -> str:
        """Get user's full name."""
        return f"{self.first_name} {self.last_name}".strip()


# Authentication schemas
class LoginRequest(BaseModel):
    """Login request schema."""
    
    email: EmailStr
    password: str = Field(..., min_length=1)
    remember_me: bool = Field(default=False)


class LoginResponse(BaseModel):
    """Login response schema."""
    
    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int  # seconds
    user: UserPublic
    mfa_required: bool = False
    mfa_token: Optional[str] = None


class MFAVerificationRequest(BaseModel):
    """MFA verification request schema."""
    
    mfa_token: str
    code: str = Field(..., min_length=6, max_length=8)


class RefreshTokenRequest(BaseModel):
    """Refresh token request schema."""
    
    refresh_token: str


class TokenResponse(BaseModel):
    """Token response schema."""
    
    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int


class PasswordResetRequest(BaseModel):
    """Password reset request schema."""
    
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    """Password reset confirmation schema."""
    
    token: str
    new_password: str = Field(..., min_length=8, max_length=128)
    confirm_password: str = Field(..., min_length=8, max_length=128)
    
    def validate_passwords_match(self) -> 'PasswordResetConfirm':
        """Validate that passwords match."""
        if self.new_password != self.confirm_password:
            raise ValueError('Passwords do not match')
        return self


class ChangePasswordRequest(BaseModel):
    """Change password request schema."""
    
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8, max_length=128)
    confirm_password: str = Field(..., min_length=8, max_length=128)
    
    def validate_passwords_match(self) -> 'ChangePasswordRequest':
        """Validate that passwords match."""
        if self.new_password != self.confirm_password:
            raise ValueError('Passwords do not match')
        return self


# MFA schemas
class MFASetupResponse(BaseModel):
    """MFA setup response schema."""
    
    secret: str
    qr_code_url: str
    backup_codes: List[str]


class MFAVerifySetupRequest(BaseModel):
    """MFA setup verification request schema."""
    
    secret: str
    code: str = Field(..., min_length=6, max_length=6)


class MFADisableRequest(BaseModel):
    """MFA disable request schema."""
    
    password: str = Field(..., min_length=1)
    code: Optional[str] = Field(None, min_length=6, max_length=8)  # TOTP code or backup code


class RecoveryCodeUseRequest(BaseModel):
    """Recovery code use request schema."""
    
    recovery_code: str = Field(..., min_length=8, max_length=8)


# Role and Permission schemas
class RoleBase(BaseModel):
    """Base role schema."""
    
    name: str = Field(..., min_length=1, max_length=50)
    display_name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    scope: str = Field(default="global", max_length=20)


class RoleCreate(RoleBase):
    """Schema for role creation."""
    pass


class RoleUpdate(BaseModel):
    """Schema for role update."""
    
    display_name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    is_active: Optional[bool] = None


class RoleInDB(RoleBase):
    """Schema for role in database."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    is_active: bool
    is_system: bool
    created_at: datetime
    updated_at: datetime


class PermissionBase(BaseModel):
    """Base permission schema."""
    
    name: str = Field(..., min_length=1, max_length=100)
    resource: str = Field(..., min_length=1, max_length=50)
    action: str = Field(..., min_length=1, max_length=20)
    description: Optional[str] = Field(None, max_length=500)


class PermissionCreate(PermissionBase):
    """Schema for permission creation."""
    pass


class PermissionInDB(PermissionBase):
    """Schema for permission in database."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    is_active: bool
    created_at: datetime


class UserRoleAssign(BaseModel):
    """Schema for assigning role to user."""
    
    user_id: int
    role_id: int
    scope_type: Optional[str] = Field(None, max_length=20)
    scope_id: Optional[int] = None
    expires_at: Optional[datetime] = None


class UserRoleInDB(BaseModel):
    """Schema for user role in database."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    user_id: int
    role_id: int
    scope_type: Optional[str] = None
    scope_id: Optional[int] = None
    is_active: bool
    assigned_at: datetime
    expires_at: Optional[datetime] = None
    
    # Nested objects
    role: RoleInDB


# Session schemas
class UserSessionInDB(BaseModel):
    """Schema for user session in database."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    user_id: int
    session_token: str
    browser_session_id: str
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    is_active: bool
    created_at: datetime
    last_used_at: datetime
    expires_at: datetime


# API Response schemas
class MessageResponse(BaseModel):
    """Generic message response schema."""
    
    message: str
    success: bool = True


class ErrorResponse(BaseModel):
    """Error response schema."""
    
    error: str
    detail: Optional[str] = None
    success: bool = False


class PaginatedResponse(BaseModel):
    """Paginated response schema."""
    
    items: List[BaseModel]
    total: int
    page: int
    per_page: int
    pages: int
    has_next: bool
    has_prev: bool