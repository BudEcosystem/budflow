"""FastAPI dependencies for authentication and authorization."""

from typing import List, Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.auth.models import User
from budflow.auth.service import UserService, AuthenticationService
from budflow.auth.security import token_manager, permission_manager
from budflow.database import get_postgres_session

# Security scheme
bearer_scheme = HTTPBearer(auto_error=False)


class AuthenticationRequired:
    """Dependency to require authentication."""
    
    def __init__(self, require_active: bool = True, require_verified: bool = False):
        self.require_active = require_active
        self.require_verified = require_verified
    
    async def __call__(
        self,
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
        db: AsyncSession = Depends(get_postgres_session)
    ) -> User:
        """Verify authentication and return current user."""
        
        if not credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Verify token
        payload = token_manager.verify_token(credentials.credentials, "access")
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Get user
        user_service = UserService(db)
        user = await user_service.get_user_by_id(user_id)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Check user status
        if self.require_active and not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Account is deactivated",
            )
        
        if self.require_verified and not user.is_verified:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Email verification required",
            )
        
        # Verify browser session if present in token
        browser_session_id = payload.get("browser_session_id")
        if browser_session_id and user.browser_session_id != browser_session_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Session invalid",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        return user


class PermissionRequired:
    """Dependency to require specific permissions."""
    
    def __init__(
        self, 
        permissions: List[str], 
        require_all: bool = True,
        allow_admin_override: bool = True
    ):
        self.permissions = permissions
        self.require_all = require_all
        self.allow_admin_override = allow_admin_override
    
    async def __call__(
        self,
        user: User = Depends(AuthenticationRequired()),
        db: AsyncSession = Depends(get_postgres_session)
    ) -> User:
        """Verify user has required permissions."""
        
        # Super admin bypass
        if self.allow_admin_override and user.is_super_admin:
            return user
        
        # Get user permissions
        user_service = UserService(db)
        user_permissions = await user_service.get_user_permissions(user.id)
        
        # Check permissions
        if self.require_all:
            has_permission = permission_manager.check_all_permissions(
                user_permissions, self.permissions
            )
        else:
            has_permission = permission_manager.check_any_permission(
                user_permissions, self.permissions
            )
        
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions"
            )
        
        return user


class ResourcePermissionRequired:
    """Dependency to require resource-specific permissions."""
    
    def __init__(
        self, 
        resource: str, 
        action: str,
        allow_admin_override: bool = True
    ):
        self.resource = resource
        self.action = action
        self.allow_admin_override = allow_admin_override
    
    async def __call__(
        self,
        user: User = Depends(AuthenticationRequired()),
        db: AsyncSession = Depends(get_postgres_session)
    ) -> User:
        """Verify user can perform action on resource."""
        
        # Super admin bypass
        if self.allow_admin_override and user.is_super_admin:
            return user
        
        # Get user permissions
        user_service = UserService(db)
        user_permissions = await user_service.get_user_permissions(user.id)
        
        # Check resource permission
        has_permission = permission_manager.has_resource_access(
            user_permissions, self.resource, self.action
        )
        
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied for {self.action} on {self.resource}"
            )
        
        return user


class AdminRequired:
    """Dependency to require admin role."""
    
    def __init__(self, require_super_admin: bool = False):
        self.require_super_admin = require_super_admin
    
    async def __call__(
        self,
        user: User = Depends(AuthenticationRequired()),
        db: AsyncSession = Depends(get_postgres_session)
    ) -> User:
        """Verify user has admin role."""
        
        if self.require_super_admin:
            if not user.is_super_admin:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Super admin access required"
                )
        else:
            if not user.is_admin:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Admin access required"
                )
        
        return user


class OptionalAuthentication:
    """Dependency for optional authentication."""
    
    async def __call__(
        self,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
        db: AsyncSession = Depends(get_postgres_session)
    ) -> Optional[User]:
        """Return user if authenticated, None otherwise."""
        
        if not credentials:
            return None
        
        # Verify token
        payload = token_manager.verify_token(credentials.credentials, "access")
        if not payload:
            return None
        
        user_id = payload.get("user_id")
        if not user_id:
            return None
        
        # Get user
        user_service = UserService(db)
        user = await user_service.get_user_by_id(user_id)
        
        if not user or not user.is_active:
            return None
        
        return user


# Common dependency instances
get_current_user = AuthenticationRequired()
get_current_active_user = AuthenticationRequired(require_active=True)
get_current_verified_user = AuthenticationRequired(require_active=True, require_verified=True)
get_admin_user = AdminRequired()
get_super_admin_user = AdminRequired(require_super_admin=True)
get_optional_user = OptionalAuthentication()

# Common permission dependencies
def require_workflow_read() -> PermissionRequired:
    """Require workflow read permission."""
    return PermissionRequired(["workflow:read"])

def require_workflow_write() -> PermissionRequired:
    """Require workflow write permissions."""
    return PermissionRequired(["workflow:create", "workflow:update"], require_all=False)

def require_workflow_delete() -> PermissionRequired:
    """Require workflow delete permission."""
    return PermissionRequired(["workflow:delete"])

def require_workflow_execute() -> PermissionRequired:
    """Require workflow execute permission."""
    return PermissionRequired(["workflow:execute"])

def require_credential_read() -> PermissionRequired:
    """Require credential read permission."""
    return PermissionRequired(["credential:read"])

def require_credential_write() -> PermissionRequired:
    """Require credential write permissions."""
    return PermissionRequired(["credential:create", "credential:update"], require_all=False)

def require_user_management() -> PermissionRequired:
    """Require user management permissions."""
    return PermissionRequired(["user:create", "user:update", "user:delete"], require_all=False)

# Resource-specific permission dependencies
def require_resource_permission(resource: str, action: str) -> ResourcePermissionRequired:
    """Create resource permission dependency."""
    return ResourcePermissionRequired(resource, action)