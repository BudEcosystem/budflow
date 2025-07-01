"""Authentication service layer."""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import structlog
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from budflow.auth.models import User, Role, Permission, UserRole, UserSession
from budflow.auth.schemas import (
    UserCreate, UserUpdate, LoginRequest, MFAVerificationRequest,
    PasswordResetRequest, PasswordResetConfirm, ChangePasswordRequest,
    UserRoleAssign, TokenResponse
)
from budflow.auth.security import (
    password_manager, token_manager, session_manager, mfa_manager,
    encryption_manager, permission_manager
)
from budflow.database import get_postgres_session

logger = structlog.get_logger()


class AuthenticationError(Exception):
    """Authentication related errors."""
    pass


class AuthorizationError(Exception):
    """Authorization related errors."""
    pass


class UserService:
    """User management service."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create_user(self, user_data: UserCreate) -> User:
        """Create a new user."""
        # Check if user already exists
        existing_user = await self.get_user_by_email(user_data.email)
        if existing_user:
            raise ValueError("User with this email already exists")
        
        # Validate password strength
        is_strong, issues = password_manager.is_password_strong(user_data.password)
        if not is_strong:
            raise ValueError(f"Password not strong enough: {', '.join(issues)}")
        
        # Hash password
        password_hash = password_manager.hash_password(user_data.password)
        
        # Create user
        user = User(
            email=user_data.email,
            password_hash=password_hash,
            first_name=user_data.first_name,
            last_name=user_data.last_name,
            uuid=uuid.uuid4()
        )
        
        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)
        
        logger.info("User created", user_id=user.id, email=user.email)
        return user
    
    async def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Get user by ID."""
        result = await self.db.execute(
            select(User)
            .options(selectinload(User.user_roles).selectinload(UserRole.role))
            .where(User.id == user_id)
        )
        return result.scalar_one_or_none()
    
    async def get_user_by_uuid(self, user_uuid: uuid.UUID) -> Optional[User]:
        """Get user by UUID."""
        result = await self.db.execute(
            select(User)
            .options(selectinload(User.user_roles).selectinload(UserRole.role))
            .where(User.uuid == user_uuid)
        )
        return result.scalar_one_or_none()
    
    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        result = await self.db.execute(
            select(User)
            .options(selectinload(User.user_roles).selectinload(UserRole.role))
            .where(User.email == email.lower())
        )
        return result.scalar_one_or_none()
    
    async def update_user(self, user_id: int, user_data: UserUpdate) -> Optional[User]:
        """Update user information."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return None
        
        # Update fields
        update_data = user_data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(user, field, value)
        
        user.updated_at = datetime.now(timezone.utc)
        
        await self.db.commit()
        await self.db.refresh(user)
        
        logger.info("User updated", user_id=user.id, updated_fields=list(update_data.keys()))
        return user
    
    async def change_password(self, user_id: int, password_data: ChangePasswordRequest) -> bool:
        """Change user password."""
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")
        
        # Verify current password
        if not password_manager.verify_password(password_data.current_password, user.password_hash):
            raise AuthenticationError("Current password is incorrect")
        
        # Validate new password strength
        is_strong, issues = password_manager.is_password_strong(password_data.new_password)
        if not is_strong:
            raise ValueError(f"Password not strong enough: {', '.join(issues)}")
        
        # Update password
        user.password_hash = password_manager.hash_password(password_data.new_password)
        user.updated_at = datetime.now(timezone.utc)
        
        await self.db.commit()
        
        logger.info("Password changed", user_id=user.id)
        return True
    
    async def get_user_permissions(self, user_id: int) -> List[str]:
        """Get all permissions for a user."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return []
        
        permissions = set()
        
        # Add super admin permissions
        if user.is_super_admin:
            permissions.add("admin:*")
        
        # Get permissions from roles
        for user_role in user.user_roles:
            if user_role.is_active and user_role.role.is_active:
                # Check if role assignment is expired
                if user_role.expires_at and user_role.expires_at < datetime.now(timezone.utc):
                    continue
                
                # Get role permissions
                result = await self.db.execute(
                    select(Permission)
                    .join(Permission.role_permissions)
                    .where(
                        and_(
                            Permission.role_permissions.any(role_id=user_role.role_id),
                            Permission.is_active == True
                        )
                    )
                )
                role_permissions = result.scalars().all()
                
                for permission in role_permissions:
                    permissions.add(f"{permission.resource}:{permission.action}")
        
        return list(permissions)
    
    async def assign_role(self, user_id: int, role_assignment: UserRoleAssign) -> bool:
        """Assign role to user."""
        # Check if user exists
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")
        
        # Check if role exists
        result = await self.db.execute(select(Role).where(Role.id == role_assignment.role_id))
        role = result.scalar_one_or_none()
        if not role:
            raise ValueError("Role not found")
        
        # Check if assignment already exists
        existing = await self.db.execute(
            select(UserRole).where(
                and_(
                    UserRole.user_id == user_id,
                    UserRole.role_id == role_assignment.role_id,
                    UserRole.scope_type == role_assignment.scope_type,
                    UserRole.scope_id == role_assignment.scope_id
                )
            )
        )
        
        if existing.scalar_one_or_none():
            raise ValueError("Role assignment already exists")
        
        # Create assignment
        user_role = UserRole(
            user_id=user_id,
            role_id=role_assignment.role_id,
            scope_type=role_assignment.scope_type,
            scope_id=role_assignment.scope_id,
            expires_at=role_assignment.expires_at,
            assigned_by_user_id=role_assignment.user_id  # This should come from the current user
        )
        
        self.db.add(user_role)
        await self.db.commit()
        
        logger.info("Role assigned", user_id=user_id, role_id=role_assignment.role_id)
        return True


class AuthenticationService:
    """Authentication service for login, logout, token management."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.user_service = UserService(db)
    
    async def authenticate_user(self, login_data: LoginRequest) -> Tuple[Optional[User], bool]:
        """Authenticate user with email and password."""
        user = await self.user_service.get_user_by_email(login_data.email)
        
        if not user:
            raise AuthenticationError("Invalid email or password")
        
        if not user.is_active:
            raise AuthenticationError("Account is deactivated")
        
        if not password_manager.verify_password(login_data.password, user.password_hash):
            raise AuthenticationError("Invalid email or password")
        
        # Update last login
        user.last_login_at = datetime.now(timezone.utc)
        await self.db.commit()
        
        return user, user.mfa_enabled
    
    async def login(self, login_data: LoginRequest, ip_address: str, user_agent: str) -> Dict:
        """Login user and create session."""
        user, mfa_required = await self.authenticate_user(login_data)
        
        # Generate browser session ID
        browser_session_id = session_manager.generate_browser_session_id()
        
        if mfa_required:
            # Create temporary MFA token
            mfa_token = token_manager.create_mfa_token(user.id, browser_session_id)
            
            return {
                "mfa_required": True,
                "mfa_token": mfa_token,
                "user_id": user.id
            }
        
        # Create full session
        return await self._create_user_session(
            user, browser_session_id, login_data.remember_me, ip_address, user_agent
        )
    
    async def verify_mfa_and_login(
        self, 
        mfa_data: MFAVerificationRequest,
        ip_address: str,
        user_agent: str
    ) -> Dict:
        """Verify MFA code and complete login."""
        # Verify MFA token
        payload = token_manager.verify_token(mfa_data.mfa_token, "mfa")
        if not payload:
            raise AuthenticationError("Invalid or expired MFA token")
        
        user_id = payload.get("user_id")
        browser_session_id = payload.get("browser_session_id")
        
        user = await self.user_service.get_user_by_id(user_id)
        if not user or not user.mfa_enabled:
            raise AuthenticationError("Invalid MFA verification")
        
        # Verify TOTP code
        if user.mfa_secret:
            decrypted_secret = encryption_manager.decrypt(user.mfa_secret)
            if not mfa_manager.verify_totp_code(decrypted_secret, mfa_data.code):
                # Try recovery code
                if user.recovery_codes_hash:
                    is_valid, new_hash = mfa_manager.verify_recovery_code(
                        user.recovery_codes_hash, mfa_data.code
                    )
                    if is_valid:
                        user.recovery_codes_hash = new_hash
                        await self.db.commit()
                    else:
                        raise AuthenticationError("Invalid MFA code")
                else:
                    raise AuthenticationError("Invalid MFA code")
        
        # Create session
        return await self._create_user_session(
            user, browser_session_id, False, ip_address, user_agent
        )
    
    async def _create_user_session(
        self, 
        user: User, 
        browser_session_id: str,
        remember_me: bool,
        ip_address: str,
        user_agent: str
    ) -> Dict:
        """Create user session and tokens."""
        # Create tokens
        token_data = session_manager.create_session_tokens(
            user.id, browser_session_id, remember_me
        )
        
        # Create session record
        session_token = session_manager.generate_session_token()
        expires_at = datetime.now(timezone.utc) + timedelta(
            days=30 if remember_me else token_manager.refresh_token_expire_days
        )
        
        user_session = UserSession(
            user_id=user.id,
            session_token=session_token,
            browser_session_id=browser_session_id,
            ip_address=ip_address,
            user_agent=user_agent,
            expires_at=expires_at
        )
        
        self.db.add(user_session)
        
        # Update user session info
        user.browser_session_id = browser_session_id
        user.last_login_ip = ip_address
        
        await self.db.commit()
        
        logger.info("User logged in", user_id=user.id, ip_address=ip_address)
        
        return {
            "access_token": token_data["access_token"],
            "refresh_token": token_data["refresh_token"],
            "token_type": "Bearer",
            "expires_in": token_data["expires_in"],
            "user": user,
            "mfa_required": False
        }
    
    async def refresh_token(self, refresh_token: str) -> Optional[TokenResponse]:
        """Refresh access token using refresh token."""
        payload = token_manager.verify_token(refresh_token, "refresh")
        if not payload:
            return None
        
        user_id = payload.get("user_id")
        browser_session_id = payload.get("browser_session_id")
        
        # Verify user and session
        user = await self.user_service.get_user_by_id(user_id)
        if not user or not user.is_active:
            return None
        
        # Check if session exists and is valid
        result = await self.db.execute(
            select(UserSession).where(
                and_(
                    UserSession.user_id == user_id,
                    UserSession.browser_session_id == browser_session_id,
                    UserSession.is_active == True,
                    UserSession.expires_at > datetime.now(timezone.utc)
                )
            )
        )
        session = result.scalar_one_or_none()
        
        if not session:
            return None
        
        # Create new tokens
        token_data = session_manager.create_session_tokens(
            user_id, browser_session_id, False
        )
        
        # Update session last used
        session.last_used_at = datetime.now(timezone.utc)
        await self.db.commit()
        
        return TokenResponse(**token_data)
    
    async def logout(self, user_id: int, browser_session_id: str) -> bool:
        """Logout user and invalidate session."""
        # Deactivate session
        result = await self.db.execute(
            select(UserSession).where(
                and_(
                    UserSession.user_id == user_id,
                    UserSession.browser_session_id == browser_session_id,
                    UserSession.is_active == True
                )
            )
        )
        session = result.scalar_one_or_none()
        
        if session:
            session.is_active = False
            await self.db.commit()
        
        logger.info("User logged out", user_id=user_id)
        return True
    
    async def logout_all_sessions(self, user_id: int) -> bool:
        """Logout user from all sessions."""
        # Deactivate all user sessions
        await self.db.execute(
            select(UserSession)
            .where(UserSession.user_id == user_id)
            .update({"is_active": False})
        )
        
        await self.db.commit()
        
        logger.info("User logged out from all sessions", user_id=user_id)
        return True