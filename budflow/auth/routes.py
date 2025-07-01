"""Authentication API routes."""

from typing import List

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.auth.dependencies import (
    get_current_user, get_admin_user, require_user_management
)
from budflow.auth.models import User
from budflow.auth.schemas import (
    UserCreate, UserUpdate, UserPublic, UserInDB,
    LoginRequest, LoginResponse, MFAVerificationRequest,
    RefreshTokenRequest, TokenResponse,
    PasswordResetRequest, PasswordResetConfirm, ChangePasswordRequest,
    MFASetupResponse, MFAVerifySetupRequest, MFADisableRequest,
    MessageResponse, ErrorResponse
)
from budflow.auth.service import UserService, AuthenticationService, AuthenticationError
from budflow.database import get_postgres_session

logger = structlog.get_logger()

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/register", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
async def register_user(
    user_data: UserCreate,
    db: AsyncSession = Depends(get_postgres_session)
):
    """Register a new user."""
    try:
        # Validate password match
        user_data.validate_passwords_match()
        
        user_service = UserService(db)
        user = await user_service.create_user(user_data)
        
        logger.info("User registered", user_id=user.id, email=user.email)
        return UserPublic.model_validate(user)
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/login", response_model=LoginResponse)
async def login(
    login_data: LoginRequest,
    request: Request,
    db: AsyncSession = Depends(get_postgres_session)
):
    """Login user with email and password."""
    try:
        auth_service = AuthenticationService(db)
        
        # Get client info
        ip_address = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        
        result = await auth_service.login(login_data, ip_address, user_agent)
        
        if result.get("mfa_required"):
            return LoginResponse(
                access_token="",
                refresh_token="",
                expires_in=0,
                user=UserPublic.model_validate(await UserService(db).get_user_by_id(result["user_id"])),
                mfa_required=True,
                mfa_token=result["mfa_token"]
            )
        
        return LoginResponse(
            access_token=result["access_token"],
            refresh_token=result["refresh_token"],
            token_type="Bearer",
            expires_in=result["expires_in"],
            user=UserPublic.model_validate(result["user"]),
            mfa_required=False
        )
    
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )


@router.post("/mfa/verify", response_model=LoginResponse)
async def verify_mfa(
    mfa_data: MFAVerificationRequest,
    request: Request,
    db: AsyncSession = Depends(get_postgres_session)
):
    """Verify MFA code and complete login."""
    try:
        auth_service = AuthenticationService(db)
        
        # Get client info
        ip_address = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")
        
        result = await auth_service.verify_mfa_and_login(mfa_data, ip_address, user_agent)
        
        return LoginResponse(
            access_token=result["access_token"],
            refresh_token=result["refresh_token"],
            token_type="Bearer",
            expires_in=result["expires_in"],
            user=UserPublic.model_validate(result["user"]),
            mfa_required=False
        )
    
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e)
        )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(
    refresh_data: RefreshTokenRequest,
    db: AsyncSession = Depends(get_postgres_session)
):
    """Refresh access token using refresh token."""
    auth_service = AuthenticationService(db)
    token_response = await auth_service.refresh_token(refresh_data.refresh_token)
    
    if not token_response:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token"
        )
    
    return token_response


@router.post("/logout", response_model=MessageResponse)
async def logout(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_postgres_session)
):
    """Logout current user."""
    auth_service = AuthenticationService(db)
    await auth_service.logout(current_user.id, current_user.browser_session_id)
    
    return MessageResponse(message="Successfully logged out")


@router.post("/logout-all", response_model=MessageResponse)
async def logout_all_sessions(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_postgres_session)
):
    """Logout user from all sessions."""
    auth_service = AuthenticationService(db)
    await auth_service.logout_all_sessions(current_user.id)
    
    return MessageResponse(message="Successfully logged out from all sessions")


@router.get("/me", response_model=UserInDB)
async def get_current_user_info(
    current_user: User = Depends(get_current_user)
):
    """Get current user information."""
    return UserInDB.model_validate(current_user)


@router.patch("/me", response_model=UserPublic)
async def update_current_user(
    user_data: UserUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_postgres_session)
):
    """Update current user information."""
    user_service = UserService(db)
    updated_user = await user_service.update_user(current_user.id, user_data)
    
    if not updated_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return UserPublic.model_validate(updated_user)


@router.post("/change-password", response_model=MessageResponse)
async def change_password(
    password_data: ChangePasswordRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_postgres_session)
):
    """Change current user password."""
    try:
        # Validate password match
        password_data.validate_passwords_match()
        
        user_service = UserService(db)
        await user_service.change_password(current_user.id, password_data)
        
        return MessageResponse(message="Password changed successfully")
    
    except (ValueError, AuthenticationError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/setup-mfa", response_model=MFASetupResponse)
async def setup_mfa(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_postgres_session)
):
    """Set up multi-factor authentication."""
    if current_user.mfa_enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="MFA is already enabled"
        )
    
    from budflow.auth.security import mfa_manager, encryption_manager
    
    # Generate secret and QR code
    secret = mfa_manager.generate_secret()
    qr_code_url = mfa_manager.generate_qr_code_url(secret, current_user.email)
    backup_codes = mfa_manager.generate_recovery_codes()
    
    return MFASetupResponse(
        secret=secret,
        qr_code_url=qr_code_url,
        backup_codes=backup_codes
    )


@router.post("/verify-mfa-setup", response_model=MessageResponse)
async def verify_mfa_setup(
    setup_data: MFAVerifySetupRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_postgres_session)
):
    """Verify and complete MFA setup."""
    if current_user.mfa_enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="MFA is already enabled"
        )
    
    from budflow.auth.security import mfa_manager, encryption_manager
    
    # Verify TOTP code
    if not mfa_manager.verify_totp_code(setup_data.secret, setup_data.code):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid verification code"
        )
    
    # Enable MFA
    current_user.mfa_enabled = True
    current_user.mfa_secret = encryption_manager.encrypt(setup_data.secret)
    
    # Generate and store recovery codes
    backup_codes = mfa_manager.generate_recovery_codes()
    current_user.recovery_codes_hash = mfa_manager.hash_recovery_codes(backup_codes)
    
    await db.commit()
    
    logger.info("MFA enabled", user_id=current_user.id)
    return MessageResponse(message="MFA enabled successfully")


@router.post("/disable-mfa", response_model=MessageResponse)
async def disable_mfa(
    disable_data: MFADisableRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_postgres_session)
):
    """Disable multi-factor authentication."""
    if not current_user.mfa_enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="MFA is not enabled"
        )
    
    from budflow.auth.security import password_manager, mfa_manager, encryption_manager
    
    # Verify password
    if not password_manager.verify_password(disable_data.password, current_user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid password"
        )
    
    # Verify MFA code if provided
    if disable_data.code:
        is_valid = False
        
        # Try TOTP code
        if current_user.mfa_secret:
            decrypted_secret = encryption_manager.decrypt(current_user.mfa_secret)
            is_valid = mfa_manager.verify_totp_code(decrypted_secret, disable_data.code)
        
        # Try recovery code
        if not is_valid and current_user.recovery_codes_hash:
            is_valid, _ = mfa_manager.verify_recovery_code(
                current_user.recovery_codes_hash, disable_data.code
            )
        
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid MFA code"
            )
    
    # Disable MFA
    current_user.mfa_enabled = False
    current_user.mfa_secret = None
    current_user.recovery_codes_hash = None
    
    await db.commit()
    
    logger.info("MFA disabled", user_id=current_user.id)
    return MessageResponse(message="MFA disabled successfully")


# Admin routes
@router.get("/users", response_model=List[UserPublic])
async def list_users(
    skip: int = 0,
    limit: int = 100,
    admin_user: User = Depends(require_user_management()),
    db: AsyncSession = Depends(get_postgres_session)
):
    """List all users (admin only)."""
    from sqlalchemy import select
    
    result = await db.execute(
        select(User)
        .where(User.is_active == True)
        .offset(skip)
        .limit(limit)
        .order_by(User.created_at.desc())
    )
    users = result.scalars().all()
    
    return [UserPublic.model_validate(user) for user in users]


@router.get("/users/{user_id}", response_model=UserInDB)
async def get_user(
    user_id: int,
    admin_user: User = Depends(require_user_management()),
    db: AsyncSession = Depends(get_postgres_session)
):
    """Get user by ID (admin only)."""
    user_service = UserService(db)
    user = await user_service.get_user_by_id(user_id)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return UserInDB.model_validate(user)


@router.patch("/users/{user_id}", response_model=UserPublic)
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    admin_user: User = Depends(require_user_management()),
    db: AsyncSession = Depends(get_postgres_session)
):
    """Update user (admin only)."""
    user_service = UserService(db)
    updated_user = await user_service.update_user(user_id, user_data)
    
    if not updated_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return UserPublic.model_validate(updated_user)