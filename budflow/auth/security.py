"""Security utilities for authentication and authorization."""

import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import bcrypt
from cryptography.fernet import Fernet
from jose import JWTError, jwt
from passlib.context import CryptContext

from budflow.config import settings


class PasswordManager:
    """Password hashing and verification utilities."""
    
    def __init__(self):
        self.pwd_context = CryptContext(
            schemes=["bcrypt"], 
            deprecated="auto",
            bcrypt__rounds=settings.salt_rounds
        )
    
    def hash_password(self, password: str) -> str:
        """Hash a password using bcrypt."""
        return self.pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        return self.pwd_context.verify(plain_password, hashed_password)
    
    def is_password_strong(self, password: str) -> tuple[bool, List[str]]:
        """Check if password meets strength requirements."""
        issues = []
        
        if len(password) < 8:
            issues.append("Password must be at least 8 characters long")
        
        if len(password) > 128:
            issues.append("Password must not exceed 128 characters")
        
        if not any(c.islower() for c in password):
            issues.append("Password must contain at least one lowercase letter")
        
        if not any(c.isupper() for c in password):
            issues.append("Password must contain at least one uppercase letter")
        
        if not any(c.isdigit() for c in password):
            issues.append("Password must contain at least one digit")
        
        if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
            issues.append("Password must contain at least one special character")
        
        # Check for common patterns
        if password.lower() in ["password", "123456", "qwerty", "admin"]:
            issues.append("Password is too common")
        
        return len(issues) == 0, issues


class TokenManager:
    """JWT token management utilities."""
    
    def __init__(self):
        self.secret_key = settings.secret_key
        self.algorithm = settings.algorithm
        self.access_token_expire_hours = settings.access_token_expire_hours
        self.refresh_token_expire_days = settings.refresh_token_expire_days
    
    def create_access_token(
        self, 
        data: Dict[str, Any], 
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """Create JWT access token."""
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(hours=self.access_token_expire_hours)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "type": "access"
        })
        
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    def create_refresh_token(
        self, 
        data: Dict[str, Any], 
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """Create JWT refresh token."""
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(days=self.refresh_token_expire_days)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.now(timezone.utc),
            "type": "refresh"
        })
        
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    def create_mfa_token(self, user_id: int, browser_session_id: str) -> str:
        """Create temporary MFA verification token."""
        data = {
            "user_id": user_id,
            "browser_session_id": browser_session_id,
            "type": "mfa",
            "exp": datetime.now(timezone.utc) + timedelta(minutes=5)  # 5 minute expiry
        }
        
        return jwt.encode(data, self.secret_key, algorithm=self.algorithm)
    
    def verify_token(self, token: str, token_type: str = "access") -> Optional[Dict[str, Any]]:
        """Verify and decode JWT token."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Check token type
            if payload.get("type") != token_type:
                return None
            
            # Check expiration
            exp = payload.get("exp")
            if exp and datetime.fromtimestamp(exp, timezone.utc) < datetime.now(timezone.utc):
                return None
            
            return payload
        
        except JWTError:
            return None
    
    def get_token_expiry(self, token: str) -> Optional[datetime]:
        """Get token expiration datetime."""
        try:
            payload = jwt.decode(
                token, 
                self.secret_key, 
                algorithms=[self.algorithm],
                options={"verify_exp": False}  # Don't verify expiration for this check
            )
            exp = payload.get("exp")
            if exp:
                return datetime.fromtimestamp(exp, timezone.utc)
            return None
        except JWTError:
            return None


class EncryptionManager:
    """Data encryption utilities."""
    
    def __init__(self):
        # Derive a consistent key from the encryption key setting
        key_material = settings.encryption_key.encode()
        key = hashlib.sha256(key_material).digest()
        self.fernet = Fernet(Fernet.generate_key())  # We'll use a proper key derivation in production
    
    def encrypt(self, data: str) -> str:
        """Encrypt sensitive data."""
        return self.fernet.encrypt(data.encode()).decode()
    
    def decrypt(self, encrypted_data: str) -> str:
        """Decrypt sensitive data."""
        return self.fernet.decrypt(encrypted_data.encode()).decode()


class SessionManager:
    """Session management utilities."""
    
    def __init__(self):
        self.token_manager = TokenManager()
    
    def generate_browser_session_id(self) -> str:
        """Generate unique browser session ID."""
        return str(uuid.uuid4())
    
    def generate_session_token(self) -> str:
        """Generate secure session token."""
        return secrets.token_urlsafe(32)
    
    def create_session_tokens(
        self, 
        user_id: int, 
        browser_session_id: str,
        remember_me: bool = False
    ) -> Dict[str, str]:
        """Create access and refresh tokens for session."""
        
        # Base token data
        token_data = {
            "user_id": user_id,
            "browser_session_id": browser_session_id,
        }
        
        # Create tokens
        access_token = self.token_manager.create_access_token(token_data)
        
        # Longer expiry for refresh token if remember_me is True
        if remember_me:
            refresh_expires = timedelta(days=30)
        else:
            refresh_expires = timedelta(days=self.token_manager.refresh_token_expire_days)
        
        refresh_token = self.token_manager.create_refresh_token(
            token_data, 
            expires_delta=refresh_expires
        )
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": int(timedelta(hours=self.token_manager.access_token_expire_hours).total_seconds())
        }


class MFAManager:
    """Multi-Factor Authentication utilities."""
    
    def __init__(self):
        self.encryption = EncryptionManager()
    
    def generate_secret(self) -> str:
        """Generate TOTP secret."""
        import pyotp
        return pyotp.random_base32()
    
    def generate_qr_code_url(self, secret: str, user_email: str) -> str:
        """Generate QR code URL for TOTP setup."""
        import pyotp
        
        totp = pyotp.TOTP(secret)
        return totp.provisioning_uri(
            name=user_email,
            issuer_name=settings.mfa_issuer
        )
    
    def verify_totp_code(self, secret: str, code: str) -> bool:
        """Verify TOTP code."""
        import pyotp
        
        totp = pyotp.TOTP(secret)
        return totp.verify(code, valid_window=1)  # Allow 1 step tolerance
    
    def generate_recovery_codes(self, count: int = 10) -> List[str]:
        """Generate recovery codes."""
        codes = []
        for _ in range(count):
            # Generate 8-character alphanumeric code
            code = ''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(8))
            codes.append(code)
        return codes
    
    def hash_recovery_codes(self, codes: List[str]) -> str:
        """Hash recovery codes for storage."""
        import json
        
        # Hash each code and store as JSON
        hashed_codes = []
        for code in codes:
            code_hash = hashlib.sha256(code.encode()).hexdigest()
            hashed_codes.append(code_hash)
        
        return json.dumps(hashed_codes)
    
    def verify_recovery_code(self, stored_hash: str, code: str) -> tuple[bool, str]:
        """Verify recovery code and return updated hash if valid."""
        import json
        
        try:
            hashed_codes = json.loads(stored_hash)
            code_hash = hashlib.sha256(code.upper().encode()).hexdigest()
            
            if code_hash in hashed_codes:
                # Remove used code
                hashed_codes.remove(code_hash)
                return True, json.dumps(hashed_codes)
            
            return False, stored_hash
        
        except (json.JSONDecodeError, ValueError):
            return False, stored_hash


class PermissionManager:
    """Permission checking utilities."""
    
    @staticmethod
    def check_permission(
        user_permissions: List[str], 
        required_permission: str
    ) -> bool:
        """Check if user has required permission."""
        return required_permission in user_permissions
    
    @staticmethod
    def check_any_permission(
        user_permissions: List[str], 
        required_permissions: List[str]
    ) -> bool:
        """Check if user has any of the required permissions."""
        return any(perm in user_permissions for perm in required_permissions)
    
    @staticmethod
    def check_all_permissions(
        user_permissions: List[str], 
        required_permissions: List[str]
    ) -> bool:
        """Check if user has all required permissions."""
        return all(perm in user_permissions for perm in required_permissions)
    
    @staticmethod
    def has_resource_access(
        user_permissions: List[str],
        resource: str,
        action: str
    ) -> bool:
        """Check if user can perform action on resource."""
        permission = f"{resource}:{action}"
        wildcard_resource = f"{resource}:*"
        admin_permission = "admin:*"
        
        return any(perm in user_permissions for perm in [
            permission, 
            wildcard_resource, 
            admin_permission
        ])


# Global instances
password_manager = PasswordManager()
token_manager = TokenManager()
encryption_manager = EncryptionManager()
session_manager = SessionManager()
mfa_manager = MFAManager()
permission_manager = PermissionManager()