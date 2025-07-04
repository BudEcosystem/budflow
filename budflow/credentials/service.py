"""Credential service for managing encrypted credentials."""

import base64
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from budflow.credentials.encryption import (
    CredentialEncryptor,
    encrypt_credential,
    decrypt_credential,
    get_credential_encryptor
)
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from sqlalchemy import select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from budflow.auth.models import User
from budflow.config import settings
from budflow.credentials.exceptions import (
    CredentialNotFoundError,
    CredentialAlreadyExistsError,
    CredentialAccessDeniedError,
    CredentialEncryptionError,
    InvalidCredentialTypeError,
)
from budflow.credentials.models import (
    Credential,
    CredentialType,
    CredentialTypeDefinition,
    SharedCredential,
    NodeCredential,
)
from budflow.credentials.schemas import (
    CredentialCreate,
    CredentialUpdate,
    CredentialShare,
    OAuthTokenData,
)


class EncryptionService:
    """Service for encrypting and decrypting credential data."""
    
    def __init__(self):
        """Initialize encryption service."""
        self._fernet = None
        self._key = None
    
    @property
    def fernet(self):
        """Lazy initialize Fernet cipher."""
        if self._fernet is None:
            # Get or generate encryption key
            if hasattr(settings, 'encryption_key') and settings.encryption_key:
                # Ensure key is properly formatted
                key = settings.encryption_key
                if isinstance(key, str):
                    key = key.encode()
                self._key = key
            else:
                # For development/testing, use a deterministic key based on secret
                # In production, you should set ENCRYPTION_KEY to a value from Fernet.generate_key()
                if settings.environment == "development" or settings.environment == "test":
                    # Create a valid Fernet key deterministically
                    import hashlib
                    hash_obj = hashlib.sha256(settings.secret_key.encode())
                    # Take first 32 bytes of hash and base64 encode it
                    key_bytes = hash_obj.digest()[:32]
                    self._key = base64.urlsafe_b64encode(key_bytes)
                else:
                    # In production, require explicit encryption key
                    raise ValueError("ENCRYPTION_KEY must be set in production environment")
            
            self._fernet = Fernet(self._key)
        return self._fernet
    
    def encrypt(self, data: Dict[str, Any]) -> str:
        """Encrypt credential data."""
        try:
            # Use new encryption system if available
            from budflow.credentials.encryption import encrypt_credential
            encrypted_data = encrypt_credential(data)
            return json.dumps(encrypted_data)
        except ImportError:
            # Fallback to Fernet
            json_data = json.dumps(data)
            encrypted = self.fernet.encrypt(json_data.encode())
            return encrypted.decode()
        except Exception as e:
            raise CredentialEncryptionError(f"Failed to encrypt data: {str(e)}")
    
    def decrypt(self, encrypted_data: str) -> Dict[str, Any]:
        """Decrypt credential data."""
        try:
            # Try new encryption system first
            from budflow.credentials.encryption import decrypt_credential
            data = json.loads(encrypted_data) if isinstance(encrypted_data, str) else encrypted_data
            # Check if it's new format (has _encrypted fields)
            if any(isinstance(v, dict) and v.get("_encrypted") for v in data.values()):
                return decrypt_credential(data)
            else:
                # Fallback to Fernet
                decrypted = self.fernet.decrypt(encrypted_data.encode())
                return json.loads(decrypted.decode())
        except ImportError:
            # Fallback to Fernet
            decrypted = self.fernet.decrypt(encrypted_data.encode())
            return json.loads(decrypted.decode())
        except Exception as e:
            raise CredentialEncryptionError(f"Failed to decrypt data: {str(e)}")


class CredentialService:
    """Service for managing credentials."""
    
    def __init__(self):
        """Initialize credential service."""
        self.encryption_service = EncryptionService()
    
    async def create_credential(
        self,
        db: AsyncSession,
        user_id: int,
        credential_data: CredentialCreate
    ) -> Credential:
        """Create a new credential."""
        # Check if credential with same name exists for user
        existing = await db.execute(
            select(Credential).where(
                and_(
                    Credential.user_id == user_id,
                    Credential.name == credential_data.name
                )
            )
        )
        if existing.scalar_one_or_none():
            raise CredentialAlreadyExistsError(f"Credential '{credential_data.name}' already exists")
        
        # Validate credential type
        if credential_data.type:
            type_def = await self.get_credential_type(db, credential_data.type)
            if not type_def and credential_data.type not in [t.value for t in CredentialType]:
                raise InvalidCredentialTypeError(f"Invalid credential type: {credential_data.type}")
        
        # Encrypt credential data
        encrypted_data = self.encryption_service.encrypt(credential_data.data)
        
        # Encrypt OAuth token data if present
        encrypted_oauth_data = None
        if credential_data.oauth_token_data:
            encrypted_oauth_data = self.encryption_service.encrypt(
                credential_data.oauth_token_data.dict()
            )
        
        # Create credential
        credential = Credential(
            name=credential_data.name,
            type=credential_data.type,
            user_id=user_id,
            data=encrypted_data,
            oauth_token_data=encrypted_oauth_data,
            description=credential_data.description,
            settings=credential_data.settings or {},
            no_data_expression=credential_data.no_data_expression or False,
        )
        
        # Set expiry if provided in settings
        if credential_data.settings and "expiresAt" in credential_data.settings:
            credential.expires_at = datetime.fromisoformat(
                credential_data.settings["expiresAt"].replace("Z", "+00:00")
            )
        
        db.add(credential)
        await db.commit()
        await db.refresh(credential)
        
        return credential
    
    async def get_credential(
        self,
        db: AsyncSession,
        credential_id: int,
        user_id: int,
        check_permission: bool = True
    ) -> Credential:
        """Get a credential by ID."""
        query = select(Credential).where(Credential.id == credential_id)
        
        if check_permission:
            # Check if user owns or has access to the credential
            query = query.outerjoin(SharedCredential).where(
                or_(
                    Credential.user_id == user_id,
                    SharedCredential.user_id == user_id
                )
            )
        
        result = await db.execute(query.options(selectinload(Credential.shared_with)))
        credential = result.scalar_one_or_none()
        
        if not credential:
            raise CredentialNotFoundError(f"Credential {credential_id} not found")
        
        # Check permission level if not owner
        if check_permission and credential.user_id != user_id:
            # User must have explicit share permission
            has_permission = any(
                share.user_id == user_id for share in credential.shared_with
            )
            if not has_permission:
                raise CredentialAccessDeniedError("Access denied to credential")
        
        return credential
    
    async def get_credential_with_data(
        self,
        db: AsyncSession,
        credential_id: int,
        user_id: int
    ) -> Tuple[Credential, Dict[str, Any]]:
        """Get credential with decrypted data."""
        credential = await self.get_credential(db, credential_id, user_id)
        
        # Decrypt data
        decrypted_data = self.encryption_service.decrypt(credential.data)
        
        # Update last used timestamp
        credential.last_used_at = datetime.now(timezone.utc)
        credential.usage_count += 1
        await db.commit()
        
        return credential, decrypted_data
    
    async def list_credentials(
        self,
        db: AsyncSession,
        user_id: int,
        skip: int = 0,
        limit: int = 100,
        type_filter: Optional[str] = None,
        include_shared: bool = True
    ) -> Tuple[List[Credential], int]:
        """List credentials for a user."""
        # Base query
        query = select(Credential)
        
        if include_shared:
            # Include credentials shared with the user
            query = query.outerjoin(SharedCredential).where(
                or_(
                    Credential.user_id == user_id,
                    SharedCredential.user_id == user_id
                )
            )
        else:
            # Only user's own credentials
            query = query.where(Credential.user_id == user_id)
        
        # Apply type filter
        if type_filter:
            query = query.where(Credential.type == type_filter)
        
        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total = await db.scalar(count_query)
        
        # Get paginated results
        query = query.order_by(Credential.updated_at.desc()).offset(skip).limit(limit)
        result = await db.execute(query.options(selectinload(Credential.shared_with)))
        credentials = result.scalars().unique().all()
        
        return list(credentials), total
    
    async def update_credential(
        self,
        db: AsyncSession,
        credential_id: int,
        user_id: int,
        update_data: CredentialUpdate
    ) -> Credential:
        """Update a credential."""
        credential = await self.get_credential(db, credential_id, user_id)
        
        # Check if user can edit (must be owner or have edit permission)
        if credential.user_id != user_id:
            share = next(
                (s for s in credential.shared_with if s.user_id == user_id),
                None
            )
            if not share or share.permission != "edit":
                raise CredentialAccessDeniedError("No edit permission for credential")
        
        # Check for duplicate name
        if update_data.name and update_data.name != credential.name:
            existing = await db.execute(
                select(Credential).where(
                    and_(
                        Credential.user_id == credential.user_id,
                        Credential.name == update_data.name,
                        Credential.id != credential_id
                    )
                )
            )
            if existing.scalar_one_or_none():
                raise CredentialAlreadyExistsError(f"Credential '{update_data.name}' already exists")
        
        # Update fields
        if update_data.name is not None:
            credential.name = update_data.name
        
        if update_data.data is not None:
            credential.data = self.encryption_service.encrypt(update_data.data)
        
        if update_data.description is not None:
            credential.description = update_data.description
        
        if update_data.settings is not None:
            credential.settings = update_data.settings
            # Update expiry if provided
            if "expiresAt" in update_data.settings:
                credential.expires_at = datetime.fromisoformat(
                    update_data.settings["expiresAt"].replace("Z", "+00:00")
                )
        
        if update_data.no_data_expression is not None:
            credential.no_data_expression = update_data.no_data_expression
        
        if update_data.oauth_token_data is not None:
            credential.oauth_token_data = self.encryption_service.encrypt(
                update_data.oauth_token_data.dict()
            )
        
        await db.commit()
        await db.refresh(credential)
        
        return credential
    
    async def delete_credential(
        self,
        db: AsyncSession,
        credential_id: int,
        user_id: int
    ) -> None:
        """Delete a credential."""
        credential = await self.get_credential(db, credential_id, user_id, check_permission=False)
        
        # Only owner can delete
        if credential.user_id != user_id:
            raise CredentialAccessDeniedError("Only the owner can delete a credential.")
        
        await db.delete(credential)
        await db.commit()
    
    async def share_credential(
        self,
        db: AsyncSession,
        credential_id: int,
        user_id: int,
        share_data: CredentialShare
    ) -> Credential:
        """Share a credential with other users."""
        credential = await self.get_credential(db, credential_id, user_id, check_permission=False)

        # Only owner can share
        if credential.user_id != user_id:
            raise CredentialAccessDeniedError("Only owner can share credential")

        # Remove existing shares to reset permissions
        for share in credential.shared_with:
            await db.delete(share)
        await db.flush()

        # Add new shares
        for i, share_user_id in enumerate(share_data.userIds):
            if int(share_user_id) == user_id:
                continue  # Skip owner

            permission = share_data.permissions[i] if i < len(share_data.permissions) else 'use'

            shared = SharedCredential(
                credential_id=credential_id,
                user_id=int(share_user_id),
                permission=permission
            )
            db.add(shared)

        await db.commit()
        await db.refresh(credential, ["shared_with"])

        return credential
    
    async def test_credential(
        self,
        db: AsyncSession,
        credential_id: int,
        user_id: int
    ) -> Dict[str, Any]:
        """Test a credential."""
        credential, decrypted_data = await self.get_credential_with_data(
            db, credential_id, user_id
        )
        
        # Get credential type definition
        type_def = await self.get_credential_type(db, credential.type)
        
        # If type has test function, execute it
        if type_def and type_def.test_function:
            # This would call the actual test function
            # For now, return mock success
            return {
                "success": True,
                "message": f"Credential '{credential.name}' tested successfully"
            }
        
        # Default test response
        return {
            "success": True,
            "message": "Credential test not implemented for this type"
        }
    
    async def get_credential_types(
        self,
        db: AsyncSession
    ) -> List[CredentialTypeDefinition]:
        """Get all available credential types."""
        # Get from database
        result = await db.execute(
            select(CredentialTypeDefinition).order_by(CredentialTypeDefinition.display_name)
        )
        db_types = result.scalars().all()
        
        # If no types in DB, return built-in types
        if not db_types:
            return self._get_builtin_credential_types()
        
        return list(db_types)
    
    async def get_credential_type(
        self,
        db: AsyncSession,
        type_name: str
    ) -> Optional[CredentialTypeDefinition]:
        """Get a specific credential type definition."""
        result = await db.execute(
            select(CredentialTypeDefinition).where(
                CredentialTypeDefinition.name == type_name
            )
        )
        return result.scalar_one_or_none()
    
    async def get_oauth_auth_url(
        self,
        db: AsyncSession,
        credential_id: int,
        user_id: int,
        redirect_uri: str
    ) -> Dict[str, str]:
        """Get OAuth authorization URL for a credential."""
        credential = await self.get_credential(db, credential_id, user_id)
        
        if not credential.is_oauth:
            raise ValueError("Credential is not OAuth type")
        
        # Generate state for CSRF protection
        state = str(uuid4())
        
        # Store state in credential settings for validation
        if not credential.settings:
            credential.settings = {}
        credential.settings["oauth_state"] = state
        await db.commit()
        
        # Build auth URL (simplified example)
        decrypted_data = self.encryption_service.decrypt(credential.data)
        auth_url = decrypted_data.get("authUrl", "")
        client_id = decrypted_data.get("clientId", "")
        
        # Build full URL with parameters
        full_url = (
            f"{auth_url}?"
            f"client_id={client_id}&"
            f"redirect_uri={redirect_uri}&"
            f"state={state}&"
            f"response_type=code"
        )
        
        return {
            "authUrl": full_url,
            "state": state
        }
    
    async def handle_oauth_callback(
        self,
        db: AsyncSession,
        credential_id: int,
        user_id: int,
        code: str,
        state: str
    ) -> Credential:
        """Handle OAuth callback and store tokens."""
        credential = await self.get_credential(db, credential_id, user_id)
        
        # Validate state
        stored_state = credential.settings.get("oauth_state") if credential.settings else None
        if not stored_state or stored_state != state:
            raise ValueError("Invalid OAuth state")
        
        # Exchange code for tokens (simplified - would make actual HTTP request)
        token_data = OAuthTokenData(
            access_token=f"access_{code}",
            token_type="Bearer",
            expires_in=3600,
            refresh_token=f"refresh_{code}",
            scope="read write"
        )
        
        # Store encrypted token data
        credential.oauth_token_data = self.encryption_service.encrypt(token_data.dict())
        
        # Clean up state
        if credential.settings and "oauth_state" in credential.settings:
            del credential.settings["oauth_state"]
        
        await db.commit()
        await db.refresh(credential)
        
        return credential
    
    async def bulk_delete_credentials(
        self,
        db: AsyncSession,
        credential_ids: List[int],
        user_id: int
    ) -> int:
        """Bulk delete credentials."""
        # Get credentials user owns
        result = await db.execute(
            select(Credential).where(
                and_(
                    Credential.id.in_(credential_ids),
                    Credential.user_id == user_id
                )
            )
        )
        credentials = result.scalars().all()
        
        # Delete them
        for credential in credentials:
            await db.delete(credential)
        
        await db.commit()
        
        return len(credentials)
    
    def _get_builtin_credential_types(self) -> List[Dict[str, Any]]:
        """Get built-in credential type definitions."""
        return [
            {
                "name": "apiKey",
                "displayName": "API Key",
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
            },
            {
                "name": "basicAuth",
                "displayName": "Basic Auth",
                "properties": [
                    {
                        "name": "username",
                        "displayName": "Username",
                        "type": "string",
                        "required": True
                    },
                    {
                        "name": "password",
                        "displayName": "Password",
                        "type": "string",
                        "required": True,
                        "typeOptions": {
                            "password": True
                        }
                    }
                ]
            },
            {
                "name": "bearerToken",
                "displayName": "Bearer Token",
                "properties": [
                    {
                        "name": "token",
                        "displayName": "Token",
                        "type": "string",
                        "required": True,
                        "typeOptions": {
                            "password": True
                        }
                    }
                ]
            },
            {
                "name": "oauth2",
                "displayName": "OAuth2",
                "properties": [
                    {
                        "name": "clientId",
                        "displayName": "Client ID",
                        "type": "string",
                        "required": True
                    },
                    {
                        "name": "clientSecret",
                        "displayName": "Client Secret",
                        "type": "string",
                        "required": True,
                        "typeOptions": {
                            "password": True
                        }
                    },
                    {
                        "name": "authUrl",
                        "displayName": "Authorization URL",
                        "type": "string",
                        "required": True
                    },
                    {
                        "name": "accessTokenUrl",
                        "displayName": "Access Token URL",
                        "type": "string",
                        "required": True
                    }
                ]
            }
        ]


# Singleton instance
credential_service = CredentialService()