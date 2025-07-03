"""Binary data manager for handling storage operations."""

import hashlib
import gzip
import io
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4
from pathlib import Path
import fnmatch
import structlog

from budflow.credentials.encryption import CredentialEncryption
from budflow.config import settings

from . import (
    BinaryDataConfig,
    BinaryDataMetadata,
    BinaryDataError,
    BinaryDataNotFoundError,
    StorageBackend,
    FileSystemBackend,
    S3Backend
)

logger = structlog.get_logger()


class BinaryDataManager:
    """Manages binary data storage with compression and encryption."""
    
    def __init__(self, config: BinaryDataConfig):
        """Initialize binary data manager."""
        self.config = config
        
        # Initialize storage backend with credentials from settings
        if config.backend == "filesystem":
            self.backend = FileSystemBackend(config)
        elif config.backend == "s3":
            from budflow.config import settings
            # Update config with settings values
            s3_config = config.copy(
                s3_bucket=config.s3_bucket or settings.s3_bucket,
                s3_region=config.s3_region or settings.s3_region,
                s3_endpoint=config.s3_endpoint or settings.s3_endpoint_url,
                s3_access_key_id=config.s3_access_key_id or settings.s3_access_key_id,
                s3_secret_access_key=config.s3_secret_access_key or settings.s3_secret_access_key
            )
            self.backend = S3Backend(s3_config)
        else:
            raise BinaryDataError(f"Unknown backend: {config.backend}")
        
        # Initialize encryption if enabled
        self.encryption = None
        if config.enable_encryption:
            encryption_key = config.encryption_key or getattr(settings, 'encryption_key', None)
            if not encryption_key:
                # Generate a default test key for development
                import secrets
                import base64
                encryption_key = base64.b64encode(secrets.token_bytes(32)).decode('utf-8')
            self.encryption = CredentialEncryption(encryption_key)
        
        # In-memory metadata cache (production would use Redis/DB)
        self._metadata_cache: Dict[UUID, BinaryDataMetadata] = {}
        
        logger.info(
            "Initialized binary data manager",
            backend=config.backend,
            compression=config.enable_compression,
            encryption=config.enable_encryption
        )
    
    async def store(
        self,
        data: bytes,
        filename: str,
        content_type: str,
        metadata: Optional[Dict[str, Any]] = None,
        ttl_days: Optional[int] = None
    ) -> UUID:
        """Store binary data with optional compression and encryption."""
        try:
            # Validate file size
            if len(data) > self.config.max_file_size:
                raise BinaryDataError(
                    f"File size {len(data)} exceeds maximum size {self.config.max_file_size}"
                )
            
            # Validate MIME type
            if self.config.allowed_mime_types:
                if not self._is_mime_type_allowed(content_type):
                    raise BinaryDataError(
                        f"MIME type '{content_type}' is not allowed"
                    )
            
            # Calculate checksum of original data
            checksum = hashlib.sha256(data).hexdigest()
            
            # Compress if enabled
            is_compressed = False
            if self.config.enable_compression:
                compressed_data = gzip.compress(data)
                # Only use compression if it reduces size
                if len(compressed_data) < len(data):
                    data = compressed_data
                    is_compressed = True
            
            # Encrypt if enabled
            is_encrypted = False
            if self.encryption:
                data = await self._encrypt_data(data)
                is_encrypted = True
            
            # Generate metadata
            binary_id = uuid4()
            expires_at = None
            if ttl_days is not None:
                expires_at = datetime.now(timezone.utc) + timedelta(days=ttl_days)
            elif self.config.ttl_days:
                expires_at = datetime.now(timezone.utc) + timedelta(days=self.config.ttl_days)
            
            binary_metadata = BinaryDataMetadata(
                id=binary_id,
                filename=filename,
                content_type=content_type,
                size=len(data),
                checksum=checksum,
                created_at=datetime.now(timezone.utc),
                expires_at=expires_at,
                metadata=metadata or {},
                storage_path="",  # Will be set after storage
                is_compressed=is_compressed,
                is_encrypted=is_encrypted
            )
            
            # Store data
            storage_path = await self.backend.store(data, binary_metadata)
            binary_metadata.storage_path = storage_path
            
            # Cache metadata
            self._metadata_cache[binary_id] = binary_metadata
            
            logger.info(
                "Stored binary data",
                id=binary_id,
                filename=filename,
                size=len(data),
                compressed=is_compressed,
                encrypted=is_encrypted
            )
            
            return binary_id
            
        except Exception as e:
            logger.error(
                "Failed to store binary data",
                error=str(e),
                filename=filename
            )
            raise
    
    async def retrieve(self, binary_id: UUID) -> bytes:
        """Retrieve binary data by ID."""
        try:
            # Get metadata
            metadata = await self.get_metadata(binary_id)
            if not metadata:
                raise BinaryDataNotFoundError(f"Binary data {binary_id} not found")
            
            # Check if expired
            if metadata.expires_at and metadata.expires_at < datetime.now(timezone.utc):
                raise BinaryDataNotFoundError(f"Binary data {binary_id} has expired")
            
            # Retrieve data
            data = await self.backend.retrieve(metadata.storage_path)
            
            # Decrypt if encrypted
            if metadata.is_encrypted and self.encryption:
                data = await self._decrypt_data(data)
            
            # Decompress if compressed
            if metadata.is_compressed:
                data = gzip.decompress(data)
            
            logger.debug(
                "Retrieved binary data",
                id=binary_id,
                size=len(data)
            )
            
            return data
            
        except Exception as e:
            logger.error(
                "Failed to retrieve binary data",
                error=str(e),
                id=binary_id
            )
            raise
    
    async def delete(self, binary_id: UUID) -> None:
        """Delete binary data by ID."""
        try:
            # Get metadata
            metadata = await self.get_metadata(binary_id)
            if not metadata:
                raise BinaryDataNotFoundError(f"Binary data {binary_id} not found")
            
            # Delete from backend
            await self.backend.delete(metadata.storage_path)
            
            # Remove from cache
            self._metadata_cache.pop(binary_id, None)
            
            logger.info("Deleted binary data", id=binary_id)
            
        except Exception as e:
            logger.error(
                "Failed to delete binary data",
                error=str(e),
                id=binary_id
            )
            raise
    
    async def get_metadata(self, binary_id: UUID) -> Optional[BinaryDataMetadata]:
        """Get metadata for binary data."""
        # Check cache first
        if binary_id in self._metadata_cache:
            return self._metadata_cache[binary_id]
        
        # In production, would query database
        return None
    
    async def get_signed_url(
        self,
        binary_id: UUID,
        expires_in: int = 3600
    ) -> str:
        """Get a signed URL for accessing binary data."""
        try:
            # Get metadata
            metadata = await self.get_metadata(binary_id)
            if not metadata:
                raise BinaryDataNotFoundError(f"Binary data {binary_id} not found")
            
            # Get URL from backend
            url = await self.backend.get_url(metadata.storage_path, expires_in)
            
            logger.debug(
                "Generated signed URL",
                id=binary_id,
                expires_in=expires_in
            )
            
            return url
            
        except Exception as e:
            logger.error(
                "Failed to generate signed URL",
                error=str(e),
                id=binary_id
            )
            raise
    
    async def cleanup_expired(self) -> int:
        """Clean up expired binary data."""
        try:
            now = datetime.now(timezone.utc)
            expired_ids = []
            
            # Find expired items
            for binary_id, metadata in self._metadata_cache.items():
                if metadata.expires_at and metadata.expires_at < now:
                    expired_ids.append(binary_id)
            
            # Delete expired items
            for binary_id in expired_ids:
                try:
                    await self.delete(binary_id)
                except Exception as e:
                    logger.error(
                        "Failed to delete expired binary data",
                        error=str(e),
                        id=binary_id
                    )
            
            logger.info(
                "Cleaned up expired binary data",
                count=len(expired_ids)
            )
            
            return len(expired_ids)
            
        except Exception as e:
            logger.error(
                "Failed to cleanup expired data",
                error=str(e)
            )
            return 0
    
    def _is_mime_type_allowed(self, content_type: str) -> bool:
        """Check if MIME type is allowed."""
        if not self.config.allowed_mime_types:
            return True
        
        for pattern in self.config.allowed_mime_types:
            if fnmatch.fnmatch(content_type, pattern):
                return True
        
        return False
    
    async def _encrypt_data(self, data: bytes) -> bytes:
        """Encrypt binary data."""
        if not self.encryption:
            return data
        
        # Convert to base64 string for encryption
        import base64
        data_str = base64.b64encode(data).decode('utf-8')
        
        # Encrypt (sync method)
        encrypted = self.encryption.encrypt(data_str)
        
        # Convert back to bytes
        return encrypted.encode('utf-8')
    
    async def _decrypt_data(self, data: bytes) -> bytes:
        """Decrypt binary data."""
        if not self.encryption:
            return data
        
        # Convert to string
        encrypted_str = data.decode('utf-8')
        
        # Decrypt (sync method)
        decrypted = self.encryption.decrypt(encrypted_str)
        
        # Convert from base64 back to bytes
        import base64
        return base64.b64decode(decrypted)