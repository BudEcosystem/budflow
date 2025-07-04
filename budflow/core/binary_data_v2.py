"""Binary data handling system for BudFlow."""

import asyncio
import gzip
import hashlib
import io
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple, Union
from uuid import uuid4, UUID

import aiofiles
import boto3
from botocore.exceptions import ClientError
from cryptography.fernet import Fernet
from pydantic import BaseModel, Field, ConfigDict, field_validator
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import Base
from sqlalchemy import Column, String, Integer, DateTime, Boolean, JSON
from sqlalchemy.dialects.postgresql import UUID


class BinaryDataMode(str, Enum):
    """Binary data storage modes."""
    
    FILESYSTEM = "filesystem"
    S3 = "s3"
    DISABLED = "disabled"


class BinaryDataConfig(BaseModel):
    """Configuration for binary data handling."""
    
    mode: BinaryDataMode = BinaryDataMode.FILESYSTEM
    filesystem_path: str = "/data/binary"
    s3_bucket: str = "budflow-binary"
    s3_region: str = "us-east-1"
    s3_endpoint: Optional[str] = None  # For S3-compatible services
    max_size_mb: int = 250
    ttl_minutes: int = 60
    enable_compression: bool = False
    enable_encryption: bool = False
    encryption_key: Optional[str] = None
    chunk_size: int = 1024 * 1024  # 1MB chunks
    
    @field_validator("encryption_key")
    def validate_encryption_key(cls, v, info):
        if info.data.get("enable_encryption") and not v:
            raise ValueError("Encryption key required when encryption is enabled")
        return v


class BinaryDataMeta(BaseModel):
    """Binary data metadata for passing around."""
    
    id: Optional[str] = None
    filename: str
    mime_type: str
    size: int
    checksum: str = ""
    storage_path: Optional[str] = None
    storage_mode: str = BinaryDataMode.FILESYSTEM
    is_compressed: bool = False
    is_encrypted: bool = False
    created_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    workflow_id: Optional[str] = None
    execution_id: Optional[str] = None
    node_id: Optional[str] = None
    extra_metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """Check if the data has expired."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) > self.expires_at
    
    @classmethod
    def from_db_model(cls, db_model: "BinaryDataMetadata") -> "BinaryDataMeta":
        """Create BinaryDataMeta from database model."""
        return cls(
            id=str(db_model.id) if db_model.id else None,
            filename=db_model.filename,
            mime_type=db_model.mime_type,
            size=db_model.size,
            checksum=db_model.checksum,
            storage_path=db_model.storage_path,
            storage_mode=db_model.storage_mode,
            is_compressed=db_model.is_compressed,
            is_encrypted=db_model.is_encrypted,
            created_at=db_model.created_at,
            expires_at=db_model.expires_at,
            workflow_id=str(db_model.workflow_id) if db_model.workflow_id else None,
            execution_id=str(db_model.execution_id) if db_model.execution_id else None,
            node_id=db_model.node_id,
            extra_metadata=db_model.extra_metadata or {},
        )
    
    model_config = ConfigDict(from_attributes=True)


class BinaryDataMetadata(Base):
    """Binary data metadata stored in database."""
    
    __tablename__ = "binary_data"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    filename = Column(String(255), nullable=False)
    mime_type = Column(String(100), nullable=False)
    size = Column(Integer, nullable=False)
    checksum = Column(String(64), nullable=False)
    storage_path = Column(String(500))
    storage_mode = Column(String(20), nullable=False)
    is_compressed = Column(Boolean, default=False)
    is_encrypted = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True))
    workflow_id = Column(UUID(as_uuid=True))
    execution_id = Column(UUID(as_uuid=True))
    node_id = Column(String(100))
    extra_metadata = Column(JSON, default=dict)
    
    def is_expired(self) -> bool:
        """Check if the data has expired."""
        if not self.expires_at:
            return False
        return datetime.now(timezone.utc) > self.expires_at


class BinaryDataReference(BaseModel):
    """Reference to binary data passed between nodes."""
    
    id: str
    url: Optional[str] = None
    mode: BinaryDataMode
    metadata: Optional[Dict[str, Any]] = None
    
    model_config = ConfigDict(from_attributes=True)


class BinaryDataError(Exception):
    """Base exception for binary data errors."""
    pass


class BinaryDataNotFoundError(BinaryDataError):
    """Binary data not found error."""
    pass


class BinaryDataStorageError(BinaryDataError):
    """Binary data storage error."""
    pass


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    def __init__(self, config: BinaryDataConfig):
        self.config = config
    
    @abstractmethod
    async def store(self, data_id: str, data: bytes) -> str:
        """Store binary data and return storage path/key."""
        pass
    
    @abstractmethod
    async def retrieve(self, data_id: str) -> bytes:
        """Retrieve binary data."""
        pass
    
    @abstractmethod
    async def delete(self, data_id: str) -> bool:
        """Delete binary data."""
        pass
    
    @abstractmethod
    async def exists(self, data_id: str) -> bool:
        """Check if binary data exists."""
        pass
    
    @abstractmethod
    async def store_stream(
        self, data_id: str, stream: Union[io.BytesIO, AsyncIterator[bytes]]
    ) -> str:
        """Store binary data from stream."""
        pass
    
    @abstractmethod
    async def retrieve_stream(self, data_id: str) -> AsyncIterator[bytes]:
        """Retrieve binary data as stream."""
        pass


class FileSystemBackend(StorageBackend):
    """File system storage backend."""
    
    def _get_path(self, data_id: str) -> Path:
        """Get file path for data ID."""
        # Create subdirectories based on ID prefix for better organization
        prefix = data_id[:2]
        path = Path(self.config.filesystem_path) / prefix / data_id
        return path
    
    async def store(self, data_id: str, data: bytes) -> str:
        """Store binary data to filesystem."""
        path = self._get_path(data_id)
        
        # Create directories
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write data
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)
        
        return str(path)
    
    async def retrieve(self, data_id: str) -> bytes:
        """Retrieve binary data from filesystem."""
        path = self._get_path(data_id)
        
        if not path.exists():
            raise BinaryDataNotFoundError(f"Data {data_id} not found")
        
        async with aiofiles.open(path, "rb") as f:
            return await f.read()
    
    async def delete(self, data_id: str) -> bool:
        """Delete binary data from filesystem."""
        path = self._get_path(data_id)
        
        if path.exists():
            path.unlink()
            
            # Try to remove empty parent directory
            try:
                path.parent.rmdir()
            except OSError:
                pass  # Directory not empty
            
            return True
        
        return False
    
    async def exists(self, data_id: str) -> bool:
        """Check if binary data exists."""
        path = self._get_path(data_id)
        return path.exists()
    
    async def store_stream(
        self, data_id: str, stream: Union[io.BytesIO, AsyncIterator[bytes]]
    ) -> str:
        """Store binary data from stream."""
        path = self._get_path(data_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        async with aiofiles.open(path, "wb") as f:
            if isinstance(stream, io.BytesIO):
                # Handle BytesIO
                stream.seek(0)
                await f.write(stream.read())
            else:
                # Handle async iterator
                async for chunk in stream:
                    await f.write(chunk)
        
        return str(path)
    
    async def retrieve_stream(self, data_id: str) -> AsyncIterator[bytes]:
        """Retrieve binary data as stream."""
        path = self._get_path(data_id)
        
        if not path.exists():
            raise BinaryDataNotFoundError(f"Data {data_id} not found")
        
        async with aiofiles.open(path, "rb") as f:
            while chunk := await f.read(self.config.chunk_size):
                yield chunk


class S3Backend(StorageBackend):
    """S3 storage backend."""
    
    def __init__(self, config: BinaryDataConfig):
        super().__init__(config)
        self.s3_client = boto3.client(
            "s3",
            region_name=config.s3_region,
            endpoint_url=config.s3_endpoint,
        )
    
    def _get_key(self, data_id: str) -> str:
        """Get S3 key for data ID."""
        prefix = data_id[:2]
        return f"binary-data/{prefix}/{data_id}"
    
    async def store(self, data_id: str, data: bytes) -> str:
        """Store binary data to S3."""
        key = self._get_key(data_id)
        
        try:
            # Run in executor to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.s3_client.put_object(
                    Bucket=self.config.s3_bucket,
                    Key=key,
                    Body=data,
                )
            )
            return key
        except ClientError as e:
            raise BinaryDataStorageError(f"Failed to store data: {e}")
    
    async def retrieve(self, data_id: str) -> bytes:
        """Retrieve binary data from S3."""
        key = self._get_key(data_id)
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.s3_client.get_object(
                    Bucket=self.config.s3_bucket,
                    Key=key,
                )
            )
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise BinaryDataNotFoundError(f"Data {data_id} not found")
            raise BinaryDataStorageError(f"Failed to retrieve data: {e}")
    
    async def delete(self, data_id: str) -> bool:
        """Delete binary data from S3."""
        key = self._get_key(data_id)
        
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.s3_client.delete_object(
                    Bucket=self.config.s3_bucket,
                    Key=key,
                )
            )
            return True
        except ClientError:
            return False
    
    async def exists(self, data_id: str) -> bool:
        """Check if binary data exists in S3."""
        key = self._get_key(data_id)
        
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.s3_client.head_object(
                    Bucket=self.config.s3_bucket,
                    Key=key,
                )
            )
            return True
        except ClientError:
            return False
    
    async def store_stream(
        self, data_id: str, stream: Union[io.BytesIO, AsyncIterator[bytes]]
    ) -> str:
        """Store binary data from stream using multipart upload."""
        # For simplicity, collect stream into memory
        # In production, use multipart upload for large files
        if isinstance(stream, io.BytesIO):
            stream.seek(0)
            data = stream.read()
        else:
            chunks = []
            async for chunk in stream:
                chunks.append(chunk)
            data = b"".join(chunks)
        
        return await self.store(data_id, data)
    
    async def retrieve_stream(self, data_id: str) -> AsyncIterator[bytes]:
        """Retrieve binary data as stream from S3."""
        # For simplicity, retrieve all data and yield in chunks
        # In production, use S3 range requests for true streaming
        data = await self.retrieve(data_id)
        
        for i in range(0, len(data), self.config.chunk_size):
            yield data[i:i + self.config.chunk_size]
    
    async def generate_signed_url(
        self, data_id: str, expires_in: int = 3600
    ) -> str:
        """Generate signed URL for direct S3 access."""
        key = self._get_key(data_id)
        
        try:
            loop = asyncio.get_event_loop()
            url = await loop.run_in_executor(
                None,
                self.s3_client.generate_presigned_url,
                "get_object",
                {"Bucket": self.config.s3_bucket, "Key": key},
                expires_in,
            )
            return url
        except ClientError as e:
            raise BinaryDataStorageError(f"Failed to generate URL: {e}")
    
    async def initiate_multipart_upload(self, data_id: str) -> str:
        """Initiate multipart upload."""
        key = self._get_key(data_id)
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.s3_client.create_multipart_upload(
                    Bucket=self.config.s3_bucket,
                    Key=key,
                )
            )
            return response["UploadId"]
        except ClientError as e:
            raise BinaryDataStorageError(f"Failed to initiate upload: {e}")
    
    async def upload_part(
        self, data_id: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        """Upload a part in multipart upload."""
        key = self._get_key(data_id)
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.s3_client.upload_part(
                    Bucket=self.config.s3_bucket,
                    Key=key,
                    Body=data,
                    PartNumber=part_number,
                    UploadId=upload_id,
                )
            )
            return response["ETag"]
        except ClientError as e:
            raise BinaryDataStorageError(f"Failed to upload part: {e}")
    
    async def complete_multipart_upload(
        self, data_id: str, upload_id: str, parts: List[Dict[str, Any]]
    ) -> None:
        """Complete multipart upload."""
        key = self._get_key(data_id)
        
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.s3_client.complete_multipart_upload(
                    Bucket=self.config.s3_bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
            )
        except ClientError as e:
            raise BinaryDataStorageError(f"Failed to complete upload: {e}")


class BinaryDataService:
    """Main service for binary data handling."""
    
    def __init__(self, config: BinaryDataConfig, db: Optional[AsyncSession] = None):
        self.config = config
        self.db = db
        self._backend = self._create_backend()
        self._cipher = self._create_cipher() if config.enable_encryption else None
    
    def _create_backend(self) -> StorageBackend:
        """Create storage backend based on configuration."""
        if self.config.mode == BinaryDataMode.FILESYSTEM:
            return FileSystemBackend(self.config)
        elif self.config.mode == BinaryDataMode.S3:
            return S3Backend(self.config)
        else:
            raise ValueError(f"Unsupported storage mode: {self.config.mode}")
    
    def _create_cipher(self) -> Optional[Fernet]:
        """Create encryption cipher."""
        if not self.config.encryption_key:
            return None
        
        # Ensure key is 32 bytes (base64 encoded)
        key = self.config.encryption_key.encode()
        if len(key) < 32:
            key = key.ljust(32, b"0")
        elif len(key) > 32:
            key = key[:32]
        
        import base64
        key_b64 = base64.urlsafe_b64encode(key)
        return Fernet(key_b64)
    
    def _calculate_checksum(self, data: bytes) -> str:
        """Calculate SHA256 checksum of data."""
        return hashlib.sha256(data).hexdigest()
    
    def _compress_data(self, data: bytes) -> bytes:
        """Compress data using gzip."""
        return gzip.compress(data)
    
    def _decompress_data(self, data: bytes) -> bytes:
        """Decompress gzip data."""
        return gzip.decompress(data)
    
    def _encrypt_data(self, data: bytes) -> bytes:
        """Encrypt data."""
        if not self._cipher:
            return data
        return self._cipher.encrypt(data)
    
    def _decrypt_data(self, data: bytes) -> bytes:
        """Decrypt data."""
        if not self._cipher:
            return data
        return self._cipher.decrypt(data)
    
    async def store(
        self,
        data: bytes,
        metadata: BinaryDataMeta,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> BinaryDataReference:
        """Store binary data."""
        # Check size limit
        if len(data) > self.config.max_size_mb * 1024 * 1024:
            raise BinaryDataStorageError(
                f"Data size {len(data)} exceeds maximum {self.config.max_size_mb}MB"
            )
        
        # Generate ID and calculate checksum
        data_id = metadata.id or str(uuid4())
        checksum = self._calculate_checksum(data)
        
        # Process data
        processed_data = data
        is_compressed = False
        is_encrypted = False
        
        # Compress if enabled
        if self.config.enable_compression:
            processed_data = self._compress_data(processed_data)
            is_compressed = True
        
        # Encrypt if enabled
        if self.config.enable_encryption:
            processed_data = self._encrypt_data(processed_data)
            is_encrypted = True
        
        # Set expiration
        expires_at = metadata.expires_at
        if not expires_at and self.config.ttl_minutes:
            expires_at = datetime.now(timezone.utc) + timedelta(
                minutes=self.config.ttl_minutes
            )
        
        # Store data
        storage_path = await self._backend.store(str(data_id), processed_data)
        
        # Save metadata to database
        if self.db:
            db_metadata = BinaryDataMetadata(
                id=UUID(data_id),
                filename=metadata.filename,
                mime_type=metadata.mime_type,
                size=metadata.size,
                checksum=checksum,
                storage_path=storage_path,
                storage_mode=self.config.mode.value,
                is_compressed=is_compressed,
                is_encrypted=is_encrypted,
                created_at=metadata.created_at or datetime.now(timezone.utc),
                expires_at=expires_at,
                workflow_id=UUID(metadata.workflow_id) if metadata.workflow_id else None,
                execution_id=UUID(metadata.execution_id) if metadata.execution_id else None,
                node_id=metadata.node_id,
                extra_metadata=metadata.extra_metadata,
            )
            self.db.add(db_metadata)
            await self.db.commit()
        
        # Progress callback
        if progress_callback:
            progress_callback(len(data), len(data))
        
        # Create reference
        reference = BinaryDataReference(
            id=data_id,
            mode=self.config.mode,
            metadata={"filename": metadata.filename, "mime_type": metadata.mime_type},
        )
        
        # Add URL for S3
        if self.config.mode == BinaryDataMode.S3 and hasattr(self._backend, "generate_signed_url"):
            reference.url = await self._backend.generate_signed_url(data_id)
        
        return reference
    
    async def retrieve(
        self, data_id: str
    ) -> Tuple[bytes, BinaryDataMeta]:
        """Retrieve binary data and metadata."""
        # Get metadata
        db_metadata = None
        if self.db:
            result = await self.db.execute(
                select(BinaryDataMetadata).where(BinaryDataMetadata.id == data_id)
            )
            db_metadata = result.scalar_one_or_none()
        
        if not db_metadata:
            raise BinaryDataNotFoundError(f"Metadata for {data_id} not found")
        
        # Check expiration
        if db_metadata.is_expired():
            raise BinaryDataNotFoundError(f"Data {data_id} has expired")
        
        # Retrieve data
        data = await self._backend.retrieve(data_id)
        
        # Decrypt if needed
        if db_metadata.is_encrypted:
            data = self._decrypt_data(data)
        
        # Decompress if needed
        if db_metadata.is_compressed:
            data = self._decompress_data(data)
        
        # Verify checksum
        checksum = self._calculate_checksum(data)
        if checksum != db_metadata.checksum:
            raise BinaryDataError(f"Checksum mismatch for {data_id}")
        
        # Convert to BinaryDataMeta
        metadata = BinaryDataMeta.from_db_model(db_metadata)
        
        return data, metadata
    
    async def delete(self, data_id: str) -> bool:
        """Delete binary data and metadata."""
        # Delete from storage
        deleted = await self._backend.delete(data_id)
        
        # Delete metadata
        if self.db and deleted:
            await self.db.execute(
                BinaryDataMetadata.__table__.delete().where(
                    BinaryDataMetadata.id == data_id
                )
            )
            await self.db.commit()
        
        return deleted
    
    async def exists(self, data_id: str) -> bool:
        """Check if binary data exists."""
        return await self._backend.exists(data_id)
    
    async def store_stream(
        self,
        stream: Union[io.BytesIO, AsyncIterator[bytes]],
        metadata: BinaryDataMeta,
    ) -> BinaryDataReference:
        """Store binary data from stream."""
        # For now, collect stream and use regular store
        # In production, implement true streaming with chunked processing
        if isinstance(stream, io.BytesIO):
            stream.seek(0)
            data = stream.read()
        else:
            chunks = []
            async for chunk in stream:
                chunks.append(chunk)
            data = b"".join(chunks)
        
        return await self.store(data, metadata)
    
    async def retrieve_stream(self, data_id: str) -> AsyncIterator[bytes]:
        """Retrieve binary data as stream."""
        # Get metadata first to ensure data exists
        await self.retrieve(data_id)
        
        # Stream from backend
        async for chunk in self._backend.retrieve_stream(data_id):
            # Note: In production, handle decryption/decompression in streaming mode
            yield chunk
    
    async def cleanup_expired(self) -> int:
        """Clean up expired binary data."""
        if not self.db:
            return 0
        
        # Find expired entries
        result = await self.db.execute(
            select(BinaryDataMetadata).where(
                BinaryDataMetadata.expires_at < datetime.now(timezone.utc)
            )
        )
        
        expired = result.scalars().all()
        count = 0
        
        for metadata in expired:
            if await self.delete(str(metadata.id)):
                count += 1
        
        return count
    
    async def find_by_workflow(self, workflow_id: str) -> List[BinaryDataMeta]:
        """Find all binary data for a workflow."""
        if not self.db:
            return []
        
        result = await self.db.execute(
            select(BinaryDataMetadata).where(
                BinaryDataMetadata.workflow_id == workflow_id
            )
        )
        
        db_items = result.scalars().all()
        return [BinaryDataMeta.from_db_model(item) for item in db_items]
    
    async def find_by_execution(self, execution_id: str) -> List[BinaryDataMeta]:
        """Find all binary data for an execution."""
        if not self.db:
            return []
        
        result = await self.db.execute(
            select(BinaryDataMetadata).where(
                BinaryDataMetadata.execution_id == execution_id
            )
        )
        
        db_items = result.scalars().all()
        return [BinaryDataMeta.from_db_model(item) for item in db_items]
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        if not self.db:
            return {}
        
        # Get total count and size
        result = await self.db.execute(
            select(
                func.count(BinaryDataMetadata.id).label("count"),
                func.sum(BinaryDataMetadata.size).label("total_size"),
            )
        )
        
        stats = result.one()
        
        return {
            "total_files": stats.count or 0,
            "total_size_bytes": stats.total_size or 0,
            "total_size_mb": (stats.total_size or 0) / (1024 * 1024),
            "storage_mode": self.config.mode.value,
        }