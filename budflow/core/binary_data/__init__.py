"""Binary data management system."""

from typing import Any, Dict, Optional, AsyncIterator, List
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass
class BinaryDataMetadata:
    """Metadata for binary data."""
    id: UUID
    filename: str
    content_type: str
    size: int
    checksum: str
    created_at: datetime
    expires_at: Optional[datetime]
    metadata: Dict[str, Any]
    storage_path: str
    is_compressed: bool
    is_encrypted: bool


@dataclass
class BinaryDataConfig:
    """Configuration for binary data storage."""
    backend: str  # 'filesystem' or 's3'
    filesystem_path: Optional[str] = None
    s3_bucket: Optional[str] = None
    s3_region: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    allowed_mime_types: Optional[List[str]] = None
    ttl_days: Optional[int] = None
    enable_compression: bool = True
    enable_encryption: bool = True
    encryption_key: Optional[str] = None

    def copy(self, **kwargs) -> 'BinaryDataConfig':
        """Create a copy with updated values."""
        import copy
        new_config = copy.deepcopy(self)
        for key, value in kwargs.items():
            if hasattr(new_config, key):
                setattr(new_config, key, value)
        return new_config


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    async def store(self, data: bytes, metadata: BinaryDataMetadata) -> str:
        """Store binary data and return storage path."""
        pass
    
    @abstractmethod
    async def retrieve(self, path: str) -> bytes:
        """Retrieve binary data from storage."""
        pass
    
    @abstractmethod
    async def delete(self, path: str) -> None:
        """Delete binary data from storage."""
        pass
    
    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Check if binary data exists."""
        pass
    
    @abstractmethod
    async def stream(self, path: str, chunk_size: int = 8192) -> AsyncIterator[bytes]:
        """Stream binary data in chunks."""
        pass
    
    @abstractmethod
    async def get_url(self, path: str, expires_in: int = 3600) -> str:
        """Get a URL for accessing the binary data."""
        pass


class BinaryDataError(Exception):
    """Base exception for binary data errors."""
    pass


class BinaryDataNotFoundError(BinaryDataError):
    """Raised when binary data is not found."""
    pass


class BinaryDataStorageError(BinaryDataError):
    """Raised when storage operations fail."""
    pass


# Additional classes for compatibility
from dataclasses import dataclass
from enum import Enum


class BinaryDataMode(Enum):
    """Binary data storage mode."""
    FILESYSTEM = "filesystem"
    S3 = "s3"
    DATABASE = "database"


@dataclass
class BinaryDataMeta:
    """Simplified metadata for binary data."""
    id: str
    size: int
    mime_type: str
    file_name: Optional[str] = None


@dataclass
class BinaryDataReference:
    """Reference to binary data in workflow context."""
    mode: BinaryDataMode
    id: str
    meta: BinaryDataMeta


# Import implementations
from .filesystem import FileSystemBackend
from .s3 import S3Backend
from .manager import BinaryDataManager


# Alias for compatibility
BinaryDataService = BinaryDataManager


__all__ = [
    'BinaryDataMetadata',
    'BinaryDataConfig',
    'StorageBackend',
    'BinaryDataError',
    'BinaryDataNotFoundError',
    'BinaryDataStorageError',
    'FileSystemBackend',
    'S3Backend',
    'BinaryDataManager',
    'BinaryDataService',
    'BinaryDataMode',
    'BinaryDataMeta',
    'BinaryDataReference',
]