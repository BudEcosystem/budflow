"""File system storage backend for binary data."""

import os
import aiofiles
import aiofiles.os
from pathlib import Path
from typing import AsyncIterator, Optional
from datetime import datetime
from urllib.parse import quote
import structlog

from . import (
    StorageBackend,
    BinaryDataMetadata,
    BinaryDataConfig,
    BinaryDataError,
    BinaryDataNotFoundError
)

logger = structlog.get_logger()


class FileSystemBackend(StorageBackend):
    """File system storage backend implementation."""
    
    def __init__(self, config: BinaryDataConfig):
        """Initialize filesystem backend."""
        self.config = config
        self.base_path = Path(config.filesystem_path or "/tmp/budflow/binary_data")
        
        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            "Initialized filesystem backend",
            base_path=str(self.base_path)
        )
    
    async def store(self, data: bytes, metadata: BinaryDataMetadata) -> str:
        """Store binary data to filesystem."""
        try:
            # Generate storage path
            date_path = datetime.now().strftime("%Y/%m/%d")
            file_path = self.base_path / date_path / metadata.filename
            
            # Create directory
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write data
            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(data)
            
            storage_path = str(file_path)
            
            logger.debug(
                "Stored binary data",
                path=storage_path,
                size=len(data)
            )
            
            return storage_path
            
        except Exception as e:
            logger.error(
                "Failed to store binary data",
                error=str(e),
                filename=metadata.filename
            )
            raise BinaryDataError(f"Failed to store data: {str(e)}")
    
    async def retrieve(self, path: str) -> bytes:
        """Retrieve binary data from filesystem."""
        try:
            file_path = Path(path)
            
            if not file_path.exists():
                raise BinaryDataNotFoundError(f"File not found: {path}")
            
            async with aiofiles.open(file_path, 'rb') as f:
                data = await f.read()
            
            logger.debug(
                "Retrieved binary data",
                path=path,
                size=len(data)
            )
            
            return data
            
        except BinaryDataNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "Failed to retrieve binary data",
                error=str(e),
                path=path
            )
            raise BinaryDataError(f"Failed to retrieve data: {str(e)}")
    
    async def delete(self, path: str) -> None:
        """Delete binary data from filesystem."""
        try:
            file_path = Path(path)
            
            if file_path.exists():
                await aiofiles.os.remove(file_path)
                
                # Try to remove empty parent directories
                try:
                    parent = file_path.parent
                    while parent != self.base_path and not any(parent.iterdir()):
                        parent.rmdir()
                        parent = parent.parent
                except:
                    pass
                
                logger.debug("Deleted binary data", path=path)
            
        except Exception as e:
            logger.error(
                "Failed to delete binary data",
                error=str(e),
                path=path
            )
            raise BinaryDataError(f"Failed to delete data: {str(e)}")
    
    async def exists(self, path: str) -> bool:
        """Check if binary data exists."""
        try:
            return Path(path).exists()
        except Exception:
            return False
    
    async def stream(
        self,
        path: str,
        chunk_size: int = 8192
    ) -> AsyncIterator[bytes]:
        """Stream binary data in chunks."""
        try:
            file_path = Path(path)
            
            if not file_path.exists():
                raise BinaryDataNotFoundError(f"File not found: {path}")
            
            async with aiofiles.open(file_path, 'rb') as f:
                while chunk := await f.read(chunk_size):
                    yield chunk
            
        except BinaryDataNotFoundError:
            raise
        except Exception as e:
            logger.error(
                "Failed to stream binary data",
                error=str(e),
                path=path
            )
            raise BinaryDataError(f"Failed to stream data: {str(e)}")
    
    async def get_url(self, path: str, expires_in: int = 3600) -> str:
        """Get a file:// URL for the binary data."""
        file_path = Path(path)
        
        if not file_path.exists():
            raise BinaryDataNotFoundError(f"File not found: {path}")
        
        # Return file:// URL
        return f"file://{quote(str(file_path.absolute()))}"