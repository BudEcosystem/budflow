"""S3 storage backend for binary data."""

import boto3
from botocore.exceptions import ClientError
from typing import AsyncIterator, Optional
from datetime import datetime
import structlog
from urllib.parse import urlparse

from . import (
    StorageBackend,
    BinaryDataMetadata,
    BinaryDataConfig,
    BinaryDataError,
    BinaryDataNotFoundError
)

logger = structlog.get_logger()


class S3Backend(StorageBackend):
    """S3 storage backend implementation."""
    
    def __init__(self, config: BinaryDataConfig):
        """Initialize S3 backend."""
        self.config = config
        self.bucket = config.s3_bucket
        
        # Initialize S3 client with AWS credentials
        self.s3_client = boto3.client(
            's3',
            region_name=config.s3_region,
            endpoint_url=config.s3_endpoint,
            aws_access_key_id=config.s3_access_key_id,
            aws_secret_access_key=config.s3_secret_access_key,
            use_ssl=True,
            verify=False  # For development with self-signed certs
        )
        
        logger.info(
            "Initialized S3 backend",
            bucket=self.bucket,
            region=config.s3_region
        )
    
    def _parse_s3_path(self, path: str) -> tuple[str, str]:
        """Parse S3 path to extract bucket and key."""
        if path.startswith("s3://"):
            parsed = urlparse(path)
            return parsed.netloc, parsed.path.lstrip('/')
        else:
            # Assume it's just the key
            return self.bucket, path
    
    async def store(self, data: bytes, metadata: BinaryDataMetadata) -> str:
        """Store binary data to S3."""
        try:
            # Generate S3 key
            date_path = datetime.now().strftime("%Y/%m/%d")
            key = f"data/{date_path}/{metadata.filename}"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType=metadata.content_type,
                Metadata={
                    'checksum': metadata.checksum,
                    'compressed': str(metadata.is_compressed),
                    'encrypted': str(metadata.is_encrypted)
                }
            )
            
            storage_path = f"s3://{self.bucket}/{key}"
            
            logger.debug(
                "Stored binary data to S3",
                bucket=self.bucket,
                key=key,
                size=len(data)
            )
            
            return storage_path
            
        except Exception as e:
            logger.error(
                "Failed to store binary data to S3",
                error=str(e),
                filename=metadata.filename
            )
            raise BinaryDataError(f"Failed to store data to S3: {str(e)}")
    
    async def retrieve(self, path: str) -> bytes:
        """Retrieve binary data from S3."""
        try:
            bucket, key = self._parse_s3_path(path)
            
            response = self.s3_client.get_object(
                Bucket=bucket,
                Key=key
            )
            
            data = response['Body'].read()
            
            logger.debug(
                "Retrieved binary data from S3",
                bucket=bucket,
                key=key,
                size=len(data)
            )
            
            return data
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise BinaryDataNotFoundError(f"S3 object not found: {path}")
            else:
                logger.error(
                    "Failed to retrieve binary data from S3",
                    error=str(e),
                    path=path
                )
                raise BinaryDataError(f"Failed to retrieve data from S3: {str(e)}")
    
    async def delete(self, path: str) -> None:
        """Delete binary data from S3."""
        try:
            bucket, key = self._parse_s3_path(path)
            
            self.s3_client.delete_object(
                Bucket=bucket,
                Key=key
            )
            
            logger.debug(
                "Deleted binary data from S3",
                bucket=bucket,
                key=key
            )
            
        except Exception as e:
            logger.error(
                "Failed to delete binary data from S3",
                error=str(e),
                path=path
            )
            raise BinaryDataError(f"Failed to delete data from S3: {str(e)}")
    
    async def exists(self, path: str) -> bool:
        """Check if binary data exists in S3."""
        try:
            bucket, key = self._parse_s3_path(path)
            
            self.s3_client.head_object(
                Bucket=bucket,
                Key=key
            )
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(
                    "Failed to check S3 object existence",
                    error=str(e),
                    path=path
                )
                return False
    
    async def stream(
        self,
        path: str,
        chunk_size: int = 8192
    ) -> AsyncIterator[bytes]:
        """Stream binary data from S3 in chunks."""
        try:
            bucket, key = self._parse_s3_path(path)
            
            response = self.s3_client.get_object(
                Bucket=bucket,
                Key=key
            )
            
            body = response['Body']
            
            # Stream chunks
            while True:
                chunk = body.read(chunk_size)
                if not chunk:
                    break
                yield chunk
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise BinaryDataNotFoundError(f"S3 object not found: {path}")
            else:
                logger.error(
                    "Failed to stream binary data from S3",
                    error=str(e),
                    path=path
                )
                raise BinaryDataError(f"Failed to stream data from S3: {str(e)}")
    
    async def get_url(self, path: str, expires_in: int = 3600) -> str:
        """Generate a presigned URL for S3 object."""
        try:
            bucket, key = self._parse_s3_path(path)
            
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket,
                    'Key': key
                },
                ExpiresIn=expires_in
            )
            
            logger.debug(
                "Generated presigned URL",
                bucket=bucket,
                key=key,
                expires_in=expires_in
            )
            
            return url
            
        except Exception as e:
            logger.error(
                "Failed to generate presigned URL",
                error=str(e),
                path=path
            )
            raise BinaryDataError(f"Failed to generate URL: {str(e)}")