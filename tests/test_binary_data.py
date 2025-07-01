"""Test binary data handling system."""

import asyncio
import os
import tempfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import AsyncIterator, Optional
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError

from budflow.core.binary_data import (
    BinaryDataConfig,
    BinaryDataMetadata,
    BinaryDataMeta,
    BinaryDataMode,
    BinaryDataReference,
    BinaryDataService,
    FileSystemBackend,
    S3Backend,
    BinaryDataError,
    BinaryDataNotFoundError,
    BinaryDataStorageError,
)


@pytest.fixture
def binary_config():
    """Create binary data configuration."""
    return BinaryDataConfig(
        mode=BinaryDataMode.FILESYSTEM,
        filesystem_path="/tmp/budflow/binary",
        s3_bucket="budflow-binary-data",
        s3_region="us-east-1",
        max_size_mb=100,
        ttl_minutes=60,
        enable_compression=True,
        enable_encryption=True,
        encryption_key="test-encryption-key-1234567890",
    )


@pytest.fixture
def filesystem_backend(binary_config):
    """Create filesystem backend."""
    return FileSystemBackend(binary_config)


@pytest.fixture
def s3_backend(binary_config):
    """Create S3 backend with mocked AWS."""
    with mock_aws():
        # Create mock S3 bucket
        s3 = boto3.client('s3', region_name=binary_config.s3_region)
        s3.create_bucket(Bucket=binary_config.s3_bucket)
        
        binary_config.mode = BinaryDataMode.S3
        yield S3Backend(binary_config)


@pytest.fixture
def binary_service(binary_config):
    """Create binary data service."""
    return BinaryDataService(binary_config)


@pytest.fixture
def sample_data():
    """Create sample binary data."""
    return b"This is test binary data content!"


@pytest.fixture
def large_data():
    """Create large binary data for streaming tests."""
    # 5MB of data
    return b"x" * (5 * 1024 * 1024)


@pytest.mark.unit
class TestBinaryDataConfig:
    """Test binary data configuration."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = BinaryDataConfig()
        
        assert config.mode == BinaryDataMode.FILESYSTEM
        assert config.filesystem_path == "/data/binary"
        assert config.max_size_mb == 250
        assert config.ttl_minutes == 60
        assert config.enable_compression is False
        assert config.enable_encryption is False
    
    def test_s3_config_validation(self):
        """Test S3 configuration validation."""
        config = BinaryDataConfig(mode=BinaryDataMode.S3)
        
        # Should have default S3 settings
        assert config.s3_bucket == "budflow-binary"
        assert config.s3_region == "us-east-1"
    
    def test_encryption_key_validation(self):
        """Test encryption key validation."""
        with pytest.raises(ValueError, match="Encryption key required"):
            BinaryDataConfig(
                enable_encryption=True,
                encryption_key=""
            )


@pytest.mark.unit
class TestBinaryDataMetadata:
    """Test binary data metadata."""
    
    def test_metadata_creation(self):
        """Test creating metadata."""
        metadata = BinaryDataMeta(
            id=str(uuid4()),
            filename="test.pdf",
            mime_type="application/pdf",
            size=1024,
            checksum="abc123",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            workflow_id=str(uuid4()),
            execution_id=str(uuid4()),
            node_id="node_1",
        )
        
        assert metadata.filename == "test.pdf"
        assert metadata.mime_type == "application/pdf"
        assert metadata.size == 1024
        assert metadata.is_expired() is False
    
    def test_metadata_expiration(self):
        """Test metadata expiration check."""
        metadata = BinaryDataMeta(
            id=str(uuid4()),
            filename="test.txt",
            mime_type="text/plain",
            size=100,
            checksum="xyz789",
            created_at=datetime.now(timezone.utc) - timedelta(hours=2),
            expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        
        assert metadata.is_expired() is True


@pytest.mark.unit
class TestFileSystemBackend:
    """Test filesystem storage backend."""
    
    @pytest.mark.asyncio
    async def test_store_binary_data(self, filesystem_backend, sample_data):
        """Test storing binary data."""
        data_id = str(uuid4())
        
        # Store data
        path = await filesystem_backend.store(data_id, sample_data)
        
        assert path is not None
        assert data_id in path
        
        # Verify file exists
        assert os.path.exists(path)
        
        # Cleanup
        os.remove(path)
    
    @pytest.mark.asyncio
    async def test_retrieve_binary_data(self, filesystem_backend, sample_data):
        """Test retrieving binary data."""
        data_id = str(uuid4())
        
        # Store data first
        path = await filesystem_backend.store(data_id, sample_data)
        
        # Retrieve data
        retrieved = await filesystem_backend.retrieve(data_id)
        
        assert retrieved == sample_data
        
        # Cleanup
        os.remove(path)
    
    @pytest.mark.asyncio
    async def test_delete_binary_data(self, filesystem_backend, sample_data):
        """Test deleting binary data."""
        data_id = str(uuid4())
        
        # Store data first
        path = await filesystem_backend.store(data_id, sample_data)
        assert os.path.exists(path)
        
        # Delete data
        deleted = await filesystem_backend.delete(data_id)
        
        assert deleted is True
        assert not os.path.exists(path)
    
    @pytest.mark.asyncio
    async def test_stream_large_data(self, filesystem_backend, large_data):
        """Test streaming large binary data."""
        data_id = str(uuid4())
        
        # Store using stream
        stream = BytesIO(large_data)
        path = await filesystem_backend.store_stream(data_id, stream)
        
        # Retrieve using stream
        chunks = []
        async for chunk in filesystem_backend.retrieve_stream(data_id):
            chunks.append(chunk)
        
        retrieved = b"".join(chunks)
        assert retrieved == large_data
        
        # Cleanup
        os.remove(path)
    
    @pytest.mark.asyncio
    async def test_exists_check(self, filesystem_backend, sample_data):
        """Test checking if data exists."""
        data_id = str(uuid4())
        
        # Should not exist initially
        assert await filesystem_backend.exists(data_id) is False
        
        # Store data
        path = await filesystem_backend.store(data_id, sample_data)
        
        # Should exist now
        assert await filesystem_backend.exists(data_id) is True
        
        # Cleanup
        os.remove(path)


@pytest.mark.unit
class TestS3Backend:
    """Test S3 storage backend."""
    
    @pytest.mark.asyncio
    async def test_store_binary_data(self, s3_backend, sample_data):
        """Test storing binary data in S3."""
        data_id = str(uuid4())
        
        # Store data
        key = await s3_backend.store(data_id, sample_data)
        
        assert key is not None
        assert data_id in key
    
    @pytest.mark.asyncio
    async def test_retrieve_binary_data(self, s3_backend, sample_data):
        """Test retrieving binary data from S3."""
        data_id = str(uuid4())
        
        # Store data first
        await s3_backend.store(data_id, sample_data)
        
        # Retrieve data
        retrieved = await s3_backend.retrieve(data_id)
        
        assert retrieved == sample_data
    
    @pytest.mark.asyncio
    async def test_generate_signed_url(self, s3_backend, sample_data):
        """Test generating signed URL for S3 object."""
        data_id = str(uuid4())
        
        # Store data first
        await s3_backend.store(data_id, sample_data)
        
        # Generate signed URL
        url = await s3_backend.generate_signed_url(data_id, expires_in=3600)
        
        assert url is not None
        assert "https://" in url
        assert data_id in url
    
    @pytest.mark.asyncio
    async def test_multipart_upload(self, s3_backend, large_data):
        """Test multipart upload for large files."""
        data_id = str(uuid4())
        
        # Initiate multipart upload
        upload_id = await s3_backend.initiate_multipart_upload(data_id)
        
        assert upload_id is not None
        
        # Upload parts
        chunk_size = 5 * 1024 * 1024  # 5MB chunks
        parts = []
        
        for i, offset in enumerate(range(0, len(large_data), chunk_size)):
            chunk = large_data[offset:offset + chunk_size]
            etag = await s3_backend.upload_part(
                data_id, upload_id, i + 1, chunk
            )
            parts.append({"PartNumber": i + 1, "ETag": etag})
        
        # Complete upload
        await s3_backend.complete_multipart_upload(data_id, upload_id, parts)
        
        # Verify data
        retrieved = await s3_backend.retrieve(data_id)
        assert retrieved == large_data


@pytest.mark.unit
class TestBinaryDataService:
    """Test main binary data service."""
    
    @pytest.mark.asyncio
    async def test_store_and_retrieve(self, binary_service, sample_data):
        """Test storing and retrieving binary data."""
        # Create metadata
        metadata = BinaryDataMeta(
            filename="test.bin",
            mime_type="application/octet-stream",
            size=len(sample_data),
            checksum="",  # Will be calculated by service
            storage_mode=BinaryDataMode.FILESYSTEM,
            workflow_id=str(uuid4()),
            execution_id=str(uuid4()),
        )
        
        # Store data
        reference = await binary_service.store(sample_data, metadata)
        
        assert reference is not None
        assert reference.id is not None
        assert reference.url is None  # No URL for filesystem
        
        # Retrieve data
        retrieved_data, retrieved_metadata = await binary_service.retrieve(
            reference.id
        )
        
        assert retrieved_data == sample_data
        assert retrieved_metadata.filename == metadata.filename
    
    @pytest.mark.asyncio
    async def test_compression(self, binary_service, sample_data):
        """Test data compression."""
        binary_service.config.enable_compression = True
        
        metadata = BinaryDataMeta(
            filename="test.txt",
            mime_type="text/plain",
            size=len(sample_data),
            checksum="",
            storage_mode=BinaryDataMode.FILESYSTEM,
        )
        
        # Store with compression
        reference = await binary_service.store(sample_data, metadata)
        
        # Retrieve should decompress automatically
        retrieved_data, _ = await binary_service.retrieve(reference.id)
        
        assert retrieved_data == sample_data
    
    @pytest.mark.asyncio
    async def test_encryption(self, binary_service, sample_data):
        """Test data encryption."""
        binary_service.config.enable_encryption = True
        binary_service.config.encryption_key = "test-key-32-bytes-long-1234567890"
        
        metadata = BinaryDataMeta(
            filename="sensitive.dat",
            mime_type="application/octet-stream",
            size=len(sample_data),
            checksum="",
            storage_mode=BinaryDataMode.FILESYSTEM,
        )
        
        # Store with encryption
        reference = await binary_service.store(sample_data, metadata)
        
        # Retrieve should decrypt automatically
        retrieved_data, _ = await binary_service.retrieve(reference.id)
        
        assert retrieved_data == sample_data
    
    @pytest.mark.asyncio
    async def test_ttl_cleanup(self, binary_service, sample_data):
        """Test TTL-based cleanup."""
        # Create expired metadata
        metadata = BinaryDataMeta(
            filename="old.dat",
            mime_type="application/octet-stream",
            size=len(sample_data),
            checksum="",
            storage_mode=BinaryDataMode.FILESYSTEM,
            expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        
        # Store data
        reference = await binary_service.store(sample_data, metadata)
        
        # Run cleanup
        deleted_count = await binary_service.cleanup_expired()
        
        assert deleted_count >= 1
        
        # Data should be gone
        with pytest.raises(BinaryDataNotFoundError):
            await binary_service.retrieve(reference.id)
    
    @pytest.mark.asyncio
    async def test_size_limit(self, binary_service):
        """Test size limit enforcement."""
        binary_service.config.max_size_mb = 1  # 1MB limit
        
        # Create data larger than limit
        large_data = b"x" * (2 * 1024 * 1024)  # 2MB
        
        metadata = BinaryDataMeta(
            filename="large.bin",
            mime_type="application/octet-stream",
            size=len(large_data),
            checksum="",
            storage_mode=BinaryDataMode.FILESYSTEM,
        )
        
        # Should raise error
        with pytest.raises(BinaryDataStorageError, match="exceeds maximum"):
            await binary_service.store(large_data, metadata)
    
    @pytest.mark.asyncio
    async def test_stream_processing(self, binary_service, large_data):
        """Test stream processing for large files."""
        metadata = BinaryDataMeta(
            filename="stream.bin",
            mime_type="application/octet-stream",
            size=len(large_data),
            checksum="",
            storage_mode=BinaryDataMode.FILESYSTEM,
        )
        
        # Store using stream
        stream = BytesIO(large_data)
        reference = await binary_service.store_stream(stream, metadata)
        
        # Retrieve using stream
        chunks = []
        async for chunk in binary_service.retrieve_stream(reference.id):
            chunks.append(chunk)
        
        retrieved = b"".join(chunks)
        assert retrieved == large_data
    
    @pytest.mark.asyncio
    async def test_progress_tracking(self, binary_service, large_data):
        """Test upload/download progress tracking."""
        progress_updates = []
        
        def progress_callback(bytes_processed, total_bytes):
            progress_updates.append((bytes_processed, total_bytes))
        
        metadata = BinaryDataMeta(
            filename="progress.bin",
            mime_type="application/octet-stream",
            size=len(large_data),
            checksum="",
            storage_mode=BinaryDataMode.FILESYSTEM,
        )
        
        # Store with progress tracking
        reference = await binary_service.store(
            large_data, metadata, progress_callback=progress_callback
        )
        
        # Should have progress updates
        assert len(progress_updates) > 0
        assert progress_updates[-1][0] == len(large_data)
    
    @pytest.mark.asyncio
    async def test_metadata_search(self, binary_service, sample_data):
        """Test searching for binary data by metadata."""
        workflow_id = str(uuid4())
        
        # Store multiple files
        for i in range(3):
            metadata = BinaryDataMeta(
                filename=f"file_{i}.dat",
                mime_type="application/octet-stream",
                size=len(sample_data),
                checksum="",
                storage_mode=BinaryDataMode.FILESYSTEM,
                workflow_id=workflow_id,
            )
            await binary_service.store(sample_data, metadata)
        
        # Search by workflow ID
        results = await binary_service.find_by_workflow(workflow_id)
        
        assert len(results) == 3
        assert all(r.workflow_id == workflow_id for r in results)


@pytest.mark.unit
class TestBinaryDataReference:
    """Test binary data reference."""
    
    def test_reference_creation(self):
        """Test creating binary data reference."""
        ref = BinaryDataReference(
            id=str(uuid4()),
            url="https://example.com/data/123",
            mode=BinaryDataMode.S3,
        )
        
        assert ref.id is not None
        assert ref.url == "https://example.com/data/123"
        assert ref.mode == BinaryDataMode.S3
    
    def test_reference_serialization(self):
        """Test reference serialization."""
        ref = BinaryDataReference(
            id=str(uuid4()),
            mode=BinaryDataMode.FILESYSTEM,
        )
        
        # Convert to dict
        data = ref.model_dump()
        
        assert "id" in data
        assert "mode" in data
        assert data["mode"] == "filesystem"


@pytest.mark.integration
class TestBinaryDataIntegration:
    """Integration tests for binary data system."""
    
    @pytest.mark.asyncio
    async def test_workflow_binary_data_lifecycle(self, binary_service):
        """Test complete binary data lifecycle in workflow context."""
        workflow_id = str(uuid4())
        execution_id = str(uuid4())
        
        # Simulate receiving binary data in webhook
        webhook_data = b"Uploaded file content"
        
        metadata = BinaryDataMeta(
            filename="upload.pdf",
            mime_type="application/pdf",
            size=len(webhook_data),
            checksum="",
            storage_mode=BinaryDataMode.FILESYSTEM,
            workflow_id=workflow_id,
            execution_id=execution_id,
            node_id="webhook_1",
        )
        
        # Store in binary data service
        reference = await binary_service.store(webhook_data, metadata)
        
        # Pass reference through workflow
        # ... workflow execution ...
        
        # Retrieve in another node
        retrieved_data, retrieved_metadata = await binary_service.retrieve(
            reference.id
        )
        
        assert retrieved_data == webhook_data
        assert retrieved_metadata.workflow_id == workflow_id
        
        # Cleanup after workflow completes
        await binary_service.delete(reference.id)
        
        # Verify deletion
        assert await binary_service.exists(reference.id) is False
    
    @pytest.mark.asyncio
    async def test_s3_backend_with_real_aws(self):
        """Test S3 backend with real AWS (skip in CI)."""
        if not os.getenv("AWS_ACCESS_KEY_ID"):
            pytest.skip("AWS credentials not available")
        
        config = BinaryDataConfig(
            mode=BinaryDataMode.S3,
            s3_bucket=os.getenv("TEST_S3_BUCKET", "budflow-test"),
            s3_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        
        service = BinaryDataService(config)
        
        # Test basic operations
        test_data = b"Real S3 test data"
        metadata = BinaryDataMeta(
            filename="s3_test.txt",
            mime_type="text/plain",
            size=len(test_data),
            checksum="",
            storage_mode=BinaryDataMode.S3,
        )
        
        # Store
        reference = await service.store(test_data, metadata)
        assert reference.url is not None
        
        # Retrieve
        retrieved, _ = await service.retrieve(reference.id)
        assert retrieved == test_data
        
        # Cleanup
        await service.delete(reference.id)