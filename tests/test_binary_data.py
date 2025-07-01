"""Tests for binary data management system."""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4

from budflow.core.binary_data import (
    BinaryDataManager,
    FileSystemBackend,
    S3Backend,
    BinaryDataError,
    BinaryDataNotFoundError,
    BinaryDataMetadata,
    BinaryDataConfig,
    StorageBackend,
)


@pytest.fixture
def temp_storage_dir():
    """Create temporary storage directory."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def binary_config(temp_storage_dir):
    """Create binary data configuration."""
    import base64
    import secrets
    
    return BinaryDataConfig(
        backend="filesystem",
        filesystem_path=temp_storage_dir,
        s3_bucket="test-bucket",
        s3_region="us-east-1",
        max_file_size=10 * 1024 * 1024,  # 10MB
        allowed_mime_types=["image/*", "application/pdf", "text/*", "application/octet-stream"],
        ttl_days=30,
        enable_compression=True,
        enable_encryption=True,
        encryption_key=base64.b64encode(secrets.token_bytes(32)).decode('utf-8')
    )


@pytest.fixture
def filesystem_backend(binary_config):
    """Create filesystem backend."""
    return FileSystemBackend(binary_config)


@pytest.fixture
def s3_backend(binary_config):
    """Create S3 backend."""
    with patch('budflow.core.binary_data.s3.boto3') as mock_boto3:
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        backend = S3Backend(binary_config)
        # Store reference to mock client for tests
        backend._mock_client = mock_client
        return backend


@pytest.fixture
def binary_manager(binary_config):
    """Create binary data manager."""
    return BinaryDataManager(binary_config)


@pytest.fixture
def sample_binary_data():
    """Create sample binary data."""
    return b"This is test binary data content for testing purposes."


@pytest.fixture
def sample_metadata():
    """Create sample metadata."""
    return BinaryDataMetadata(
        id=uuid4(),
        filename="test-file.pdf",
        content_type="application/pdf",
        size=1024,
        checksum="abc123def456",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(days=30),
        metadata={
            "workflow_id": str(uuid4()),
            "node_id": "http_request_1",
            "user_id": str(uuid4())
        },
        storage_path="",
        is_compressed=False,
        is_encrypted=False
    )


class TestFileSystemBackend:
    """Test filesystem storage backend."""
    
    @pytest.mark.asyncio
    async def test_store_binary_data(self, filesystem_backend, sample_binary_data, sample_metadata):
        """Test storing binary data."""
        # Store data
        storage_path = await filesystem_backend.store(
            data=sample_binary_data,
            metadata=sample_metadata
        )
        
        assert storage_path is not None
        assert Path(storage_path).exists()
        
        # Read file content
        with open(storage_path, 'rb') as f:
            stored_data = f.read()
        
        assert stored_data == sample_binary_data
    
    @pytest.mark.asyncio
    async def test_retrieve_binary_data(self, filesystem_backend, sample_binary_data, sample_metadata):
        """Test retrieving binary data."""
        # Store data first
        storage_path = await filesystem_backend.store(
            data=sample_binary_data,
            metadata=sample_metadata
        )
        
        # Retrieve data
        retrieved_data = await filesystem_backend.retrieve(storage_path)
        
        assert retrieved_data == sample_binary_data
    
    @pytest.mark.asyncio
    async def test_delete_binary_data(self, filesystem_backend, sample_binary_data, sample_metadata):
        """Test deleting binary data."""
        # Store data first
        storage_path = await filesystem_backend.store(
            data=sample_binary_data,
            metadata=sample_metadata
        )
        
        assert Path(storage_path).exists()
        
        # Delete data
        await filesystem_backend.delete(storage_path)
        
        assert not Path(storage_path).exists()
    
    @pytest.mark.asyncio
    async def test_stream_binary_data(self, filesystem_backend, sample_metadata):
        """Test streaming large binary data."""
        # Create large data (1MB)
        large_data = b"x" * (1024 * 1024)
        
        # Store data
        storage_path = await filesystem_backend.store(
            data=large_data,
            metadata=sample_metadata
        )
        
        # Stream data
        chunks = []
        async for chunk in filesystem_backend.stream(storage_path, chunk_size=1024):
            chunks.append(chunk)
        
        # Verify
        retrieved_data = b"".join(chunks)
        assert retrieved_data == large_data
        assert len(chunks) == 1024  # 1MB / 1KB chunks
    
    @pytest.mark.asyncio
    async def test_exists_check(self, filesystem_backend, sample_binary_data, sample_metadata):
        """Test checking if data exists."""
        # Initially doesn't exist
        fake_path = "/fake/path/data.bin"
        assert not await filesystem_backend.exists(fake_path)
        
        # Store data
        storage_path = await filesystem_backend.store(
            data=sample_binary_data,
            metadata=sample_metadata
        )
        
        # Now exists
        assert await filesystem_backend.exists(storage_path)
    
    @pytest.mark.asyncio
    async def test_get_url(self, filesystem_backend, sample_binary_data, sample_metadata):
        """Test getting URL for binary data."""
        # Store data
        storage_path = await filesystem_backend.store(
            data=sample_binary_data,
            metadata=sample_metadata
        )
        
        # Get URL
        url = await filesystem_backend.get_url(storage_path, expires_in=3600)
        
        assert url.startswith("file://")
        assert storage_path in url


class TestS3Backend:
    """Test S3 storage backend."""
    
    @pytest.mark.asyncio
    async def test_store_binary_data(self, s3_backend, sample_binary_data, sample_metadata):
        """Test storing binary data to S3."""
        # Get the mocked client
        mock_s3 = s3_backend.s3_client
        
        # Mock put_object
        mock_s3.put_object.return_value = {'ETag': '"abc123"'}
        
        # Store data
        storage_path = await s3_backend.store(
            data=sample_binary_data,
            metadata=sample_metadata
        )
        
        assert storage_path is not None
        assert storage_path.startswith("s3://")
        
        # Verify S3 call
        mock_s3.put_object.assert_called_once()
        call_args = mock_s3.put_object.call_args[1]
        assert call_args['Bucket'] == 'test-bucket'
        assert call_args['Body'] == sample_binary_data
    
    @pytest.mark.asyncio
    async def test_retrieve_binary_data(self, s3_backend, sample_binary_data):
        """Test retrieving binary data from S3."""
        # Get the mocked client
        mock_s3 = s3_backend.s3_client
        
        # Mock get_object
        mock_s3.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=sample_binary_data))
        }
        
        # Retrieve data
        storage_path = "s3://test-bucket/data/2024/01/test-file.pdf"
        retrieved_data = await s3_backend.retrieve(storage_path)
        
        assert retrieved_data == sample_binary_data
        
        # Verify S3 call
        mock_s3.get_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='data/2024/01/test-file.pdf'
        )
    
    @pytest.mark.asyncio
    async def test_delete_binary_data(self, s3_backend):
        """Test deleting binary data from S3."""
        # Get the mocked client
        mock_s3 = s3_backend.s3_client
        
        # Delete data
        storage_path = "s3://test-bucket/data/2024/01/test-file.pdf"
        await s3_backend.delete(storage_path)
        
        # Verify S3 call
        mock_s3.delete_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='data/2024/01/test-file.pdf'
        )
    
    @pytest.mark.asyncio
    async def test_stream_binary_data(self, s3_backend):
        """Test streaming large binary data from S3."""
        # Create large data chunks
        chunk1 = b"x" * 1024
        chunk2 = b"y" * 1024
        chunk3 = b"z" * 512
        
        # Get the mocked client
        mock_s3 = s3_backend.s3_client
        
        # Mock streaming response
        mock_body = Mock()
        mock_body.read = Mock(side_effect=[chunk1, chunk2, chunk3, b""])
        
        mock_s3.get_object.return_value = {'Body': mock_body}
        
        # Stream data
        storage_path = "s3://test-bucket/data/large-file.bin"
        chunks = []
        async for chunk in s3_backend.stream(storage_path):
            chunks.append(chunk)
        
        assert len(chunks) == 3
        assert chunks[0] == chunk1
        assert chunks[1] == chunk2
        assert chunks[2] == chunk3
    
    @pytest.mark.asyncio
    async def test_get_presigned_url(self, s3_backend):
        """Test generating presigned URL."""
        # Get the mocked client
        mock_s3 = s3_backend.s3_client
        
        # Mock generate_presigned_url
        mock_url = "https://test-bucket.s3.amazonaws.com/data/file.pdf?signature=xyz"
        mock_s3.generate_presigned_url.return_value = mock_url
        
        # Get URL
        storage_path = "s3://test-bucket/data/file.pdf"
        url = await s3_backend.get_url(storage_path, expires_in=3600)
        
        assert url == mock_url
        
        # Verify S3 call
        mock_s3.generate_presigned_url.assert_called_once_with(
            'get_object',
            Params={'Bucket': 'test-bucket', 'Key': 'data/file.pdf'},
            ExpiresIn=3600
        )


class TestBinaryDataManager:
    """Test binary data manager."""
    
    @pytest.mark.asyncio
    async def test_store_with_compression(self, binary_manager):
        """Test storing data with compression."""
        # Create compressible data (repeated pattern)
        compressible_data = b"This is a test string that will be repeated many times. " * 100
        
        # Store data
        binary_id = await binary_manager.store(
            data=compressible_data,
            filename="test.txt",
            content_type="text/plain",
            metadata={"test": "value"}
        )
        
        assert isinstance(binary_id, UUID)
        
        # Check metadata
        metadata = await binary_manager.get_metadata(binary_id)
        assert metadata.filename == "test.txt"
        assert metadata.content_type == "text/plain"
        assert metadata.is_compressed is True
        # Compressed size should be smaller than original
        assert metadata.size < len(compressible_data)
    
    @pytest.mark.asyncio
    async def test_store_with_encryption(self, binary_manager):
        """Test storing data with encryption."""
        sensitive_data = b"This is sensitive data"
        
        # Store data
        binary_id = await binary_manager.store(
            data=sensitive_data,
            filename="sensitive.dat",
            content_type="application/octet-stream",
            metadata={"encrypted": True}
        )
        
        # Retrieve data
        retrieved_data = await binary_manager.retrieve(binary_id)
        
        # Should get original data back (transparently decrypted)
        assert retrieved_data == sensitive_data
        
        # Check metadata
        metadata = await binary_manager.get_metadata(binary_id)
        assert metadata.is_encrypted is True
    
    @pytest.mark.asyncio
    async def test_store_with_ttl(self, binary_manager, sample_binary_data):
        """Test storing data with TTL."""
        # Store data with custom TTL
        binary_id = await binary_manager.store(
            data=sample_binary_data,
            filename="temp.dat",
            content_type="application/octet-stream",
            ttl_days=7
        )
        
        # Check metadata
        metadata = await binary_manager.get_metadata(binary_id)
        assert metadata.expires_at is not None
        
        # Check expiration is ~7 days from now
        expected_expiry = datetime.now(timezone.utc) + timedelta(days=7)
        time_diff = abs((metadata.expires_at - expected_expiry).total_seconds())
        assert time_diff < 60  # Within 1 minute
    
    @pytest.mark.asyncio
    async def test_cleanup_expired_data(self, binary_manager):
        """Test cleaning up expired data."""
        # Store data with very short TTL
        binary_id = await binary_manager.store(
            data=b"expired data",
            filename="expired.dat",
            content_type="application/octet-stream",
            ttl_days=0.00001  # Expires in ~0.864 seconds
        )
        
        # Wait for it to expire
        await asyncio.sleep(1.0)
        
        # Run cleanup
        cleaned_count = await binary_manager.cleanup_expired()
        
        assert cleaned_count >= 1
        
        # Data should be gone
        with pytest.raises(BinaryDataNotFoundError):
            await binary_manager.retrieve(binary_id)
    
    @pytest.mark.asyncio
    async def test_get_signed_url(self, binary_manager, sample_binary_data):
        """Test getting signed URL for data access."""
        # Store data
        binary_id = await binary_manager.store(
            data=sample_binary_data,
            filename="document.pdf",
            content_type="application/pdf"
        )
        
        # Get signed URL
        url = await binary_manager.get_signed_url(binary_id, expires_in=3600)
        
        assert url is not None
        assert isinstance(url, str)
        
        # URL should contain signature or be a valid file:// URL
        assert url.startswith(("https://", "file://"))
    
    @pytest.mark.asyncio
    async def test_validate_file_size(self, binary_manager):
        """Test file size validation."""
        # Try to store file that's too large
        large_data = b"x" * (11 * 1024 * 1024)  # 11MB (over 10MB limit)
        
        with pytest.raises(BinaryDataError) as exc_info:
            await binary_manager.store(
                data=large_data,
                filename="large.bin",
                content_type="application/octet-stream"
            )
        
        assert "exceeds maximum size" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_validate_mime_type(self, binary_manager):
        """Test MIME type validation."""
        # Try to store disallowed MIME type
        with pytest.raises(BinaryDataError) as exc_info:
            await binary_manager.store(
                data=b"executable content",
                filename="malware.exe",
                content_type="application/x-executable"
            )
        
        assert "not allowed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, binary_manager):
        """Test concurrent binary data operations."""
        # Store multiple files concurrently
        tasks = []
        for i in range(10):
            task = binary_manager.store(
                data=f"Content {i}".encode(),
                filename=f"file_{i}.txt",
                content_type="text/plain"
            )
            tasks.append(task)
        
        binary_ids = await asyncio.gather(*tasks)
        
        assert len(binary_ids) == 10
        assert len(set(binary_ids)) == 10  # All unique
        
        # Retrieve all concurrently
        retrieve_tasks = [
            binary_manager.retrieve(binary_id)
            for binary_id in binary_ids
        ]
        
        results = await asyncio.gather(*retrieve_tasks)
        
        for i, data in enumerate(results):
            assert data == f"Content {i}".encode()


class TestBinaryDataIntegration:
    """Integration tests for binary data system."""
    
    @pytest.mark.asyncio
    async def test_workflow_binary_data_flow(self, binary_manager):
        """Test binary data flow in workflow context."""
        # Simulate webhook with binary data
        webhook_data = {
            "text": "Hello",
            "file": b"Binary file content"
        }
        
        # Store binary part
        binary_id = await binary_manager.store(
            data=webhook_data["file"],
            filename="upload.bin",
            content_type="application/octet-stream",
            metadata={
                "webhook_id": str(uuid4()),
                "field": "file"
            }
        )
        
        # Replace binary with reference
        processed_data = {
            "text": webhook_data["text"],
            "file": {"binary_id": str(binary_id)}
        }
        
        # Later in workflow, retrieve binary
        if "binary_id" in processed_data.get("file", {}):
            file_data = await binary_manager.retrieve(
                UUID(processed_data["file"]["binary_id"])
            )
            assert file_data == webhook_data["file"]
    
    @pytest.mark.asyncio
    async def test_backend_switching(self, binary_config, temp_storage_dir):
        """Test switching between storage backends."""
        # Start with filesystem
        fs_config = binary_config.copy()
        fs_config.backend = "filesystem"
        fs_manager = BinaryDataManager(fs_config)
        
        # Store data
        binary_id = await fs_manager.store(
            data=b"Test data",
            filename="test.dat",
            content_type="application/octet-stream"
        )
        
        # Switch to S3 (mocked)
        s3_config = binary_config.copy()
        s3_config.backend = "s3"
        
        with patch('budflow.core.binary_data.s3.boto3'):
            s3_manager = BinaryDataManager(s3_config)
            
            # Should still be able to retrieve from filesystem
            # (in real implementation, might migrate data)
            metadata = await fs_manager.get_metadata(binary_id)
            assert metadata is not None