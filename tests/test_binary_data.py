"""Test binary data handling system."""

import asyncio
import io
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncIterator
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from uuid import uuid4

import pytest
import aiofiles
import boto3
from botocore.exceptions import ClientError

from budflow.core.binary_data_v2 import (
    BinaryDataConfig,
    BinaryDataMeta,
    BinaryDataMetadata,
    BinaryDataReference,
    BinaryDataService,
    BinaryDataMode,
    BinaryDataError,
    BinaryDataNotFoundError,
    BinaryDataStorageError,
    FileSystemBackend,
    S3Backend,
)


@pytest.fixture
def temp_directory():
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def binary_config(temp_directory):
    """Create binary data configuration."""
    return BinaryDataConfig(
        mode=BinaryDataMode.FILESYSTEM,
        filesystem_path=temp_directory,
        max_size_mb=10,
        ttl_minutes=60,
        enable_compression=False,
        enable_encryption=False,
        chunk_size=1024,
    )


@pytest.fixture
def s3_config():
    """Create S3 configuration."""
    return BinaryDataConfig(
        mode=BinaryDataMode.S3,
        s3_bucket="test-bucket",
        s3_region="us-east-1",
        max_size_mb=10,
        ttl_minutes=60,
    )


@pytest.fixture
def mock_db_session():
    """Create mock database session."""
    session = AsyncMock()
    session.commit = AsyncMock()
    session.add = Mock()
    return session


@pytest.fixture
def sample_data():
    """Create sample binary data."""
    return b"This is test binary data for BudFlow!"


@pytest.fixture
def sample_metadata():
    """Create sample metadata."""
    return BinaryDataMeta(
        filename="test_file.txt",
        mime_type="text/plain",
        size=37,
        workflow_id=str(uuid4()),
        execution_id=str(uuid4()),
        node_id="node_1",
    )


@pytest.mark.unit
class TestBinaryDataMeta:
    """Test BinaryDataMeta model."""
    
    def test_is_expired(self):
        """Test expiration check."""
        # Not expired - no expiration date
        meta = BinaryDataMeta(
            filename="test.txt",
            mime_type="text/plain",
            size=100,
        )
        assert not meta.is_expired()
        
        # Not expired - future date
        meta.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        assert not meta.is_expired()
        
        # Expired - past date
        meta.expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        assert meta.is_expired()
    
    def test_from_db_model(self):
        """Test creating from database model."""
        db_model = Mock(spec=BinaryDataMetadata)
        db_model.id = uuid4()
        db_model.filename = "test.txt"
        db_model.mime_type = "text/plain"
        db_model.size = 100
        db_model.checksum = "abc123"
        db_model.storage_path = "/path/to/file"
        db_model.storage_mode = BinaryDataMode.FILESYSTEM
        db_model.is_compressed = True
        db_model.is_encrypted = False
        db_model.created_at = datetime.now(timezone.utc)
        db_model.expires_at = None
        db_model.workflow_id = uuid4()
        db_model.execution_id = uuid4()
        db_model.node_id = "node_1"
        db_model.extra_metadata = {"key": "value"}
        
        meta = BinaryDataMeta.from_db_model(db_model)
        
        assert meta.id == str(db_model.id)
        assert meta.filename == "test.txt"
        assert meta.is_compressed is True
        assert meta.extra_metadata == {"key": "value"}


@pytest.mark.unit
class TestFileSystemBackend:
    """Test filesystem storage backend."""
    
    @pytest.mark.asyncio
    async def test_store_and_retrieve(self, binary_config, sample_data):
        """Test storing and retrieving data."""
        backend = FileSystemBackend(binary_config)
        data_id = str(uuid4())
        
        # Store data
        path = await backend.store(data_id, sample_data)
        assert Path(path).exists()
        
        # Retrieve data
        retrieved = await backend.retrieve(data_id)
        assert retrieved == sample_data
    
    @pytest.mark.asyncio
    async def test_delete(self, binary_config, sample_data):
        """Test deleting data."""
        backend = FileSystemBackend(binary_config)
        data_id = str(uuid4())
        
        # Store data
        await backend.store(data_id, sample_data)
        
        # Delete data
        deleted = await backend.delete(data_id)
        assert deleted is True
        
        # Verify deleted
        assert not await backend.exists(data_id)
    
    @pytest.mark.asyncio
    async def test_retrieve_not_found(self, binary_config):
        """Test retrieving non-existent data."""
        backend = FileSystemBackend(binary_config)
        
        with pytest.raises(BinaryDataNotFoundError):
            await backend.retrieve("non-existent-id")
    
    @pytest.mark.asyncio
    async def test_stream_operations(self, binary_config, sample_data):
        """Test stream store and retrieve."""
        backend = FileSystemBackend(binary_config)
        data_id = str(uuid4())
        
        # Store from BytesIO stream
        stream = io.BytesIO(sample_data)
        await backend.store_stream(data_id, stream)
        
        # Retrieve as stream
        chunks = []
        async for chunk in backend.retrieve_stream(data_id):
            chunks.append(chunk)
        
        retrieved = b"".join(chunks)
        assert retrieved == sample_data
    
    @pytest.mark.asyncio
    async def test_path_organization(self, binary_config):
        """Test path organization with prefixes."""
        backend = FileSystemBackend(binary_config)
        data_id = "abcdef123456"
        
        path = backend._get_path(data_id)
        assert "ab" in str(path)  # Check prefix directory


@pytest.mark.unit
class TestS3Backend:
    """Test S3 storage backend."""
    
    @pytest.mark.asyncio
    @patch('boto3.client')
    async def test_store_and_retrieve(self, mock_boto_client, s3_config, sample_data):
        """Test storing and retrieving data from S3."""
        # Setup mock S3 client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=sample_data))
        }
        
        backend = S3Backend(s3_config)
        data_id = str(uuid4())
        
        # Store data
        key = await backend.store(data_id, sample_data)
        assert key.startswith("binary-data/")
        
        # Verify S3 put_object was called
        mock_s3.put_object.assert_called_once()
        
        # Retrieve data
        retrieved = await backend.retrieve(data_id)
        assert retrieved == sample_data
    
    @pytest.mark.asyncio
    @patch('boto3.client')
    async def test_signed_url_generation(self, mock_boto_client, s3_config):
        """Test generating signed URLs."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.generate_presigned_url.return_value = "https://signed-url.com"
        
        backend = S3Backend(s3_config)
        data_id = str(uuid4())
        
        url = await backend.generate_signed_url(data_id, expires_in=3600)
        assert url == "https://signed-url.com"
        
        # Verify call
        mock_s3.generate_presigned_url.assert_called_once_with(
            "get_object",
            {"Bucket": "test-bucket", "Key": backend._get_key(data_id)},
            3600
        )
    
    @pytest.mark.asyncio
    @patch('boto3.client')
    async def test_multipart_upload(self, mock_boto_client, s3_config):
        """Test multipart upload operations."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.create_multipart_upload.return_value = {"UploadId": "test-upload-id"}
        mock_s3.upload_part.return_value = {"ETag": "test-etag"}
        
        backend = S3Backend(s3_config)
        data_id = str(uuid4())
        
        # Initiate multipart upload
        upload_id = await backend.initiate_multipart_upload(data_id)
        assert upload_id == "test-upload-id"
        
        # Upload part
        etag = await backend.upload_part(data_id, upload_id, 1, b"part data")
        assert etag == "test-etag"
        
        # Complete upload
        await backend.complete_multipart_upload(
            data_id, upload_id, [{"PartNumber": 1, "ETag": etag}]
        )
        
        # Verify calls
        mock_s3.complete_multipart_upload.assert_called_once()


@pytest.mark.unit
class TestBinaryDataService:
    """Test main binary data service."""
    
    @pytest.mark.asyncio
    async def test_store_and_retrieve(
        self, binary_config, mock_db_session, sample_data, sample_metadata
    ):
        """Test storing and retrieving data with metadata."""
        service = BinaryDataService(binary_config, mock_db_session)
        
        # Mock database query
        mock_result = Mock()
        mock_metadata = Mock(spec=BinaryDataMetadata)
        mock_metadata.id = uuid4()
        mock_metadata.filename = sample_metadata.filename
        mock_metadata.mime_type = sample_metadata.mime_type
        mock_metadata.size = sample_metadata.size
        mock_metadata.checksum = service._calculate_checksum(sample_data)
        mock_metadata.storage_path = f"/mock/path/{mock_metadata.id}"
        mock_metadata.storage_mode = BinaryDataMode.FILESYSTEM
        mock_metadata.is_compressed = False
        mock_metadata.is_encrypted = False
        mock_metadata.is_expired.return_value = False
        mock_metadata.created_at = datetime.now(timezone.utc)
        mock_metadata.expires_at = None
        mock_metadata.workflow_id = None
        mock_metadata.execution_id = None
        mock_metadata.node_id = None
        mock_metadata.extra_metadata = {}
        
        mock_result.scalar_one_or_none.return_value = mock_metadata
        mock_db_session.execute.return_value = mock_result
        
        # Store data
        reference = await service.store(sample_data, sample_metadata)
        assert reference.id is not None
        assert reference.mode == BinaryDataMode.FILESYSTEM
        
        # Verify database add was called
        mock_db_session.add.assert_called_once()
        mock_db_session.commit.assert_called()
        
        # Update mock to return the same ID  
        mock_metadata.id = reference.id
        
        # Retrieve data
        retrieved_data, retrieved_meta = await service.retrieve(reference.id)
        assert retrieved_data == sample_data
        assert retrieved_meta.filename == sample_metadata.filename
    
    @pytest.mark.asyncio
    async def test_compression(self, temp_directory, mock_db_session, sample_data, sample_metadata):
        """Test data compression."""
        config = BinaryDataConfig(
            mode=BinaryDataMode.FILESYSTEM,
            filesystem_path=temp_directory,
            enable_compression=True,
        )
        service = BinaryDataService(config, mock_db_session)
        
        # Store with compression
        reference = await service.store(sample_data, sample_metadata)
        
        # Check that data was compressed (stored size should be different)
        backend = FileSystemBackend(config)
        compressed_data = await backend.retrieve(reference.id)
        assert len(compressed_data) != len(sample_data)
    
    @pytest.mark.asyncio
    async def test_encryption(self, temp_directory, mock_db_session, sample_data, sample_metadata):
        """Test data encryption."""
        config = BinaryDataConfig(
            mode=BinaryDataMode.FILESYSTEM,
            filesystem_path=temp_directory,
            enable_encryption=True,
            encryption_key="test-encryption-key-32-bytes-long",
        )
        service = BinaryDataService(config, mock_db_session)
        
        # Store with encryption
        reference = await service.store(sample_data, sample_metadata)
        
        # Check that data was encrypted (stored data should be different)
        backend = FileSystemBackend(config)
        encrypted_data = await backend.retrieve(reference.id)
        assert encrypted_data != sample_data
    
    @pytest.mark.asyncio
    async def test_size_limit(self, binary_config, mock_db_session, sample_metadata):
        """Test size limit enforcement."""
        service = BinaryDataService(binary_config, mock_db_session)
        
        # Create data exceeding size limit
        large_data = b"x" * (binary_config.max_size_mb * 1024 * 1024 + 1)
        sample_metadata.size = len(large_data)
        
        with pytest.raises(BinaryDataStorageError) as exc_info:
            await service.store(large_data, sample_metadata)
        
        assert "exceeds maximum" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_expiration(self, binary_config, mock_db_session, sample_data, sample_metadata):
        """Test data expiration."""
        # Set short TTL
        binary_config.ttl_minutes = 1
        service = BinaryDataService(binary_config, mock_db_session)
        
        # Store data
        reference = await service.store(sample_data, sample_metadata)
        
        # Verify expires_at was set
        assert mock_db_session.add.called
        added_metadata = mock_db_session.add.call_args[0][0]
        assert added_metadata.expires_at is not None
        assert added_metadata.expires_at > datetime.now(timezone.utc)
    
    @pytest.mark.asyncio
    async def test_cleanup_expired(self, binary_config, mock_db_session):
        """Test cleaning up expired data."""
        service = BinaryDataService(binary_config, mock_db_session)
        
        # Mock expired entries
        expired_items = [
            Mock(id=uuid4(), expires_at=datetime.now(timezone.utc) - timedelta(hours=1)),
            Mock(id=uuid4(), expires_at=datetime.now(timezone.utc) - timedelta(hours=2)),
        ]
        
        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = expired_items
        mock_db_session.execute.return_value = mock_result
        
        # Mock backend delete
        with patch.object(service._backend, 'delete', return_value=True):
            count = await service.cleanup_expired()
        
        assert count == 2
    
    @pytest.mark.asyncio
    async def test_find_by_workflow(self, binary_config, mock_db_session):
        """Test finding data by workflow ID."""
        service = BinaryDataService(binary_config, mock_db_session)
        workflow_id = str(uuid4())
        
        # Mock database items
        db_items = [
            Mock(spec=BinaryDataMetadata, id=uuid4(), workflow_id=workflow_id),
            Mock(spec=BinaryDataMetadata, id=uuid4(), workflow_id=workflow_id),
        ]
        
        for item in db_items:
            item.filename = "test.txt"
            item.mime_type = "text/plain"
            item.size = 100
            item.checksum = "abc123"
            item.storage_path = "/path"
            item.storage_mode = BinaryDataMode.FILESYSTEM
            item.is_compressed = False
            item.is_encrypted = False
            item.created_at = datetime.now(timezone.utc)
            item.expires_at = None
            item.execution_id = None
            item.node_id = None
            item.extra_metadata = {}
        
        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = db_items
        mock_db_session.execute.return_value = mock_result
        
        items = await service.find_by_workflow(workflow_id)
        assert len(items) == 2
        assert all(item.workflow_id == workflow_id for item in items)


@pytest.mark.integration
class TestBinaryDataIntegration:
    """Integration tests for binary data system."""
    
    @pytest.mark.asyncio
    async def test_full_lifecycle(
        self, temp_directory, mock_db_session, sample_data, sample_metadata
    ):
        """Test complete binary data lifecycle."""
        config = BinaryDataConfig(
            mode=BinaryDataMode.FILESYSTEM,
            filesystem_path=temp_directory,
            enable_compression=True,
            enable_encryption=True,
            encryption_key="test-key-32-bytes-padding-needed",
        )
        service = BinaryDataService(config, mock_db_session)
        
        # Store data
        reference = await service.store(sample_data, sample_metadata)
        assert reference.id is not None
        
        # Mock database query for retrieve
        mock_result = Mock()
        mock_metadata = Mock(spec=BinaryDataMetadata)
        mock_metadata.id = reference.id
        mock_metadata.filename = sample_metadata.filename
        mock_metadata.mime_type = sample_metadata.mime_type
        mock_metadata.size = sample_metadata.size
        mock_metadata.checksum = service._calculate_checksum(sample_data)
        mock_metadata.storage_path = f"{temp_directory}/{reference.id[:2]}/{reference.id}"
        mock_metadata.storage_mode = BinaryDataMode.FILESYSTEM
        mock_metadata.is_compressed = True
        mock_metadata.is_encrypted = True
        mock_metadata.is_expired.return_value = False
        mock_metadata.created_at = datetime.now(timezone.utc)
        mock_metadata.expires_at = None
        mock_metadata.workflow_id = None
        mock_metadata.execution_id = None
        mock_metadata.node_id = None
        mock_metadata.extra_metadata = {}
        
        mock_result.scalar_one_or_none.return_value = mock_metadata
        mock_db_session.execute.return_value = mock_result
        
        # Retrieve data
        retrieved_data, retrieved_meta = await service.retrieve(reference.id)
        assert retrieved_data == sample_data
        
        # Check exists
        exists = await service.exists(reference.id)
        assert exists is True
        
        # Delete data
        deleted = await service.delete(reference.id)
        assert deleted is True
        
        # Verify deleted
        exists = await service.exists(reference.id)
        assert exists is False