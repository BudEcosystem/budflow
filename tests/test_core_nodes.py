"""Test core node implementations with TDD methodology."""

import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone
from uuid import uuid4

from budflow.nodes.actions.http import HTTPNode
from budflow.nodes.actions.email import EmailNode
from budflow.nodes.actions.database import DatabaseNode, PostgreSQLNode, MySQLNode, MongoDBNode
from budflow.nodes.actions.file import FileNode
from budflow.nodes.base import NodeExecutionContext
from budflow.workflows.models import WorkflowNode, NodeExecution, NodeExecutionStatus


class TestHTTPNode:
    """Test HTTP node implementation."""
    
    @pytest.fixture
    def http_node(self):
        """Create HTTP node instance."""
        node_data = WorkflowNode(
            id=1,
            name="HTTP Request",
            type="http",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "method": "GET",
                "url": "https://api.example.com/users",
                "headers": {"Content-Type": "application/json"},
                "authentication": "none",
                "timeout": 30
            }
        )
        return HTTPNode(node_data)
    
    @pytest.fixture
    def execution_context(self):
        """Create execution context."""
        node_execution = NodeExecution(
            id=1,
            workflow_execution_id=1,
            node_id=1,
            status=NodeExecutionStatus.RUNNING,
            input_data={},
            output_data={},
            started_at=datetime.now(timezone.utc)
        )
        return NodeExecutionContext(
            node_execution=node_execution,
            workflow_data={},
            credentials={},
            variables={}
        )
    
    @pytest.mark.asyncio
    async def test_http_get_request(self, http_node, execution_context):
        """Test HTTP GET request."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            # Mock successful response
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.headers = {"Content-Type": "application/json"}
            mock_response.json = AsyncMock(return_value={"users": [{"id": 1, "name": "John"}]})
            mock_response.text = AsyncMock(return_value='{"users": [{"id": 1, "name": "John"}]}')
            mock_get.return_value.__aenter__.return_value = mock_response
            
            # Execute node
            result = await http_node.execute(execution_context)
            
            # Verify result
            assert result["success"] is True
            assert result["statusCode"] == 200
            assert result["data"]["users"][0]["name"] == "John"
            assert "responseHeaders" in result
    
    @pytest.mark.asyncio
    async def test_http_post_request_with_body(self, execution_context):
        """Test HTTP POST request with JSON body."""
        node_data = WorkflowNode(
            id=1,
            name="HTTP POST",
            type="http",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "method": "POST",
                "url": "https://api.example.com/users",
                "headers": {"Content-Type": "application/json"},
                "body": {"name": "Jane", "email": "jane@example.com"},
                "bodyType": "json"
            }
        )
        http_node = HTTPNode(node_data)
        
        with patch('aiohttp.ClientSession.post') as mock_post:
            mock_response = AsyncMock()
            mock_response.status = 201
            mock_response.json = AsyncMock(return_value={"id": 2, "name": "Jane"})
            mock_post.return_value.__aenter__.return_value = mock_response
            
            result = await http_node.execute(execution_context)
            
            assert result["success"] is True
            assert result["statusCode"] == 201
            mock_post.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_http_authentication_bearer(self, execution_context):
        """Test HTTP request with Bearer token authentication."""
        node_data = WorkflowNode(
            id=1,
            name="HTTP Auth",
            type="http",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "method": "GET",
                "url": "https://api.example.com/protected",
                "authentication": "bearer",
                "bearerToken": "abc123"
            }
        )
        http_node = HTTPNode(node_data)
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={"protected": "data"})
            mock_get.return_value.__aenter__.return_value = mock_response
            
            result = await http_node.execute(execution_context)
            
            # Verify Authorization header was set
            args, kwargs = mock_get.call_args
            assert kwargs["headers"]["Authorization"] == "Bearer abc123"
    
    @pytest.mark.asyncio
    async def test_http_error_handling(self, http_node, execution_context):
        """Test HTTP error handling."""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_response.text = AsyncMock(return_value="Not Found")
            mock_get.return_value.__aenter__.return_value = mock_response
            
            result = await http_node.execute(execution_context)
            
            assert result["success"] is False
            assert result["statusCode"] == 404
            assert "Not Found" in result["error"]


class TestEmailNode:
    """Test Email node implementation."""
    
    @pytest.fixture
    def email_node(self):
        """Create Email node instance."""
        node_data = WorkflowNode(
            id=1,
            name="Send Email",
            type="email",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "to": "user@example.com",
                "subject": "Test Email",
                "body": "This is a test email",
                "bodyType": "text"
            }
        )
        return EmailNode(node_data)
    
    @pytest.fixture
    def execution_context(self):
        """Create execution context with email credentials."""
        node_execution = NodeExecution(
            id=1,
            workflow_execution_id=1,
            node_id=1,
            status=NodeExecutionStatus.RUNNING,
            input_data={},
            output_data={},
            started_at=datetime.now(timezone.utc)
        )
        return NodeExecutionContext(
            node_execution=node_execution,
            workflow_data={},
            credentials={
                "smtp_host": "smtp.gmail.com",
                "smtp_port": 587,
                "smtp_username": "test@gmail.com",
                "smtp_password": "app_password",
                "smtp_use_tls": True
            },
            variables={}
        )
    
    @pytest.mark.asyncio
    async def test_send_text_email(self, email_node, execution_context):
        """Test sending text email."""
        with patch('aiosmtplib.send') as mock_send:
            mock_send.return_value = None
            
            result = await email_node.execute(execution_context)
            
            assert result["success"] is True
            assert result["messageId"] is not None
            assert result["recipient"] == "user@example.com"
            mock_send.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_send_html_email(self, execution_context):
        """Test sending HTML email."""
        node_data = WorkflowNode(
            id=1,
            name="Send HTML Email",
            type="email",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "to": "user@example.com",
                "subject": "HTML Test",
                "body": "<h1>Hello World</h1>",
                "bodyType": "html"
            }
        )
        email_node = EmailNode(node_data)
        
        with patch('aiosmtplib.send') as mock_send:
            mock_send.return_value = None
            
            result = await email_node.execute(execution_context)
            
            assert result["success"] is True
            mock_send.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_email_with_attachments(self, execution_context):
        """Test sending email with attachments."""
        node_data = WorkflowNode(
            id=1,
            name="Email with Attachment",
            type="email",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "to": "user@example.com",
                "subject": "With Attachment",
                "body": "See attachment",
                "attachments": [
                    {
                        "filename": "test.txt",
                        "content": "dGVzdCBjb250ZW50",  # base64 encoded "test content"
                        "contentType": "text/plain"
                    }
                ]
            }
        )
        email_node = EmailNode(node_data)
        
        with patch('aiosmtplib.send') as mock_send:
            mock_send.return_value = None
            
            result = await email_node.execute(execution_context)
            
            assert result["success"] is True
            assert result["attachmentCount"] == 1


class TestDatabaseNodes:
    """Test Database node implementations."""
    
    @pytest.fixture
    def postgres_node(self):
        """Create PostgreSQL node instance."""
        node_data = WorkflowNode(
            id=1,
            name="PostgreSQL Query",
            type="postgresql",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "operation": "select",
                "query": "SELECT * FROM users WHERE id = $1",
                "parameters": [1]
            }
        )
        return PostgreSQLNode(node_data)
    
    @pytest.fixture
    def execution_context(self):
        """Create execution context with database credentials."""
        node_execution = NodeExecution(
            id=1,
            workflow_execution_id=1,
            node_id=1,
            status=NodeExecutionStatus.RUNNING,
            input_data={},
            output_data={},
            started_at=datetime.now(timezone.utc)
        )
        return NodeExecutionContext(
            node_execution=node_execution,
            workflow_data={},
            credentials={
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "username": "testuser",
                "password": "testpass"
            },
            variables={}
        )
    
    @pytest.mark.asyncio
    async def test_postgresql_select_query(self, postgres_node, execution_context):
        """Test PostgreSQL SELECT query."""
        with patch('asyncpg.connect') as mock_connect:
            mock_connection = AsyncMock()
            mock_connection.fetch.return_value = [
                {"id": 1, "name": "John", "email": "john@example.com"}
            ]
            mock_connect.return_value.__aenter__.return_value = mock_connection
            
            result = await postgres_node.execute(execution_context)
            
            assert result["success"] is True
            assert len(result["data"]) == 1
            assert result["data"][0]["name"] == "John"
            assert result["rowCount"] == 1
    
    @pytest.mark.asyncio
    async def test_postgresql_insert_query(self, execution_context):
        """Test PostgreSQL INSERT query."""
        node_data = WorkflowNode(
            id=1,
            name="PostgreSQL Insert",
            type="postgresql",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "operation": "insert",
                "query": "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
                "parameters": ["Jane", "jane@example.com"]
            }
        )
        postgres_node = PostgreSQLNode(node_data)
        
        with patch('asyncpg.connect') as mock_connect:
            mock_connection = AsyncMock()
            mock_connection.fetch.return_value = [{"id": 2}]
            mock_connect.return_value.__aenter__.return_value = mock_connection
            
            result = await postgres_node.execute(execution_context)
            
            assert result["success"] is True
            assert result["data"][0]["id"] == 2
    
    @pytest.mark.asyncio
    async def test_mongodb_find_query(self, execution_context):
        """Test MongoDB find query."""
        node_data = WorkflowNode(
            id=1,
            name="MongoDB Find",
            type="mongodb",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "operation": "find",
                "collection": "users",
                "query": {"status": "active"},
                "limit": 10
            }
        )
        mongo_node = MongoDBNode(node_data)
        
        with patch('motor.motor_asyncio.AsyncIOMotorClient') as mock_client:
            mock_collection = AsyncMock()
            mock_collection.find.return_value.to_list.return_value = [
                {"_id": "507f1f77bcf86cd799439011", "name": "John", "status": "active"}
            ]
            mock_client.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
            
            result = await mongo_node.execute(execution_context)
            
            assert result["success"] is True
            assert len(result["data"]) == 1
            assert result["data"][0]["name"] == "John"


class TestFileNode:
    """Test File node implementation."""
    
    @pytest.fixture
    def file_node(self):
        """Create File node instance."""
        node_data = WorkflowNode(
            id=1,
            name="Read File",
            type="file",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "operation": "read",
                "path": "/tmp/test.txt",
                "encoding": "utf-8"
            }
        )
        return FileNode(node_data)
    
    @pytest.fixture
    def execution_context(self):
        """Create execution context."""
        node_execution = NodeExecution(
            id=1,
            workflow_execution_id=1,
            node_id=1,
            status=NodeExecutionStatus.RUNNING,
            input_data={},
            output_data={},
            started_at=datetime.now(timezone.utc)
        )
        return NodeExecutionContext(
            node_execution=node_execution,
            workflow_data={},
            credentials={},
            variables={}
        )
    
    @pytest.mark.asyncio
    async def test_read_text_file(self, file_node, execution_context):
        """Test reading text file."""
        with patch('aiofiles.open') as mock_open:
            mock_file = AsyncMock()
            mock_file.read.return_value = "Hello, World!"
            mock_open.return_value.__aenter__.return_value = mock_file
            
            result = await file_node.execute(execution_context)
            
            assert result["success"] is True
            assert result["content"] == "Hello, World!"
            assert result["encoding"] == "utf-8"
    
    @pytest.mark.asyncio
    async def test_write_text_file(self, execution_context):
        """Test writing text file."""
        node_data = WorkflowNode(
            id=1,
            name="Write File",
            type="file",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "operation": "write",
                "path": "/tmp/output.txt",
                "content": "Hello, File!",
                "encoding": "utf-8"
            }
        )
        file_node = FileNode(node_data)
        
        with patch('aiofiles.open') as mock_open:
            mock_file = AsyncMock()
            mock_open.return_value.__aenter__.return_value = mock_file
            
            result = await file_node.execute(execution_context)
            
            assert result["success"] is True
            assert result["bytesWritten"] > 0
            mock_file.write.assert_called_once_with("Hello, File!")
    
    @pytest.mark.asyncio
    async def test_delete_file(self, execution_context):
        """Test deleting file."""
        node_data = WorkflowNode(
            id=1,
            name="Delete File",
            type="file",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "operation": "delete",
                "path": "/tmp/to_delete.txt"
            }
        )
        file_node = FileNode(node_data)
        
        with patch('os.remove') as mock_remove:
            mock_remove.return_value = None
            
            result = await file_node.execute(execution_context)
            
            assert result["success"] is True
            assert result["deleted"] is True
            mock_remove.assert_called_once_with("/tmp/to_delete.txt")
    
    @pytest.mark.asyncio
    async def test_list_directory(self, execution_context):
        """Test listing directory contents."""
        node_data = WorkflowNode(
            id=1,
            name="List Directory",
            type="file",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "operation": "list",
                "path": "/tmp",
                "recursive": False
            }
        )
        file_node = FileNode(node_data)
        
        with patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = ["file1.txt", "file2.txt", "subdir"]
            
            with patch('os.path.isfile') as mock_isfile:
                mock_isfile.side_effect = lambda x: not x.endswith("subdir")
                
                result = await file_node.execute(execution_context)
                
                assert result["success"] is True
                assert len(result["files"]) == 2
                assert len(result["directories"]) == 1


class TestNodeErrorHandling:
    """Test error handling across all node types."""
    
    @pytest.mark.asyncio
    async def test_http_connection_error(self):
        """Test HTTP node connection error handling."""
        node_data = WorkflowNode(
            id=1,
            name="HTTP Error",
            type="http",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "method": "GET",
                "url": "https://nonexistent.domain.com"
            }
        )
        http_node = HTTPNode(node_data)
        
        execution_context = NodeExecutionContext(
            node_execution=NodeExecution(
                id=1,
                workflow_execution_id=1,
                node_id=1,
                status=NodeExecutionStatus.RUNNING
            ),
            workflow_data={},
            credentials={},
            variables={}
        )
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = Exception("Connection failed")
            
            result = await http_node.execute(execution_context)
            
            assert result["success"] is False
            assert "Connection failed" in result["error"]
    
    @pytest.mark.asyncio
    async def test_email_smtp_error(self):
        """Test Email node SMTP error handling."""
        node_data = WorkflowNode(
            id=1,
            name="Email Error",
            type="email",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "to": "invalid@domain",
                "subject": "Test",
                "body": "Test"
            }
        )
        email_node = EmailNode(node_data)
        
        execution_context = NodeExecutionContext(
            node_execution=NodeExecution(
                id=1,
                workflow_execution_id=1,
                node_id=1,
                status=NodeExecutionStatus.RUNNING
            ),
            workflow_data={},
            credentials={
                "smtp_host": "smtp.invalid.com",
                "smtp_port": 587,
                "smtp_username": "test",
                "smtp_password": "test"
            },
            variables={}
        )
        
        with patch('aiosmtplib.send') as mock_send:
            mock_send.side_effect = Exception("SMTP server not found")
            
            result = await email_node.execute(execution_context)
            
            assert result["success"] is False
            assert "SMTP server not found" in result["error"]
    
    @pytest.mark.asyncio
    async def test_database_connection_error(self):
        """Test Database node connection error handling."""
        node_data = WorkflowNode(
            id=1,
            name="DB Error",
            type="postgresql",
            uuid=str(uuid4()),
            workflow_id=1,
            parameters={
                "operation": "select",
                "query": "SELECT * FROM users"
            }
        )
        postgres_node = PostgreSQLNode(node_data)
        
        execution_context = NodeExecutionContext(
            node_execution=NodeExecution(
                id=1,
                workflow_execution_id=1,
                node_id=1,
                status=NodeExecutionStatus.RUNNING
            ),
            workflow_data={},
            credentials={
                "host": "invalid.host.com",
                "port": 5432,
                "database": "testdb",
                "username": "testuser",
                "password": "testpass"
            },
            variables={}
        )
        
        with patch('asyncpg.connect') as mock_connect:
            mock_connect.side_effect = Exception("Connection refused")
            
            result = await postgres_node.execute(execution_context)
            
            assert result["success"] is False
            assert "Connection refused" in result["error"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])