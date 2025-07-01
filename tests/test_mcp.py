"""Test MCP (Model Context Protocol) implementation."""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock

from budflow.mcp import (
    MCPServer,
    MCPClient,
    MCPToolHandler,
    MCPResourceHandler,
    MCPPromptHandler,
    MCPRequest,
    MCPResponse,
    MCPNotification,
    MCPError,
    MCPErrorCode,
    MCPMethodType,
    MCPTool,
    MCPToolCall,
    MCPResource,
    MCPPrompt,
    mcp_server,
)


@pytest.fixture
def mcp_server_instance():
    """Create MCP server instance for testing."""
    return MCPServer(name="Test Server", version="1.0.0")


@pytest.fixture
def mcp_client_instance():
    """Create MCP client instance for testing."""
    return MCPClient(client_name="Test Client", client_version="1.0.0")


@pytest.mark.unit
class TestMCPServer:
    """Test MCP server functionality."""
    
    def test_server_initialization(self, mcp_server_instance):
        """Test server initialization."""
        server = mcp_server_instance
        
        assert server.name == "Test Server"
        assert server.version == "1.0.0"
        assert not server.is_initialized
        assert len(server.tool_handlers) > 0  # Built-in tools
        assert len(server.resource_handlers) > 0  # Built-in resources
        assert len(server.prompt_handlers) > 0  # Built-in prompts
    
    def test_register_tool(self, mcp_server_instance):
        """Test tool registration."""
        server = mcp_server_instance
        
        async def test_handler(param1: str, param2: int = 42):
            return {"result": f"{param1}_{param2}"}
        
        server.register_tool(
            "test_tool",
            test_handler,
            "Test tool description",
            [
                {"name": "param1", "type": "string", "required": True},
                {"name": "param2", "type": "number", "required": False}
            ]
        )
        
        assert "test_tool" in server.tool_handlers
        handler = server.tool_handlers["test_tool"]
        assert handler.name == "test_tool"
        assert handler.description == "Test tool description"
        assert len(handler.parameters) == 2
    
    def test_register_resource(self, mcp_server_instance):
        """Test resource registration."""
        server = mcp_server_instance
        
        async def test_resource_handler(uri: str):
            return {"resource": uri}
        
        server.register_resource(
            "test://resources/*",
            test_resource_handler,
            "Test resource",
            "application/json"
        )
        
        assert len([h for h in server.resource_handlers if h.uri_pattern == "test://resources/*"]) == 1
    
    def test_register_prompt(self, mcp_server_instance):
        """Test prompt registration."""
        server = mcp_server_instance
        
        async def test_prompt_handler(args: dict):
            return {"messages": [{"role": "user", "content": {"type": "text", "text": "Test prompt"}}]}
        
        server.register_prompt(
            "test_prompt",
            test_prompt_handler,
            "Test prompt description"
        )
        
        assert "test_prompt" in server.prompt_handlers
        handler = server.prompt_handlers["test_prompt"]
        assert handler.name == "test_prompt"
        assert handler.description == "Test prompt description"
    
    @pytest.mark.asyncio
    async def test_handle_message_parse_error(self, mcp_server_instance):
        """Test handling malformed JSON messages."""
        server = mcp_server_instance
        
        result = await server.handle_message("invalid json")
        assert result is not None
        
        response = json.loads(result)
        assert "error" in response
        assert response["error"]["code"] == MCPErrorCode.PARSE_ERROR
    
    @pytest.mark.asyncio
    async def test_handle_initialize_request(self, mcp_server_instance):
        """Test initialize request handling."""
        server = mcp_server_instance
        
        init_request = MCPRequest(
            id=1,
            method=MCPMethodType.INITIALIZE,
            params={
                "protocol_version": "2024-11-05",
                "capabilities": {
                    "roots": {"list_changed": False},
                    "sampling": {}
                },
                "client_info": {
                    "name": "Test Client",
                    "version": "1.0.0"
                }
            }
        )
        
        response = await server.handle_request(init_request)
        
        assert response.id == 1
        assert response.error is None
        assert response.result is not None
        assert "protocol_version" in response.result
        assert "capabilities" in response.result
        assert "server_info" in response.result
        
        # Server should be initialized
        assert server.is_initialized
    
    @pytest.mark.asyncio
    async def test_handle_list_tools_request(self, mcp_server_instance):
        """Test list tools request handling."""
        server = mcp_server_instance
        
        request = MCPRequest(id=1, method=MCPMethodType.LIST_TOOLS)
        response = await server.handle_request(request)
        
        assert response.id == 1
        assert response.error is None
        assert "tools" in response.result
        assert len(response.result["tools"]) > 0
    
    @pytest.mark.asyncio
    async def test_handle_call_tool_request(self, mcp_server_instance):
        """Test call tool request handling."""
        server = mcp_server_instance
        
        # Test with built-in tool
        request = MCPRequest(
            id=1,
            method=MCPMethodType.CALL_TOOL,
            params={
                "name": "list_workflows",
                "arguments": {}
            }
        )
        
        response = await server.handle_request(request)
        
        assert response.id == 1
        assert response.error is None
        assert "content" in response.result
    
    @pytest.mark.asyncio
    async def test_handle_call_nonexistent_tool(self, mcp_server_instance):
        """Test calling non-existent tool."""
        server = mcp_server_instance
        
        request = MCPRequest(
            id=1,
            method=MCPMethodType.CALL_TOOL,
            params={
                "name": "nonexistent_tool",
                "arguments": {}
            }
        )
        
        response = await server.handle_request(request)
        
        assert response.id == 1
        assert response.error is not None
        assert response.error.code == MCPErrorCode.TOOL_NOT_FOUND
    
    @pytest.mark.asyncio
    async def test_handle_list_resources_request(self, mcp_server_instance):
        """Test list resources request handling."""
        server = mcp_server_instance
        
        request = MCPRequest(id=1, method=MCPMethodType.LIST_RESOURCES)
        response = await server.handle_request(request)
        
        assert response.id == 1
        assert response.error is None
        assert "resources" in response.result
    
    @pytest.mark.asyncio
    async def test_handle_list_prompts_request(self, mcp_server_instance):
        """Test list prompts request handling."""
        server = mcp_server_instance
        
        request = MCPRequest(id=1, method=MCPMethodType.LIST_PROMPTS)
        response = await server.handle_request(request)
        
        assert response.id == 1
        assert response.error is None
        assert "prompts" in response.result
    
    @pytest.mark.asyncio
    async def test_handle_unknown_method(self, mcp_server_instance):
        """Test handling unknown method."""
        server = mcp_server_instance
        
        request = MCPRequest(id=1, method="unknown_method")
        response = await server.handle_request(request)
        
        assert response.id == 1
        assert response.error is not None
        assert response.error.code == MCPErrorCode.METHOD_NOT_FOUND
    
    @pytest.mark.asyncio
    async def test_handle_notification(self, mcp_server_instance):
        """Test notification handling."""
        server = mcp_server_instance
        
        notification = MCPNotification(
            method=MCPMethodType.LOG,
            params={
                "level": "info",
                "data": "Test log message"
            }
        )
        
        # Should not raise exception
        await server.handle_notification(notification)


@pytest.mark.unit
class TestMCPClient:
    """Test MCP client functionality."""
    
    def test_client_initialization(self, mcp_client_instance):
        """Test client initialization."""
        client = mcp_client_instance
        
        assert client.client_name == "Test Client"
        assert client.client_version == "1.0.0"
        assert not client.is_connected
        assert not client.is_initialized
        assert len(client.pending_requests) == 0
    
    def test_generate_request_id(self, mcp_client_instance):
        """Test request ID generation."""
        client = mcp_client_instance
        
        id1 = client._generate_request_id()
        id2 = client._generate_request_id()
        
        assert id1 != id2
        assert id2 == id1 + 1
    
    @pytest.mark.asyncio
    async def test_send_request_not_connected(self, mcp_client_instance):
        """Test sending request when not connected."""
        client = mcp_client_instance
        
        with pytest.raises(Exception):  # MCPConnectionError
            await client.send_request("test_method")
    
    @pytest.mark.asyncio
    async def test_process_response_message(self, mcp_client_instance):
        """Test processing response message."""
        client = mcp_client_instance
        
        # Create a pending request
        request_id = 123
        future = asyncio.Future()
        client.pending_requests[request_id] = future
        
        # Simulate response message
        response_data = {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"test": "data"}
        }
        
        await client._process_message(json.dumps(response_data))
        
        # Future should be resolved
        assert future.done()
        response = future.result()
        assert response.id == request_id
        assert response.result == {"test": "data"}
        assert response.error is None
    
    @pytest.mark.asyncio
    async def test_process_notification_message(self, mcp_client_instance):
        """Test processing notification message."""
        client = mcp_client_instance
        
        # Simulate notification message
        notification_data = {
            "jsonrpc": "2.0",
            "method": "logging/message",
            "params": {
                "level": "info",
                "data": "Test notification"
            }
        }
        
        # Should not raise exception
        await client._process_message(json.dumps(notification_data))
    
    @pytest.mark.asyncio
    async def test_call_tool_not_initialized(self, mcp_client_instance):
        """Test calling tool when not initialized."""
        client = mcp_client_instance
        
        with pytest.raises(Exception):  # MCPConnectionError
            await client.call_tool("test_tool")


@pytest.mark.unit
class TestMCPTypes:
    """Test MCP type models."""
    
    def test_mcp_request_creation(self):
        """Test MCP request creation."""
        request = MCPRequest(
            id="test-123",
            method="test_method",
            params={"param1": "value1"}
        )
        
        assert request.id == "test-123"
        assert request.method == "test_method"
        assert request.params == {"param1": "value1"}
        assert request.jsonrpc == "2.0"
    
    def test_mcp_response_creation(self):
        """Test MCP response creation."""
        response = MCPResponse(
            id="test-123",
            result={"result_data": "test"}
        )
        
        assert response.id == "test-123"
        assert response.result == {"result_data": "test"}
        assert response.error is None
        assert response.jsonrpc == "2.0"
    
    def test_mcp_error_response(self):
        """Test MCP error response creation."""
        error = MCPError(
            code=MCPErrorCode.INVALID_REQUEST,
            message="Test error message",
            data={"extra": "info"}
        )
        
        response = MCPResponse(
            id="test-123",
            error=error
        )
        
        assert response.id == "test-123"
        assert response.result is None
        assert response.error is not None
        assert response.error.code == MCPErrorCode.INVALID_REQUEST
        assert response.error.message == "Test error message"
    
    def test_mcp_tool_creation(self):
        """Test MCP tool creation."""
        tool = MCPTool(
            name="test_tool",
            description="Test tool description",
            parameters=[
                {"name": "param1", "type": "string", "required": True}
            ]
        )
        
        assert tool.name == "test_tool"
        assert tool.description == "Test tool description"
        assert len(tool.parameters) == 1
    
    def test_mcp_tool_call_creation(self):
        """Test MCP tool call creation."""
        tool_call = MCPToolCall(
            name="test_tool",
            arguments={"param1": "value1", "param2": 42}
        )
        
        assert tool_call.name == "test_tool"
        assert tool_call.arguments == {"param1": "value1", "param2": 42}
    
    def test_mcp_resource_creation(self):
        """Test MCP resource creation."""
        resource = MCPResource(
            uri="budflow://workflows/test-workflow",
            name="Test Workflow",
            description="Test workflow resource",
            mime_type="application/json"
        )
        
        assert resource.uri == "budflow://workflows/test-workflow"
        assert resource.name == "Test Workflow"
        assert resource.mime_type == "application/json"
    
    def test_mcp_prompt_creation(self):
        """Test MCP prompt creation."""
        prompt = MCPPrompt(
            name="test_prompt",
            description="Test prompt description",
            arguments=[
                {"name": "input", "type": "string", "required": True}
            ]
        )
        
        assert prompt.name == "test_prompt"
        assert prompt.description == "Test prompt description"
        assert len(prompt.arguments) == 1


@pytest.mark.integration
class TestMCPIntegration:
    """Integration tests for MCP components."""
    
    @pytest.mark.asyncio
    async def test_server_tool_execution(self, mcp_server_instance):
        """Test end-to-end tool execution."""
        server = mcp_server_instance
        
        # Register a custom tool
        async def custom_tool(input_text: str, count: int = 1):
            return {"output": input_text * count}
        
        server.register_tool(
            "custom_tool",
            custom_tool,
            "Custom test tool",
            [
                {"name": "input_text", "type": "string", "required": True},
                {"name": "count", "type": "number", "required": False}
            ]
        )
        
        # Test tool call
        request = MCPRequest(
            id=1,
            method=MCPMethodType.CALL_TOOL,
            params={
                "name": "custom_tool",
                "arguments": {"input_text": "hello", "count": 3}
            }
        )
        
        response = await server.handle_request(request)
        
        assert response.error is None
        assert "content" in response.result
        content = response.result["content"][0]
        assert "hello" in content["text"]
    
    @pytest.mark.asyncio
    async def test_server_resource_access(self, mcp_server_instance):
        """Test resource access."""
        server = mcp_server_instance
        
        # Test built-in workflow resource
        request = MCPRequest(
            id=1,
            method=MCPMethodType.READ_RESOURCE,
            params={"uri": "budflow://workflows/test-workflow"}
        )
        
        response = await server.handle_request(request)
        
        assert response.error is None
        assert "uri" in response.result
        assert response.result["uri"] == "budflow://workflows/test-workflow"
    
    @pytest.mark.asyncio
    async def test_server_prompt_access(self, mcp_server_instance):
        """Test prompt access."""
        server = mcp_server_instance
        
        # Test built-in create workflow prompt
        request = MCPRequest(
            id=1,
            method=MCPMethodType.GET_PROMPT,
            params={
                "name": "create_workflow",
                "arguments": {
                    "description": "Process customer data",
                    "purpose": "Data validation and transformation"
                }
            }
        )
        
        response = await server.handle_request(request)
        
        assert response.error is None
        assert "description" in response.result
        assert "messages" in response.result
        assert len(response.result["messages"]) > 0


@pytest.mark.unit
class TestGlobalMCPServer:
    """Test global MCP server instance."""
    
    def test_global_server_exists(self):
        """Test that global MCP server exists."""
        assert mcp_server is not None
        assert isinstance(mcp_server, MCPServer)
        assert mcp_server.name == "BudFlow"
    
    def test_global_server_has_builtin_tools(self):
        """Test that global server has built-in tools."""
        assert len(mcp_server.tool_handlers) > 0
        
        # Check for expected built-in tools
        expected_tools = ["execute_workflow", "list_workflows", "get_workflow_status", "list_nodes"]
        for tool_name in expected_tools:
            assert tool_name in mcp_server.tool_handlers
    
    def test_global_server_has_builtin_resources(self):
        """Test that global server has built-in resources."""
        assert len(mcp_server.resource_handlers) > 0
    
    def test_global_server_has_builtin_prompts(self):
        """Test that global server has built-in prompts."""
        assert len(mcp_server.prompt_handlers) > 0