"""MCP Client implementation for BudFlow."""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

import aiohttp
import structlog
from websockets import connect, ConnectionClosed, WebSocketException

from .types import (
    MCPRequest,
    MCPResponse,
    MCPNotification,
    MCPError,
    MCPMethodType,
    MCPInitializeParams,
    MCPInitializeResult,
    MCPClientCapabilities,
    MCPTool,
    MCPToolCall,
    MCPToolResult,
    MCPResource,
    MCPResourceContent,
    MCPPrompt,
    MCPGetPromptResult,
)

logger = structlog.get_logger()


class MCPConnectionError(Exception):
    """MCP connection error."""
    pass


class MCPRequestError(Exception):
    """MCP request error."""
    
    def __init__(self, message: str, code: int = None, data: Any = None):
        super().__init__(message)
        self.code = code
        self.data = data


class MCPClient:
    """MCP Client for connecting to MCP servers."""
    
    def __init__(self, client_name: str = "BudFlow Client", client_version: str = "1.0.0"):
        self.client_name = client_name
        self.client_version = client_version
        self.logger = logger.bind(component="mcp_client")
        
        # Connection state
        self.websocket = None
        self.is_connected = False
        self.is_initialized = False
        
        # Server capabilities
        self.server_capabilities = None
        self.server_info = None
        
        # Request tracking
        self.pending_requests: Dict[Union[str, int], asyncio.Future] = {}
        self.request_id_counter = 0
        
        # Available tools, resources, prompts
        self.available_tools: List[MCPTool] = []
        self.available_resources: List[MCPResource] = []
        self.available_prompts: List[MCPPrompt] = []
    
    async def connect_websocket(self, uri: str):
        """Connect to MCP server via WebSocket."""
        try:
            self.websocket = await connect(uri)
            self.is_connected = True
            self.logger.info("Connected to MCP server", uri=uri)
            
            # Start message handling task
            asyncio.create_task(self._handle_messages())
            
        except Exception as e:
            self.logger.error("Failed to connect to MCP server", uri=uri, error=str(e))
            raise MCPConnectionError(f"Failed to connect to {uri}: {str(e)}")
    
    async def connect_stdio(self, command: List[str]):
        """Connect to MCP server via stdio."""
        # For stdio connections, we would use subprocess
        # This is a placeholder for now
        raise NotImplementedError("STDIO connection not implemented yet")
    
    async def disconnect(self):
        """Disconnect from MCP server."""
        if self.websocket:
            await self.websocket.close()
            self.is_connected = False
            self.is_initialized = False
            self.logger.info("Disconnected from MCP server")
    
    async def initialize(self) -> MCPInitializeResult:
        """Initialize connection with MCP server."""
        if not self.is_connected:
            raise MCPConnectionError("Not connected to server")
        
        capabilities = MCPClientCapabilities(
            roots={
                "list_changed": False
            },
            sampling={}
        )
        
        params = MCPInitializeParams(
            protocol_version="2024-11-05",
            capabilities=capabilities,
            client_info={
                "name": self.client_name,
                "version": self.client_version
            }
        )
        
        response = await self.send_request(MCPMethodType.INITIALIZE, params.model_dump())
        
        if response.error:
            raise MCPRequestError(
                response.error.message,
                response.error.code,
                response.error.data
            )
        
        result = MCPInitializeResult.model_validate(response.result)
        self.server_capabilities = result.capabilities
        self.server_info = result.server_info
        self.is_initialized = True
        
        self.logger.info("MCP client initialized", server_info=result.server_info)
        
        # Load available tools, resources, and prompts
        await self._load_server_capabilities()
        
        return result
    
    async def _load_server_capabilities(self):
        """Load available tools, resources, and prompts from server."""
        try:
            # Load tools
            if self.server_capabilities and self.server_capabilities.tools:
                tools_response = await self.send_request(MCPMethodType.LIST_TOOLS)
                if not tools_response.error and tools_response.result:
                    tools_data = tools_response.result.get("tools", [])
                    self.available_tools = [MCPTool.model_validate(tool) for tool in tools_data]
            
            # Load resources
            if self.server_capabilities and self.server_capabilities.resources:
                resources_response = await self.send_request(MCPMethodType.LIST_RESOURCES)
                if not resources_response.error and resources_response.result:
                    resources_data = resources_response.result.get("resources", [])
                    self.available_resources = [MCPResource.model_validate(resource) for resource in resources_data]
            
            # Load prompts
            if self.server_capabilities and self.server_capabilities.prompts:
                prompts_response = await self.send_request(MCPMethodType.LIST_PROMPTS)
                if not prompts_response.error and prompts_response.result:
                    prompts_data = prompts_response.result.get("prompts", [])
                    self.available_prompts = [MCPPrompt.model_validate(prompt) for prompt in prompts_data]
            
            self.logger.info(
                "Loaded server capabilities",
                tools_count=len(self.available_tools),
                resources_count=len(self.available_resources),
                prompts_count=len(self.available_prompts)
            )
            
        except Exception as e:
            self.logger.warning("Failed to load server capabilities", error=str(e))
    
    async def send_request(self, method: str, params: Dict[str, Any] = None) -> MCPResponse:
        """Send request to MCP server."""
        if not self.is_connected:
            raise MCPConnectionError("Not connected to server")
        
        request_id = self._generate_request_id()
        request = MCPRequest(id=request_id, method=method, params=params)
        
        # Create future for response
        future = asyncio.Future()
        self.pending_requests[request_id] = future
        
        try:
            # Send request
            await self.websocket.send(request.model_dump_json())
            
            # Wait for response with timeout
            response = await asyncio.wait_for(future, timeout=30.0)
            return response
            
        except asyncio.TimeoutError:
            if request_id in self.pending_requests:
                del self.pending_requests[request_id]
            raise MCPRequestError(f"Request timeout for method: {method}")
        except Exception as e:
            if request_id in self.pending_requests:
                del self.pending_requests[request_id]
            raise MCPRequestError(f"Request failed: {str(e)}")
    
    async def send_notification(self, method: str, params: Dict[str, Any] = None):
        """Send notification to MCP server (no response expected)."""
        if not self.is_connected:
            raise MCPConnectionError("Not connected to server")
        
        notification = MCPNotification(method=method, params=params)
        await self.websocket.send(notification.model_dump_json())
    
    async def _handle_messages(self):
        """Handle incoming messages from server."""
        try:
            async for message in self.websocket:
                await self._process_message(message)
        except ConnectionClosed:
            self.logger.info("WebSocket connection closed")
            self.is_connected = False
            self.is_initialized = False
        except WebSocketException as e:
            self.logger.error("WebSocket error", error=str(e))
            self.is_connected = False
            self.is_initialized = False
        except Exception as e:
            self.logger.error("Error handling messages", error=str(e))
    
    async def _process_message(self, message: str):
        """Process incoming message."""
        try:
            data = json.loads(message)
            
            if "id" in data and data["id"] in self.pending_requests:
                # It's a response to our request
                response = MCPResponse.model_validate(data)
                future = self.pending_requests.pop(response.id)
                future.set_result(response)
            elif "method" in data and "id" not in data:
                # It's a notification from server
                notification = MCPNotification.model_validate(data)
                await self._handle_server_notification(notification)
            else:
                self.logger.warning("Unknown message format", data=data)
                
        except json.JSONDecodeError as e:
            self.logger.error("Failed to parse JSON message", error=str(e))
        except Exception as e:
            self.logger.error("Error processing message", error=str(e))
    
    async def _handle_server_notification(self, notification: MCPNotification):
        """Handle notification from server."""
        method = notification.method
        params = notification.params or {}
        
        if method == MCPMethodType.LOG:
            # Handle server log messages
            level = params.get("level", "info")
            data = params.get("data", "")
            self.logger.info("Server log", level=level, message=data)
        elif method == MCPMethodType.PROGRESS_NOTIFICATION:
            # Handle progress notifications
            token = params.get("progress_token")
            progress = params.get("progress", 0.0)
            self.logger.info("Server progress", token=token, progress=progress)
        else:
            self.logger.info("Server notification", method=method, params=params)
    
    def _generate_request_id(self) -> int:
        """Generate unique request ID."""
        self.request_id_counter += 1
        return self.request_id_counter
    
    # High-level API methods
    
    async def list_tools(self) -> List[MCPTool]:
        """List available tools."""
        if not self.is_initialized:
            raise MCPConnectionError("Client not initialized")
        
        return self.available_tools
    
    async def call_tool(self, tool_name: str, arguments: Dict[str, Any] = None) -> MCPToolResult:
        """Call a tool on the server."""
        if not self.is_initialized:
            raise MCPConnectionError("Client not initialized")
        
        tool_call = MCPToolCall(name=tool_name, arguments=arguments or {})
        response = await self.send_request(MCPMethodType.CALL_TOOL, tool_call.model_dump())
        
        if response.error:
            raise MCPRequestError(
                response.error.message,
                response.error.code,
                response.error.data
            )
        
        return MCPToolResult.model_validate(response.result)
    
    async def list_resources(self) -> List[MCPResource]:
        """List available resources."""
        if not self.is_initialized:
            raise MCPConnectionError("Client not initialized")
        
        return self.available_resources
    
    async def read_resource(self, uri: str) -> MCPResourceContent:
        """Read a resource from the server."""
        if not self.is_initialized:
            raise MCPConnectionError("Client not initialized")
        
        response = await self.send_request(MCPMethodType.READ_RESOURCE, {"uri": uri})
        
        if response.error:
            raise MCPRequestError(
                response.error.message,
                response.error.code,
                response.error.data
            )
        
        return MCPResourceContent.model_validate(response.result)
    
    async def list_prompts(self) -> List[MCPPrompt]:
        """List available prompts."""
        if not self.is_initialized:
            raise MCPConnectionError("Client not initialized")
        
        return self.available_prompts
    
    async def get_prompt(self, prompt_name: str, arguments: Dict[str, Any] = None) -> MCPGetPromptResult:
        """Get a prompt from the server."""
        if not self.is_initialized:
            raise MCPConnectionError("Client not initialized")
        
        params = {"name": prompt_name}
        if arguments:
            params["arguments"] = arguments
        
        response = await self.send_request(MCPMethodType.GET_PROMPT, params)
        
        if response.error:
            raise MCPRequestError(
                response.error.message,
                response.error.code,
                response.error.data
            )
        
        return MCPGetPromptResult.model_validate(response.result)
    
    # BudFlow-specific methods
    
    async def execute_workflow(self, workflow_id: str, input_data: Dict[str, Any] = None, execution_mode: str = "manual") -> Dict[str, Any]:
        """Execute a BudFlow workflow."""
        params = {
            "workflow_id": workflow_id,
            "input_data": input_data,
            "execution_mode": execution_mode
        }
        
        response = await self.send_request(MCPMethodType.EXECUTE_WORKFLOW, params)
        
        if response.error:
            raise MCPRequestError(
                response.error.message,
                response.error.code,
                response.error.data
            )
        
        return response.result
    
    async def get_workflow_status(self, execution_id: str) -> Dict[str, Any]:
        """Get workflow execution status."""
        response = await self.send_request(MCPMethodType.GET_WORKFLOW_STATUS, {"execution_id": execution_id})
        
        if response.error:
            raise MCPRequestError(
                response.error.message,
                response.error.code,
                response.error.data
            )
        
        return response.result
    
    async def list_workflows(self) -> Dict[str, Any]:
        """List available workflows."""
        response = await self.send_request(MCPMethodType.LIST_WORKFLOWS)
        
        if response.error:
            raise MCPRequestError(
                response.error.message,
                response.error.code,
                response.error.data
            )
        
        return response.result


# Convenience functions for common operations

async def create_mcp_client(server_uri: str, client_name: str = "BudFlow Client") -> MCPClient:
    """Create and initialize MCP client."""
    client = MCPClient(client_name=client_name)
    await client.connect_websocket(server_uri)
    await client.initialize()
    return client


async def execute_workflow_via_mcp(server_uri: str, workflow_id: str, input_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """Execute workflow via MCP client (convenience function)."""
    client = await create_mcp_client(server_uri)
    try:
        result = await client.execute_workflow(workflow_id, input_data)
        return result
    finally:
        await client.disconnect()