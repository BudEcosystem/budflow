"""MCP Server implementation for BudFlow."""

import asyncio
import json
import logging
import traceback
from typing import Any, Callable, Dict, List, Optional, Union
from uuid import uuid4

import structlog
from fastapi import WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from .types import (
    MCPRequest,
    MCPResponse,
    MCPNotification,
    MCPError,
    MCPErrorCode,
    MCPMethodType,
    MCPInitializeParams,
    MCPInitializeResult,
    MCPServerCapabilities,
    MCPTool,
    MCPToolCall,
    MCPToolResult,
    MCPResource,
    MCPResourceContent,
    MCPPrompt,
    MCPGetPromptResult,
    MCPLogMessage,
    MCPProgressNotification,
    BudFlowWorkflowParams,
    BudFlowWorkflowResult,
    BudFlowWorkflowInfo,
)

logger = structlog.get_logger()


class MCPToolHandler:
    """Handler for MCP tool calls."""
    
    def __init__(self, name: str, handler: Callable, description: str, parameters: List[Dict[str, Any]] = None):
        self.name = name
        self.handler = handler
        self.description = description
        self.parameters = parameters or []


class MCPResourceHandler:
    """Handler for MCP resource access."""
    
    def __init__(self, uri_pattern: str, handler: Callable, description: str, mime_type: str = None):
        self.uri_pattern = uri_pattern
        self.handler = handler
        self.description = description
        self.mime_type = mime_type


class MCPPromptHandler:
    """Handler for MCP prompt access."""
    
    def __init__(self, name: str, handler: Callable, description: str, arguments: List[Dict[str, Any]] = None):
        self.name = name
        self.handler = handler
        self.description = description
        self.arguments = arguments or []


class MCPServer:
    """MCP Server for BudFlow integration."""
    
    def __init__(self, name: str = "BudFlow", version: str = "1.0.0"):
        self.name = name
        self.version = version
        self.logger = logger.bind(component="mcp_server")
        
        # Handler registries
        self.tool_handlers: Dict[str, MCPToolHandler] = {}
        self.resource_handlers: List[MCPResourceHandler] = []
        self.prompt_handlers: Dict[str, MCPPromptHandler] = {}
        
        # Connection state
        self.is_initialized = False
        self.client_capabilities = None
        
        # Active connections
        self.connections: Dict[str, WebSocket] = {}
        
        # Register built-in handlers
        self._register_builtin_handlers()
    
    def _register_builtin_handlers(self):
        """Register built-in MCP handlers."""
        # Workflow execution tool
        self.register_tool(
            "execute_workflow",
            self._execute_workflow_handler,
            "Execute a BudFlow workflow",
            [
                {"name": "workflow_id", "type": "string", "description": "Workflow ID to execute", "required": True},
                {"name": "input_data", "type": "object", "description": "Input data for workflow", "required": False},
                {"name": "execution_mode", "type": "string", "description": "Execution mode", "required": False}
            ]
        )
        
        # Workflow listing tool
        self.register_tool(
            "list_workflows",
            self._list_workflows_handler,
            "List available BudFlow workflows",
            []
        )
        
        # Workflow status tool
        self.register_tool(
            "get_workflow_status",
            self._get_workflow_status_handler,
            "Get status of a workflow execution",
            [
                {"name": "execution_id", "type": "string", "description": "Execution ID", "required": True}
            ]
        )
        
        # Node registry tool
        self.register_tool(
            "list_nodes",
            self._list_nodes_handler,
            "List available BudFlow nodes",
            [
                {"name": "category", "type": "string", "description": "Filter by category", "required": False},
                {"name": "type", "type": "string", "description": "Filter by node type", "required": False}
            ]
        )
        
        # Resource handlers for workflows
        self.register_resource(
            "budflow://workflows/*",
            self._get_workflow_resource,
            "BudFlow workflow definitions",
            "application/json"
        )
        
        # Prompt handlers
        self.register_prompt(
            "create_workflow",
            self._create_workflow_prompt,
            "Prompt for creating a new workflow",
            [
                {"name": "description", "type": "string", "description": "Workflow description", "required": True},
                {"name": "purpose", "type": "string", "description": "Workflow purpose", "required": False}
            ]
        )
    
    def register_tool(self, name: str, handler: Callable, description: str, parameters: List[Dict[str, Any]] = None):
        """Register a tool handler."""
        self.tool_handlers[name] = MCPToolHandler(name, handler, description, parameters)
        self.logger.info("Registered MCP tool", tool_name=name)
    
    def register_resource(self, uri_pattern: str, handler: Callable, description: str, mime_type: str = None):
        """Register a resource handler."""
        self.resource_handlers.append(MCPResourceHandler(uri_pattern, handler, description, mime_type))
        self.logger.info("Registered MCP resource", uri_pattern=uri_pattern)
    
    def register_prompt(self, name: str, handler: Callable, description: str, arguments: List[Dict[str, Any]] = None):
        """Register a prompt handler."""
        self.prompt_handlers[name] = MCPPromptHandler(name, handler, description, arguments)
        self.logger.info("Registered MCP prompt", prompt_name=name)
    
    async def handle_websocket(self, websocket: WebSocket):
        """Handle WebSocket connection for MCP."""
        await websocket.accept()
        connection_id = str(uuid4())
        self.connections[connection_id] = websocket
        
        self.logger.info("MCP client connected", connection_id=connection_id)
        
        try:
            while True:
                data = await websocket.receive_text()
                response = await self.handle_message(data)
                if response:
                    await websocket.send_text(response)
                    
        except WebSocketDisconnect:
            self.logger.info("MCP client disconnected", connection_id=connection_id)
        except Exception as e:
            self.logger.error("WebSocket error", error=str(e), connection_id=connection_id)
        finally:
            if connection_id in self.connections:
                del self.connections[connection_id]
    
    async def handle_message(self, data: str) -> Optional[str]:
        """Handle incoming MCP message."""
        try:
            message_data = json.loads(data)
            
            # Check if it's a request or notification
            if "id" in message_data:
                # It's a request - send response
                request = MCPRequest.model_validate(message_data)
                response = await self.handle_request(request)
                return response.model_dump_json()
            else:
                # It's a notification - no response needed
                notification = MCPNotification.model_validate(message_data)
                await self.handle_notification(notification)
                return None
                
        except json.JSONDecodeError as e:
            error_response = MCPResponse(
                id=0,
                error=MCPError(
                    code=MCPErrorCode.PARSE_ERROR,
                    message=f"JSON parse error: {str(e)}"
                )
            )
            return error_response.model_dump_json()
            
        except ValidationError as e:
            error_response = MCPResponse(
                id=message_data.get("id", 0),
                error=MCPError(
                    code=MCPErrorCode.INVALID_REQUEST,
                    message=f"Invalid request: {str(e)}"
                )
            )
            return error_response.model_dump_json()
            
        except Exception as e:
            self.logger.error("Error handling MCP message", error=str(e), traceback=traceback.format_exc())
            error_response = MCPResponse(
                id=message_data.get("id", 0) if isinstance(message_data, dict) else 0,
                error=MCPError(
                    code=MCPErrorCode.INTERNAL_ERROR,
                    message=f"Internal error: {str(e)}"
                )
            )
            return error_response.model_dump_json()
    
    async def handle_request(self, request: MCPRequest) -> MCPResponse:
        """Handle MCP request."""
        try:
            method = request.method
            params = request.params or {}
            
            if method == MCPMethodType.INITIALIZE:
                return await self._handle_initialize(request.id, params)
            elif method == MCPMethodType.LIST_TOOLS:
                return await self._handle_list_tools(request.id)
            elif method == MCPMethodType.CALL_TOOL:
                return await self._handle_call_tool(request.id, params)
            elif method == MCPMethodType.LIST_RESOURCES:
                return await self._handle_list_resources(request.id)
            elif method == MCPMethodType.READ_RESOURCE:
                return await self._handle_read_resource(request.id, params)
            elif method == MCPMethodType.LIST_PROMPTS:
                return await self._handle_list_prompts(request.id)
            elif method == MCPMethodType.GET_PROMPT:
                return await self._handle_get_prompt(request.id, params)
            elif method.startswith("budflow/"):
                return await self._handle_budflow_method(request.id, method, params)
            else:
                return MCPResponse(
                    id=request.id,
                    error=MCPError(
                        code=MCPErrorCode.METHOD_NOT_FOUND,
                        message=f"Method not found: {method}"
                    )
                )
                
        except Exception as e:
            self.logger.error("Error handling request", method=request.method, error=str(e))
            return MCPResponse(
                id=request.id,
                error=MCPError(
                    code=MCPErrorCode.INTERNAL_ERROR,
                    message=f"Internal error: {str(e)}"
                )
            )
    
    async def handle_notification(self, notification: MCPNotification):
        """Handle MCP notification."""
        try:
            method = notification.method
            params = notification.params or {}
            
            if method == MCPMethodType.LOG:
                await self._handle_log_notification(params)
            elif method == MCPMethodType.PROGRESS_NOTIFICATION:
                await self._handle_progress_notification(params)
            else:
                self.logger.warning("Unknown notification method", method=method)
                
        except Exception as e:
            self.logger.error("Error handling notification", method=notification.method, error=str(e))
    
    async def _handle_initialize(self, request_id: Union[str, int], params: Dict[str, Any]) -> MCPResponse:
        """Handle initialize request."""
        try:
            init_params = MCPInitializeParams.model_validate(params)
            self.client_capabilities = init_params.capabilities
            
            # Build server capabilities
            capabilities = MCPServerCapabilities(
                tools={
                    "list_changed": False  # We don't support dynamic tool changes yet
                },
                resources={
                    "subscribe": True,
                    "list_changed": False
                },
                prompts={
                    "list_changed": False
                },
                logging={}
            )
            
            result = MCPInitializeResult(
                protocol_version="2024-11-05",
                capabilities=capabilities,
                server_info={
                    "name": self.name,
                    "version": self.version,
                    "description": "BudFlow workflow automation platform with AI integration"
                }
            )
            
            self.is_initialized = True
            self.logger.info("MCP server initialized", client_info=init_params.client_info)
            
            return MCPResponse(id=request_id, result=result.model_dump())
            
        except ValidationError as e:
            return MCPResponse(
                id=request_id,
                error=MCPError(
                    code=MCPErrorCode.INVALID_PARAMS,
                    message=f"Invalid initialize parameters: {str(e)}"
                )
            )
    
    async def _handle_list_tools(self, request_id: Union[str, int]) -> MCPResponse:
        """Handle list tools request."""
        tools = []
        for handler in self.tool_handlers.values():
            tool = MCPTool(
                name=handler.name,
                description=handler.description,
                parameters=handler.parameters
            )
            tools.append(tool.model_dump())
        
        return MCPResponse(id=request_id, result={"tools": tools})
    
    async def _handle_call_tool(self, request_id: Union[str, int], params: Dict[str, Any]) -> MCPResponse:
        """Handle call tool request."""
        try:
            tool_call = MCPToolCall.model_validate(params)
            
            if tool_call.name not in self.tool_handlers:
                return MCPResponse(
                    id=request_id,
                    error=MCPError(
                        code=MCPErrorCode.TOOL_NOT_FOUND,
                        message=f"Tool not found: {tool_call.name}"
                    )
                )
            
            handler = self.tool_handlers[tool_call.name]
            
            try:
                result = await handler.handler(**tool_call.arguments)
                tool_result = MCPToolResult(content=[{"type": "text", "text": json.dumps(result)}])
                return MCPResponse(id=request_id, result=tool_result.model_dump())
                
            except Exception as e:
                self.logger.error("Tool execution error", tool=tool_call.name, error=str(e))
                tool_result = MCPToolResult(
                    content=[{"type": "text", "text": f"Error: {str(e)}"}],
                    is_error=True
                )
                return MCPResponse(id=request_id, result=tool_result.model_dump())
                
        except ValidationError as e:
            return MCPResponse(
                id=request_id,
                error=MCPError(
                    code=MCPErrorCode.INVALID_PARAMS,
                    message=f"Invalid tool call parameters: {str(e)}"
                )
            )
    
    async def _handle_list_resources(self, request_id: Union[str, int]) -> MCPResponse:
        """Handle list resources request."""
        resources = []
        for handler in self.resource_handlers:
            resource = MCPResource(
                uri=handler.uri_pattern,
                name=handler.description,
                description=handler.description,
                mime_type=handler.mime_type
            )
            resources.append(resource.model_dump())
        
        return MCPResponse(id=request_id, result={"resources": resources})
    
    async def _handle_read_resource(self, request_id: Union[str, int], params: Dict[str, Any]) -> MCPResponse:
        """Handle read resource request."""
        uri = params.get("uri")
        if not uri:
            return MCPResponse(
                id=request_id,
                error=MCPError(
                    code=MCPErrorCode.INVALID_PARAMS,
                    message="Missing uri parameter"
                )
            )
        
        # Find matching resource handler
        for handler in self.resource_handlers:
            # Simple pattern matching (could be improved with regex)
            if handler.uri_pattern.replace("*", "") in uri:
                try:
                    content = await handler.handler(uri)
                    resource_content = MCPResourceContent(
                        uri=uri,
                        mime_type=handler.mime_type,
                        text=json.dumps(content) if isinstance(content, dict) else str(content)
                    )
                    return MCPResponse(id=request_id, result=resource_content.model_dump())
                    
                except Exception as e:
                    return MCPResponse(
                        id=request_id,
                        error=MCPError(
                            code=MCPErrorCode.INTERNAL_ERROR,
                            message=f"Error reading resource: {str(e)}"
                        )
                    )
        
        return MCPResponse(
            id=request_id,
            error=MCPError(
                code=MCPErrorCode.RESOURCE_NOT_FOUND,
                message=f"Resource not found: {uri}"
            )
        )
    
    async def _handle_list_prompts(self, request_id: Union[str, int]) -> MCPResponse:
        """Handle list prompts request."""
        prompts = []
        for handler in self.prompt_handlers.values():
            prompt = MCPPrompt(
                name=handler.name,
                description=handler.description,
                arguments=handler.arguments
            )
            prompts.append(prompt.model_dump())
        
        return MCPResponse(id=request_id, result={"prompts": prompts})
    
    async def _handle_get_prompt(self, request_id: Union[str, int], params: Dict[str, Any]) -> MCPResponse:
        """Handle get prompt request."""
        prompt_name = params.get("name")
        if not prompt_name:
            return MCPResponse(
                id=request_id,
                error=MCPError(
                    code=MCPErrorCode.INVALID_PARAMS,
                    message="Missing name parameter"
                )
            )
        
        if prompt_name not in self.prompt_handlers:
            return MCPResponse(
                id=request_id,
                error=MCPError(
                    code=MCPErrorCode.PROMPT_NOT_FOUND,
                    message=f"Prompt not found: {prompt_name}"
                )
            )
        
        handler = self.prompt_handlers[prompt_name]
        
        try:
            prompt_data = await handler.handler(params.get("arguments", {}))
            return MCPResponse(id=request_id, result=prompt_data)
            
        except Exception as e:
            return MCPResponse(
                id=request_id,
                error=MCPError(
                    code=MCPErrorCode.INTERNAL_ERROR,
                    message=f"Error getting prompt: {str(e)}"
                )
            )
    
    async def _handle_budflow_method(self, request_id: Union[str, int], method: str, params: Dict[str, Any]) -> MCPResponse:
        """Handle BudFlow-specific methods."""
        if method == MCPMethodType.EXECUTE_WORKFLOW:
            return await self._handle_execute_workflow(request_id, params)
        elif method == MCPMethodType.GET_WORKFLOW_STATUS:
            return await self._handle_get_workflow_status(request_id, params)
        elif method == MCPMethodType.LIST_WORKFLOWS:
            return await self._handle_list_workflows_method(request_id, params)
        else:
            return MCPResponse(
                id=request_id,
                error=MCPError(
                    code=MCPErrorCode.METHOD_NOT_FOUND,
                    message=f"BudFlow method not found: {method}"
                )
            )
    
    async def _handle_log_notification(self, params: Dict[str, Any]):
        """Handle log notification."""
        try:
            log_message = MCPLogMessage.model_validate(params)
            self.logger.info("MCP log message", level=log_message.level, data=log_message.data)
        except ValidationError as e:
            self.logger.warning("Invalid log message", error=str(e))
    
    async def _handle_progress_notification(self, params: Dict[str, Any]):
        """Handle progress notification."""
        try:
            progress = MCPProgressNotification.model_validate(params)
            self.logger.info("MCP progress", token=progress.progress_token, progress=progress.progress)
        except ValidationError as e:
            self.logger.warning("Invalid progress notification", error=str(e))
    
    # Built-in tool handlers
    async def _execute_workflow_handler(self, workflow_id: str, input_data: Dict[str, Any] = None, execution_mode: str = "manual") -> Dict[str, Any]:
        """Execute a workflow."""
        # This would integrate with the actual workflow service
        from budflow.workflows.service import WorkflowService
        from budflow.database_deps import get_db
        
        # For now, return a mock response
        # In a real implementation, this would use dependency injection
        return {
            "execution_id": str(uuid4()),
            "status": "started",
            "message": f"Workflow {workflow_id} execution started"
        }
    
    async def _list_workflows_handler(self) -> Dict[str, Any]:
        """List available workflows."""
        # This would integrate with the actual workflow service
        return {
            "workflows": [
                {
                    "id": "example-workflow-1",
                    "name": "Data Processing Pipeline",
                    "description": "Process and transform incoming data",
                    "status": "active"
                },
                {
                    "id": "example-workflow-2", 
                    "name": "Email Notification Flow",
                    "description": "Send automated email notifications",
                    "status": "active"
                }
            ]
        }
    
    async def _get_workflow_status_handler(self, execution_id: str) -> Dict[str, Any]:
        """Get workflow execution status."""
        # This would integrate with the actual execution service
        return {
            "execution_id": execution_id,
            "status": "completed",
            "result": {"processed_items": 42},
            "started_at": "2024-01-01T00:00:00Z",
            "completed_at": "2024-01-01T00:05:00Z"
        }
    
    async def _list_nodes_handler(self, category: str = None, type: str = None) -> Dict[str, Any]:
        """List available nodes."""
        from budflow.nodes.registry import default_registry
        
        all_nodes = default_registry.get_all_nodes()
        
        # Filter by category or type if specified
        filtered_nodes = {}
        for key, node_class in all_nodes.items():
            definition = default_registry.get_node_definition(key)
            if definition:
                if category and definition.category != category:
                    continue
                if type and definition.type != type:
                    continue
                
                filtered_nodes[key] = {
                    "name": definition.name,
                    "description": definition.description,
                    "category": definition.category,
                    "type": definition.type
                }
        
        return {"nodes": filtered_nodes}
    
    # Built-in resource handlers
    async def _get_workflow_resource(self, uri: str) -> Dict[str, Any]:
        """Get workflow resource."""
        # Extract workflow ID from URI
        workflow_id = uri.split("/")[-1]
        
        # This would integrate with the actual workflow service
        return {
            "id": workflow_id,
            "name": f"Workflow {workflow_id}",
            "description": "Sample workflow definition",
            "nodes": [],
            "connections": []
        }
    
    # Built-in prompt handlers
    async def _create_workflow_prompt(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Create workflow prompt."""
        description = arguments.get("description", "")
        purpose = arguments.get("purpose", "")
        
        prompt_text = f"""Create a BudFlow workflow with the following requirements:

Description: {description}
Purpose: {purpose}

Please design a workflow that includes:
1. Appropriate trigger nodes (manual, webhook, or schedule)
2. Action nodes to accomplish the described task
3. Control flow nodes (if/else, loops) if needed
4. Error handling considerations

Provide the workflow configuration in JSON format with nodes and connections."""
        
        return MCPGetPromptResult(
            description="Prompt for creating a new BudFlow workflow",
            messages=[
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": prompt_text
                    }
                }
            ]
        ).model_dump()
    
    # Direct method handlers (for REST API integration)
    async def _handle_execute_workflow(self, request_id: Union[str, int], params: Dict[str, Any]) -> MCPResponse:
        """Handle execute workflow method."""
        try:
            workflow_params = BudFlowWorkflowParams.model_validate(params)
            result = await self._execute_workflow_handler(
                workflow_params.workflow_id,
                workflow_params.input_data,
                workflow_params.execution_mode
            )
            return MCPResponse(id=request_id, result=result)
        except ValidationError as e:
            return MCPResponse(
                id=request_id,
                error=MCPError(
                    code=MCPErrorCode.INVALID_PARAMS,
                    message=f"Invalid workflow parameters: {str(e)}"
                )
            )
    
    async def _handle_get_workflow_status(self, request_id: Union[str, int], params: Dict[str, Any]) -> MCPResponse:
        """Handle get workflow status method."""
        execution_id = params.get("execution_id")
        if not execution_id:
            return MCPResponse(
                id=request_id,
                error=MCPError(
                    code=MCPErrorCode.INVALID_PARAMS,
                    message="Missing execution_id parameter"
                )
            )
        
        result = await self._get_workflow_status_handler(execution_id)
        return MCPResponse(id=request_id, result=result)
    
    async def _handle_list_workflows_method(self, request_id: Union[str, int], params: Dict[str, Any]) -> MCPResponse:
        """Handle list workflows method."""
        result = await self._list_workflows_handler()
        return MCPResponse(id=request_id, result=result)
    
    async def send_notification(self, method: str, params: Dict[str, Any]):
        """Send notification to all connected clients."""
        notification = MCPNotification(method=method, params=params)
        notification_json = notification.model_dump_json()
        
        for connection in self.connections.values():
            try:
                await connection.send_text(notification_json)
            except Exception as e:
                self.logger.warning("Failed to send notification", error=str(e))
    
    async def send_log(self, level: str, message: str, data: Any = None):
        """Send log message to clients."""
        await self.send_notification(
            MCPMethodType.LOG,
            MCPLogMessage(level=level, data=data or message).model_dump()
        )
    
    async def send_progress(self, token: Union[str, int], progress: float, total: float = None):
        """Send progress notification to clients."""
        await self.send_notification(
            MCPMethodType.PROGRESS_NOTIFICATION,
            MCPProgressNotification(
                progress_token=token,
                progress=progress,
                total=total
            ).model_dump()
        )


# Global MCP server instance
mcp_server = MCPServer()