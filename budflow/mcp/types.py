"""Model Context Protocol (MCP) types and models."""

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field


class MCPMethodType(str, Enum):
    """MCP method types."""
    # Capability-related methods
    INITIALIZE = "initialize"
    GET_CAPABILITIES = "get_capabilities"
    
    # Tool-related methods
    LIST_TOOLS = "tools/list"
    CALL_TOOL = "tools/call"
    
    # Resource-related methods
    LIST_RESOURCES = "resources/list"
    READ_RESOURCE = "resources/read"
    SUBSCRIBE_RESOURCE = "resources/subscribe"
    UNSUBSCRIBE_RESOURCE = "resources/unsubscribe"
    
    # Prompt-related methods
    LIST_PROMPTS = "prompts/list"
    GET_PROMPT = "prompts/get"
    
    # Logging
    LOG = "logging/message"
    
    # Progress
    PROGRESS_NOTIFICATION = "notifications/progress"
    
    # Custom BudFlow methods
    EXECUTE_WORKFLOW = "budflow/execute_workflow"
    GET_WORKFLOW_STATUS = "budflow/get_workflow_status"
    LIST_WORKFLOWS = "budflow/list_workflows"


class MCPErrorCode(int, Enum):
    """MCP error codes."""
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    
    # MCP-specific errors
    RESOURCE_NOT_FOUND = -32001
    TOOL_NOT_FOUND = -32002
    PROMPT_NOT_FOUND = -32003
    CAPABILITY_NOT_SUPPORTED = -32004
    ACCESS_DENIED = -32005


class MCPMessage(BaseModel):
    """Base MCP message."""
    jsonrpc: str = Field(default="2.0", description="JSON-RPC version")


class MCPRequest(MCPMessage):
    """MCP request message."""
    id: Union[str, int] = Field(..., description="Request ID")
    method: str = Field(..., description="Method name")
    params: Optional[Dict[str, Any]] = Field(default=None, description="Method parameters")


class MCPNotification(MCPMessage):
    """MCP notification message (no response expected)."""
    method: str = Field(..., description="Method name") 
    params: Optional[Dict[str, Any]] = Field(default=None, description="Method parameters")


class MCPError(BaseModel):
    """MCP error object."""
    code: int = Field(..., description="Error code")
    message: str = Field(..., description="Error message")
    data: Optional[Any] = Field(default=None, description="Additional error data")


class MCPResponse(MCPMessage):
    """MCP response message."""
    id: Union[str, int] = Field(..., description="Request ID")
    result: Optional[Any] = Field(default=None, description="Result data")
    error: Optional[MCPError] = Field(default=None, description="Error object")


class MCPCapability(BaseModel):
    """MCP capability definition."""
    name: str = Field(..., description="Capability name")
    version: str = Field(default="1.0.0", description="Capability version")
    description: Optional[str] = Field(default=None, description="Capability description")


class MCPServerCapabilities(BaseModel):
    """MCP server capabilities."""
    tools: Optional[Dict[str, Any]] = Field(default=None, description="Tool capabilities")
    resources: Optional[Dict[str, Any]] = Field(default=None, description="Resource capabilities")
    prompts: Optional[Dict[str, Any]] = Field(default=None, description="Prompt capabilities")
    logging: Optional[Dict[str, Any]] = Field(default=None, description="Logging capabilities")
    experimental: Optional[Dict[str, Any]] = Field(default=None, description="Experimental capabilities")


class MCPClientCapabilities(BaseModel):
    """MCP client capabilities."""
    roots: Optional[Dict[str, Any]] = Field(default=None, description="Root capabilities")
    sampling: Optional[Dict[str, Any]] = Field(default=None, description="Sampling capabilities")
    experimental: Optional[Dict[str, Any]] = Field(default=None, description="Experimental capabilities")


class MCPInitializeParams(BaseModel):
    """Parameters for initialize request."""
    protocol_version: str = Field(..., description="Protocol version")
    capabilities: MCPClientCapabilities = Field(..., description="Client capabilities")
    client_info: Dict[str, str] = Field(..., description="Client information")


class MCPInitializeResult(BaseModel):
    """Result of initialize request."""
    protocol_version: str = Field(..., description="Protocol version")
    capabilities: MCPServerCapabilities = Field(..., description="Server capabilities")
    server_info: Dict[str, str] = Field(..., description="Server information")


class MCPToolParameter(BaseModel):
    """Tool parameter definition."""
    name: str = Field(..., description="Parameter name")
    type: str = Field(..., description="Parameter type")
    description: Optional[str] = Field(default=None, description="Parameter description")
    required: bool = Field(default=False, description="Whether parameter is required")
    enum: Optional[List[Any]] = Field(default=None, description="Allowed values")


class MCPTool(BaseModel):
    """MCP tool definition."""
    name: str = Field(..., description="Tool name")
    description: str = Field(..., description="Tool description")
    parameters: List[MCPToolParameter] = Field(default_factory=list, description="Tool parameters")
    input_schema: Optional[Dict[str, Any]] = Field(default=None, description="JSON schema for input")


class MCPToolCall(BaseModel):
    """Tool call request."""
    name: str = Field(..., description="Tool name")
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Tool arguments")


class MCPToolResult(BaseModel):
    """Tool call result."""
    content: List[Dict[str, Any]] = Field(default_factory=list, description="Result content")
    is_error: bool = Field(default=False, description="Whether result is an error")


class MCPResource(BaseModel):
    """MCP resource definition."""
    uri: str = Field(..., description="Resource URI")
    name: str = Field(..., description="Resource name")
    description: Optional[str] = Field(default=None, description="Resource description")
    mime_type: Optional[str] = Field(default=None, description="MIME type")


class MCPResourceContent(BaseModel):
    """Resource content."""
    uri: str = Field(..., description="Resource URI")
    mime_type: Optional[str] = Field(default=None, description="MIME type")
    text: Optional[str] = Field(default=None, description="Text content")
    blob: Optional[bytes] = Field(default=None, description="Binary content")


class MCPPrompt(BaseModel):
    """MCP prompt definition."""
    name: str = Field(..., description="Prompt name")
    description: Optional[str] = Field(default=None, description="Prompt description")
    arguments: List[MCPToolParameter] = Field(default_factory=list, description="Prompt arguments")


class MCPPromptMessage(BaseModel):
    """Prompt message."""
    role: str = Field(..., description="Message role (user, assistant, system)")
    content: Dict[str, Any] = Field(..., description="Message content")


class MCPGetPromptResult(BaseModel):
    """Result of get prompt request."""
    description: Optional[str] = Field(default=None, description="Prompt description")
    messages: List[MCPPromptMessage] = Field(..., description="Prompt messages")


class MCPLogLevel(str, Enum):
    """Log levels."""
    DEBUG = "debug"
    INFO = "info"
    NOTICE = "notice"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    ALERT = "alert"
    EMERGENCY = "emergency"


class MCPLogMessage(BaseModel):
    """Log message."""
    level: MCPLogLevel = Field(..., description="Log level")
    data: Any = Field(..., description="Log data")
    logger: Optional[str] = Field(default=None, description="Logger name")


class MCPProgressNotification(BaseModel):
    """Progress notification."""
    progress_token: Union[str, int] = Field(..., description="Progress token")
    progress: float = Field(..., description="Progress value (0.0 to 1.0)")
    total: Optional[float] = Field(default=None, description="Total progress")


# BudFlow-specific types
class BudFlowWorkflowParams(BaseModel):
    """Parameters for BudFlow workflow execution."""
    workflow_id: str = Field(..., description="Workflow ID")
    input_data: Optional[Dict[str, Any]] = Field(default=None, description="Input data")
    execution_mode: str = Field(default="manual", description="Execution mode")


class BudFlowWorkflowResult(BaseModel):
    """Result of BudFlow workflow execution."""
    execution_id: str = Field(..., description="Execution ID")
    status: str = Field(..., description="Execution status")
    result: Optional[Dict[str, Any]] = Field(default=None, description="Execution result")
    error: Optional[str] = Field(default=None, description="Error message if failed")


class BudFlowWorkflowInfo(BaseModel):
    """BudFlow workflow information."""
    id: str = Field(..., description="Workflow ID")
    name: str = Field(..., description="Workflow name")
    description: Optional[str] = Field(default=None, description="Workflow description")
    status: str = Field(..., description="Workflow status")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Update timestamp")