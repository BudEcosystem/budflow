"""Model Context Protocol (MCP) integration for BudFlow.

This module provides MCP server and client implementations for integrating
with Claude and other AI systems.
"""

from .server import MCPServer, MCPToolHandler, MCPResourceHandler, MCPPromptHandler, mcp_server
from .client import MCPClient, MCPConnectionError, MCPRequestError, create_mcp_client, execute_workflow_via_mcp
from .types import (
    MCPMessage,
    MCPRequest,
    MCPResponse,
    MCPNotification,
    MCPError,
    MCPToolCall,
    MCPTool,
    MCPResource,
    MCPPrompt,
    MCPMethodType,
    MCPErrorCode,
    BudFlowWorkflowParams,
    BudFlowWorkflowResult,
    BudFlowWorkflowInfo,
)

__all__ = [
    # Server components
    "MCPServer",
    "MCPToolHandler",
    "MCPResourceHandler", 
    "MCPPromptHandler",
    "mcp_server",
    
    # Client components
    "MCPClient",
    "MCPConnectionError",
    "MCPRequestError",
    "create_mcp_client",
    "execute_workflow_via_mcp",
    
    # Types
    "MCPMessage",
    "MCPRequest",
    "MCPResponse",
    "MCPNotification",
    "MCPError",
    "MCPToolCall",
    "MCPTool",
    "MCPResource",
    "MCPPrompt",
    "MCPMethodType",
    "MCPErrorCode",
    "BudFlowWorkflowParams",
    "BudFlowWorkflowResult",
    "BudFlowWorkflowInfo",
]