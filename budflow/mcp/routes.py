"""MCP routes for FastAPI integration."""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends
from fastapi.responses import JSONResponse
from typing import Dict, Any

from .server import mcp_server
from .client import MCPClient, MCPConnectionError, MCPRequestError
from budflow.auth.dependencies import get_current_user_optional

router = APIRouter(prefix="/mcp", tags=["mcp"])


@router.websocket("/ws")
async def mcp_websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for MCP connections."""
    await mcp_server.handle_websocket(websocket)


@router.get("/capabilities")
async def get_mcp_capabilities():
    """Get MCP server capabilities."""
    return JSONResponse({
        "server_info": {
            "name": mcp_server.name,
            "version": mcp_server.version,
            "description": "BudFlow workflow automation platform with AI integration"
        },
        "tools": [
            {
                "name": handler.name,
                "description": handler.description,
                "parameters": handler.parameters
            }
            for handler in mcp_server.tool_handlers.values()
        ],
        "resources": [
            {
                "uri_pattern": handler.uri_pattern,
                "description": handler.description,
                "mime_type": handler.mime_type
            }
            for handler in mcp_server.resource_handlers
        ],
        "prompts": [
            {
                "name": handler.name,
                "description": handler.description,
                "arguments": handler.arguments
            }
            for handler in mcp_server.prompt_handlers.values()
        ]
    })


@router.get("/tools")
async def list_mcp_tools():
    """List available MCP tools."""
    tools = []
    for handler in mcp_server.tool_handlers.values():
        tools.append({
            "name": handler.name,
            "description": handler.description,
            "parameters": handler.parameters
        })
    
    return {"tools": tools}


@router.post("/tools/{tool_name}/call")
async def call_mcp_tool(
    tool_name: str,
    arguments: Dict[str, Any],
    user=Depends(get_current_user_optional)
):
    """Call an MCP tool via REST API."""
    if tool_name not in mcp_server.tool_handlers:
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
    
    handler = mcp_server.tool_handlers[tool_name]
    
    try:
        result = await handler.handler(**arguments)
        return {"result": result, "success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tool execution failed: {str(e)}")


@router.get("/resources")
async def list_mcp_resources():
    """List available MCP resources."""
    resources = []
    for handler in mcp_server.resource_handlers:
        resources.append({
            "uri_pattern": handler.uri_pattern,
            "description": handler.description,
            "mime_type": handler.mime_type
        })
    
    return {"resources": resources}


@router.get("/resources/read")
async def read_mcp_resource(uri: str):
    """Read an MCP resource via REST API."""
    # Find matching resource handler
    for handler in mcp_server.resource_handlers:
        # Simple pattern matching
        if handler.uri_pattern.replace("*", "") in uri:
            try:
                content = await handler.handler(uri)
                return {
                    "uri": uri,
                    "content": content,
                    "mime_type": handler.mime_type
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error reading resource: {str(e)}")
    
    raise HTTPException(status_code=404, detail=f"Resource not found: {uri}")


@router.get("/prompts")
async def list_mcp_prompts():
    """List available MCP prompts."""
    prompts = []
    for handler in mcp_server.prompt_handlers.values():
        prompts.append({
            "name": handler.name,
            "description": handler.description,
            "arguments": handler.arguments
        })
    
    return {"prompts": prompts}


@router.post("/prompts/{prompt_name}")
async def get_mcp_prompt(
    prompt_name: str,
    arguments: Dict[str, Any] = None
):
    """Get an MCP prompt via REST API."""
    if prompt_name not in mcp_server.prompt_handlers:
        raise HTTPException(status_code=404, detail=f"Prompt not found: {prompt_name}")
    
    handler = mcp_server.prompt_handlers[prompt_name]
    
    try:
        prompt_data = await handler.handler(arguments or {})
        return prompt_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting prompt: {str(e)}")


@router.post("/client/connect")
async def create_mcp_client_connection(
    server_uri: str,
    client_name: str = "BudFlow REST Client"
):
    """Create MCP client connection via REST API."""
    try:
        client = MCPClient(client_name=client_name)
        await client.connect_websocket(server_uri)
        await client.initialize()
        
        return {
            "success": True,
            "server_info": client.server_info,
            "capabilities": client.server_capabilities.model_dump() if client.server_capabilities else None,
            "available_tools": len(client.available_tools),
            "available_resources": len(client.available_resources),
            "available_prompts": len(client.available_prompts)
        }
    except MCPConnectionError as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating client: {str(e)}")


# Health check endpoint
@router.get("/health")
async def mcp_health_check():
    """Check MCP server health."""
    return {
        "status": "healthy",
        "server_name": mcp_server.name,
        "server_version": mcp_server.version,
        "is_initialized": mcp_server.is_initialized,
        "active_connections": len(mcp_server.connections),
        "registered_tools": len(mcp_server.tool_handlers),
        "registered_resources": len(mcp_server.resource_handlers),
        "registered_prompts": len(mcp_server.prompt_handlers)
    }