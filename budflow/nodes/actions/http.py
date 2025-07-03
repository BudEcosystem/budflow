"""HTTP action node for making HTTP requests."""

import asyncio
import json
import base64
from typing import Any, Dict, Optional, Union
from urllib.parse import urlencode

import aiohttp
import structlog

from budflow.nodes.base import ActionNode, NodeExecutionContext


logger = structlog.get_logger()


class HTTPNode(ActionNode):
    """Node for making HTTP requests."""
    
    # Node metadata
    name = "HTTP Request"
    display_name = "HTTP Request"
    description = "Make HTTP requests to web APIs and services"
    category = "Communication"
    version = "1.0.0"
    tags = ["http", "api", "web", "request"]
    
    def __init__(self, node_data):
        """Initialize HTTP node."""
        super().__init__(node_data)
        self.timeout = aiohttp.ClientTimeout(
            total=self.parameters.get("timeout", 30)
        )
    
    @classmethod
    def get_definition(cls):
        """Get node definition."""
        from budflow.nodes.base import NodeDefinition, NodeParameter, ParameterType, NodeType, NodeCategory
        
        return NodeDefinition(
            type=NodeType.ACTION,
            name="HTTP Request",
            description="Make HTTP requests to web APIs and services",
            category=NodeCategory.NETWORK,
            parameters=[
                NodeParameter(
                    name="method",
                    display_name="HTTP Method",
                    type=ParameterType.STRING,
                    required=True,
                    default="GET",
                    options=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
                ),
                NodeParameter(
                    name="url",
                    display_name="URL",
                    type=ParameterType.STRING,
                    required=True,
                    description="The URL to make the request to"
                ),
                NodeParameter(
                    name="headers",
                    display_name="Headers",
                    type=ParameterType.JSON,
                    required=False,
                    description="HTTP headers to include"
                ),
                NodeParameter(
                    name="authentication",
                    display_name="Authentication",
                    type=ParameterType.STRING,
                    required=False,
                    default="none",
                    options=["none", "bearer", "basic", "api_key"]
                ),
                NodeParameter(
                    name="timeout",
                    display_name="Timeout (seconds)",
                    type=ParameterType.NUMBER,
                    required=False,
                    default=30
                )
            ]
        )
    
    async def execute(self, context: NodeExecutionContext) -> Dict[str, Any]:
        """Execute HTTP request."""
        try:
            method = self.parameters.get("method", "GET").upper()
            url = self.parameters.get("url")
            
            if not url:
                return {
                    "success": False,
                    "error": "URL is required"
                }
            
            # Prepare headers
            headers = self._prepare_headers()
            
            # Prepare authentication
            headers.update(self._prepare_authentication())
            
            # Prepare request body
            request_data = self._prepare_body()
            
            # Prepare query parameters
            params = self.parameters.get("queryParameters", {})
            
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    **request_data
                ) as response:
                    
                    # Get response data
                    response_headers = dict(response.headers)
                    response_text = await response.text()
                    
                    # Try to parse as JSON
                    try:
                        response_data = await response.json()
                    except (json.JSONDecodeError, aiohttp.ContentTypeError):
                        response_data = response_text
                    
                    # Handle success/error based on status code
                    success = 200 <= response.status < 400
                    
                    result = {
                        "success": success,
                        "statusCode": response.status,
                        "statusText": response.reason,
                        "data": response_data,
                        "responseHeaders": response_headers,
                        "responseTime": None  # Would need to measure this
                    }
                    
                    if not success:
                        result["error"] = f"HTTP {response.status}: {response.reason} - {response_text}"
                    
                    return result
                    
        except asyncio.TimeoutError:
            return {
                "success": False,
                "error": f"Request timeout after {self.timeout.total} seconds"
            }
        except Exception as e:
            logger.error(
                "HTTP request failed",
                error=str(e),
                url=self.parameters.get("url"),
                method=self.parameters.get("method")
            )
            return {
                "success": False,
                "error": f"HTTP request failed: {str(e)}"
            }
    
    def _prepare_headers(self) -> Dict[str, str]:
        """Prepare request headers."""
        headers = {}
        
        # Add custom headers
        custom_headers = self.parameters.get("headers", {})
        if isinstance(custom_headers, dict):
            headers.update(custom_headers)
        
        # Set content type based on body type
        body_type = self.parameters.get("bodyType", "none")
        if body_type == "json":
            headers["Content-Type"] = "application/json"
        elif body_type == "form":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif body_type == "xml":
            headers["Content-Type"] = "application/xml"
        
        return headers
    
    def _prepare_authentication(self) -> Dict[str, str]:
        """Prepare authentication headers."""
        headers = {}
        auth_type = self.parameters.get("authentication", "none")
        
        if auth_type == "bearer":
            token = self.parameters.get("bearerToken")
            if token:
                headers["Authorization"] = f"Bearer {token}"
        
        elif auth_type == "basic":
            username = self.parameters.get("basicUsername")
            password = self.parameters.get("basicPassword")
            if username and password:
                credentials = base64.b64encode(
                    f"{username}:{password}".encode()
                ).decode()
                headers["Authorization"] = f"Basic {credentials}"
        
        elif auth_type == "api_key":
            api_key = self.parameters.get("apiKey")
            key_header = self.parameters.get("apiKeyHeader", "X-API-Key")
            if api_key:
                headers[key_header] = api_key
        
        return headers
    
    def _prepare_body(self) -> Dict[str, Any]:
        """Prepare request body."""
        body_type = self.parameters.get("bodyType", "none")
        body_data = self.parameters.get("body")
        
        if body_type == "none" or not body_data:
            return {}
        
        elif body_type == "json":
            if isinstance(body_data, (dict, list)):
                return {"json": body_data}
            else:
                return {"data": json.dumps(body_data)}
        
        elif body_type == "form":
            if isinstance(body_data, dict):
                return {"data": urlencode(body_data)}
            else:
                return {"data": str(body_data)}
        
        elif body_type == "raw":
            return {"data": str(body_data)}
        
        elif body_type == "binary":
            if isinstance(body_data, str):
                # Assume base64 encoded
                try:
                    binary_data = base64.b64decode(body_data)
                    return {"data": binary_data}
                except Exception:
                    return {"data": body_data.encode()}
            else:
                return {"data": body_data}
        
        return {}
    
    def validate_parameters(self) -> Dict[str, Any]:
        """Validate node parameters."""
        errors = []
        
        # Check required parameters
        if not self.parameters.get("url"):
            errors.append("URL is required")
        
        method = self.parameters.get("method", "GET").upper()
        valid_methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
        if method not in valid_methods:
            errors.append(f"Invalid HTTP method: {method}")
        
        # Validate authentication
        auth_type = self.parameters.get("authentication", "none")
        if auth_type == "bearer" and not self.parameters.get("bearerToken"):
            errors.append("Bearer token is required for bearer authentication")
        
        if auth_type == "basic":
            if not self.parameters.get("basicUsername"):
                errors.append("Username is required for basic authentication")
            if not self.parameters.get("basicPassword"):
                errors.append("Password is required for basic authentication")
        
        if auth_type == "api_key" and not self.parameters.get("apiKey"):
            errors.append("API key is required for API key authentication")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }