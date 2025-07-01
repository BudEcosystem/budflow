"""Action node implementations."""

import asyncio
import json
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any, Dict, List, Optional

import httpx
import sqlalchemy
from sqlalchemy import create_engine, text

from budflow.workflows.models import NodeType
from .base import ActionNode, NodeDefinition, NodeCategory, NodeParameter, ParameterType


class HttpRequestNode(ActionNode):
    """HTTP request action node."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute HTTP request."""
        # Get parameters
        url = self.evaluate_expression(self.get_parameter("url", ""))
        method = self.get_parameter("method", "GET")
        headers = self.get_parameter("headers", {})
        body = self.get_parameter("body")
        query_params = self.get_parameter("query_params", {})
        timeout = self.get_parameter("timeout", 30)
        
        # Evaluate expressions in headers and query params
        evaluated_headers = {}
        for key, value in headers.items():
            if isinstance(value, str):
                evaluated_headers[key] = self.evaluate_expression(value)
            else:
                evaluated_headers[key] = value
        
        evaluated_params = {}
        for key, value in query_params.items():
            if isinstance(value, str):
                evaluated_params[key] = self.evaluate_expression(value)
            else:
                evaluated_params[key] = value
        
        # Prepare request body
        request_body = None
        if body:
            if isinstance(body, str):
                request_body = self.evaluate_expression(body)
            else:
                request_body = body
        
        # Make HTTP request
        results = []
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            for item in self.context.input_data or [{}]:
                try:
                    # Build URL with item data
                    item_url = self.evaluate_expression(url, item)
                    
                    # Make request
                    response = await client.request(
                        method=method,
                        url=item_url,
                        headers=evaluated_headers,
                        params=evaluated_params,
                        json=request_body if request_body else None
                    )
                    
                    # Parse response
                    response_data = {
                        "status_code": response.status_code,
                        "headers": dict(response.headers),
                        "url": str(response.url),
                    }
                    
                    # Try to parse JSON response
                    try:
                        response_data["body"] = response.json()
                    except Exception:
                        response_data["body"] = response.text
                    
                    results.append(response_data)
                    
                except Exception as e:
                    self.logger.error("HTTP request failed", error=str(e), url=url)
                    results.append({
                        "error": str(e),
                        "status_code": 0,
                        "url": url
                    })
        
        return results
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="HTTP Request",
            type=NodeType.ACTION,
            category=NodeCategory.NETWORK,
            description="Make HTTP requests to APIs",
            icon="globe",
            color="#339AF0",
            parameters=[
                NodeParameter(
                    name="url",
                    type=ParameterType.STRING,
                    required=True,
                    description="Request URL",
                    placeholder="https://api.example.com/endpoint"
                ),
                NodeParameter(
                    name="method",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="GET",
                    options=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
                    description="HTTP method"
                ),
                NodeParameter(
                    name="headers",
                    type=ParameterType.JSON,
                    default={},
                    description="Request headers"
                ),
                NodeParameter(
                    name="query_params",
                    type=ParameterType.JSON,
                    default={},
                    description="Query parameters"
                ),
                NodeParameter(
                    name="body",
                    type=ParameterType.JSON,
                    description="Request body (for POST/PUT/PATCH)"
                ),
                NodeParameter(
                    name="timeout",
                    type=ParameterType.NUMBER,
                    default=30,
                    min_value=1,
                    max_value=300,
                    description="Request timeout in seconds"
                ),
            ],
            inputs=["main"],
            outputs=["main", "error"]
        )


class EmailNode(ActionNode):
    """Email sending action node."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Send email."""
        # Get parameters
        to = self.evaluate_expression(self.get_parameter("to", ""))
        subject = self.evaluate_expression(self.get_parameter("subject", ""))
        body = self.evaluate_expression(self.get_parameter("body", ""))
        html = self.get_parameter("html", False)
        cc = self.get_parameter("cc", "")
        bcc = self.get_parameter("bcc", "")
        
        # SMTP settings
        smtp_host = self.get_parameter("smtp_host")
        smtp_port = self.get_parameter("smtp_port", 587)
        smtp_user = self.get_parameter("smtp_user")
        smtp_password = self.get_parameter("smtp_password")
        smtp_tls = self.get_parameter("smtp_tls", True)
        
        results = []
        
        for item in self.context.input_data or [{}]:
            try:
                # Evaluate expressions with item data
                item_to = self.evaluate_expression(to, item)
                item_subject = self.evaluate_expression(subject, item)
                item_body = self.evaluate_expression(body, item)
                
                # Create message
                msg = MIMEMultipart('alternative') if html else MIMEText(item_body)
                msg['Subject'] = item_subject
                msg['From'] = smtp_user
                msg['To'] = item_to
                
                if cc:
                    msg['Cc'] = self.evaluate_expression(cc, item)
                if bcc:
                    msg['Bcc'] = self.evaluate_expression(bcc, item)
                
                if html:
                    msg.attach(MIMEText(item_body, 'html'))
                
                # Send email
                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    if smtp_tls:
                        server.starttls()
                    server.login(smtp_user, smtp_password)
                    server.send_message(msg)
                
                results.append({
                    "success": True,
                    "to": item_to,
                    "subject": item_subject,
                    "sent_at": datetime.now(timezone.utc).isoformat()
                })
                
            except Exception as e:
                self.logger.error("Failed to send email", error=str(e))
                results.append({
                    "success": False,
                    "error": str(e),
                    "to": to
                })
        
        return results
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="Send Email",
            type=NodeType.ACTION,
            category=NodeCategory.COMMUNICATION,
            description="Send emails via SMTP",
            icon="mail",
            color="#FA5252",
            parameters=[
                NodeParameter(
                    name="to",
                    type=ParameterType.STRING,
                    required=True,
                    description="Recipient email address",
                    placeholder="user@example.com"
                ),
                NodeParameter(
                    name="subject",
                    type=ParameterType.STRING,
                    required=True,
                    description="Email subject"
                ),
                NodeParameter(
                    name="body",
                    type=ParameterType.STRING,
                    required=True,
                    multiline=True,
                    description="Email body"
                ),
                NodeParameter(
                    name="html",
                    type=ParameterType.BOOLEAN,
                    default=False,
                    description="Send as HTML email"
                ),
                NodeParameter(
                    name="cc",
                    type=ParameterType.STRING,
                    description="CC recipients"
                ),
                NodeParameter(
                    name="bcc",
                    type=ParameterType.STRING,
                    description="BCC recipients"
                ),
                NodeParameter(
                    name="smtp_host",
                    type=ParameterType.STRING,
                    required=True,
                    description="SMTP server host"
                ),
                NodeParameter(
                    name="smtp_port",
                    type=ParameterType.NUMBER,
                    default=587,
                    description="SMTP server port"
                ),
                NodeParameter(
                    name="smtp_user",
                    type=ParameterType.STRING,
                    required=True,
                    description="SMTP username"
                ),
                NodeParameter(
                    name="smtp_password",
                    type=ParameterType.CREDENTIAL,
                    required=True,
                    description="SMTP password"
                ),
                NodeParameter(
                    name="smtp_tls",
                    type=ParameterType.BOOLEAN,
                    default=True,
                    description="Use TLS"
                ),
            ],
            inputs=["main"],
            outputs=["main"]
        )


class DatabaseNode(ActionNode):
    """Database query action node."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute database query."""
        # Get parameters
        operation = self.get_parameter("operation", "SELECT")
        query = self.get_parameter("query", "")
        database_url = self.get_parameter("database", "")
        
        results = []
        
        # Create database engine
        engine = create_engine(database_url)
        
        for item in self.context.input_data or [{}]:
            try:
                # Evaluate query with item data
                evaluated_query = self.evaluate_expression(query, item)
                
                # Execute query
                with engine.connect() as connection:
                    if operation == "SELECT":
                        result = connection.execute(text(evaluated_query))
                        rows = result.fetchall()
                        
                        # Convert rows to dictionaries
                        for row in rows:
                            results.append(dict(row._mapping))
                    
                    else:  # INSERT, UPDATE, DELETE
                        result = connection.execute(text(evaluated_query))
                        connection.commit()
                        
                        results.append({
                            "operation": operation,
                            "affected_rows": result.rowcount,
                            "success": True
                        })
                
            except Exception as e:
                self.logger.error("Database query failed", error=str(e))
                results.append({
                    "success": False,
                    "error": str(e),
                    "query": evaluated_query
                })
        
        return results
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="Database",
            type=NodeType.ACTION,
            category=NodeCategory.DATABASE,
            description="Execute database queries",
            icon="database",
            color="#12B886",
            parameters=[
                NodeParameter(
                    name="operation",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="SELECT",
                    options=["SELECT", "INSERT", "UPDATE", "DELETE"],
                    description="Database operation"
                ),
                NodeParameter(
                    name="query",
                    type=ParameterType.STRING,
                    required=True,
                    multiline=True,
                    description="SQL query to execute"
                ),
                NodeParameter(
                    name="database",
                    type=ParameterType.CREDENTIAL,
                    required=True,
                    description="Database connection string"
                ),
            ],
            inputs=["main"],
            outputs=["main", "error"]
        )


# Additional action nodes can be added here:
# - FileNode (read/write files)
# - SlackNode (send Slack messages)
# - TelegramNode (send Telegram messages)
# - S3Node (AWS S3 operations)
# - etc.