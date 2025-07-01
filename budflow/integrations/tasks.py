"""Celery tasks for integrations and external services.

This module contains tasks for handling integrations with external services,
including webhooks, API calls, and third-party service interactions.
"""

import asyncio
import httpx
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
from uuid import UUID

import structlog
from celery import Task
from celery.exceptions import Retry

from budflow.config import settings
from budflow.database import get_db
from budflow.webhooks.service import WebhookService
from budflow.credentials.service import CredentialService
from budflow.core.binary_data import BinaryDataManager
from budflow.worker import celery_app
from budflow.metrics import metrics

logger = structlog.get_logger()


class IntegrationTask(Task):
    """Base class for integration tasks."""
    
    autoretry_for = (httpx.TimeoutException, httpx.NetworkError)
    retry_kwargs = {'max_retries': 3, 'countdown': 10}
    retry_backoff = True
    retry_backoff_max = 600
    
    def before_start(self, task_id, args, kwargs):
        """Called before task execution starts."""
        logger.info(
            "Starting integration task",
            task_id=task_id,
            task_name=self.name,
            kwargs=kwargs
        )
        metrics.increment("integration_tasks_started", tags={"task": self.name})
    
    def on_success(self, retval, task_id, args, kwargs):
        """Called on successful task completion."""
        metrics.increment("integration_tasks_completed", tags={"task": self.name})
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Called on task failure."""
        logger.error(
            "Integration task failed",
            task_id=task_id,
            task_name=self.name,
            error=str(exc),
            exc_info=einfo
        )
        metrics.increment("integration_tasks_failed", tags={"task": self.name})


@celery_app.task(
    base=IntegrationTask,
    name="budflow.process_webhook",
    queue="integrations",
    time_limit=60
)
def process_webhook_task(
    webhook_id: str,
    request_data: Dict[str, Any],
    headers: Dict[str, str],
    method: str = "POST"
) -> Dict[str, Any]:
    """Process incoming webhook data.
    
    Args:
        webhook_id: ID of the webhook
        request_data: Request body data
        headers: Request headers
        method: HTTP method
    
    Returns:
        Processing result
    """
    return asyncio.run(_process_webhook(webhook_id, request_data, headers, method))


async def _process_webhook(
    webhook_id: str,
    request_data: Dict[str, Any],
    headers: Dict[str, str],
    method: str
) -> Dict[str, Any]:
    """Async implementation of webhook processing."""
    async for session in get_db():
        webhook_service = WebhookService(session)
        binary_manager = BinaryDataManager()
        
        # Get webhook
        webhook = await webhook_service.get_webhook(UUID(webhook_id))
        if not webhook:
            raise ValueError(f"Webhook {webhook_id} not found")
        
        # Process binary data if present
        processed_data = request_data
        if headers.get("content-type", "").startswith("multipart/"):
            # Store binary data and replace with references
            for key, value in request_data.items():
                if isinstance(value, bytes):
                    # Store binary data
                    binary_id = await binary_manager.store(
                        data=value,
                        metadata={
                            "webhook_id": webhook_id,
                            "field": key,
                            "content_type": headers.get("content-type")
                        }
                    )
                    processed_data[key] = {"binary_id": str(binary_id)}
        
        # Trigger workflow execution
        from budflow.workflows.tasks import execute_workflow_task
        
        result = execute_workflow_task.apply_async(
            kwargs={
                "workflow_id": str(webhook.workflow_id),
                "trigger_data": {
                    "type": "webhook",
                    "webhook_id": webhook_id,
                    "method": method,
                    "headers": headers,
                    "data": processed_data
                },
                "initial_data": processed_data,
                "execution_mode": "webhook"
            }
        )
        
        return {
            "webhook_id": webhook_id,
            "workflow_triggered": True,
            "task_id": result.id,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }


@celery_app.task(
    base=IntegrationTask,
    name="budflow.http_request",
    queue="integrations",
    time_limit=120
)
def http_request_task(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    body: Optional[Dict[str, Any]] = None,
    auth: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    retry_on_failure: bool = True
) -> Dict[str, Any]:
    """Make HTTP request to external service.
    
    Args:
        url: URL to request
        method: HTTP method
        headers: Request headers
        body: Request body
        auth: Authentication credentials
        timeout: Request timeout in seconds
        retry_on_failure: Whether to retry on failure
    
    Returns:
        Response data
    """
    return asyncio.run(_http_request(
        url, method, headers, body, auth, timeout, retry_on_failure
    ))


async def _http_request(
    url: str,
    method: str,
    headers: Optional[Dict[str, str]],
    body: Optional[Dict[str, Any]],
    auth: Optional[Dict[str, str]],
    timeout: int,
    retry_on_failure: bool
) -> Dict[str, Any]:
    """Async implementation of HTTP request."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            # Prepare auth
            auth_obj = None
            if auth:
                if auth["type"] == "basic":
                    auth_obj = httpx.BasicAuth(auth["username"], auth["password"])
                elif auth["type"] == "bearer":
                    headers = headers or {}
                    headers["Authorization"] = f"Bearer {auth['token']}"
            
            # Make request
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                json=body if body else None,
                auth=auth_obj
            )
            
            # Parse response
            response_data = {
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "body": None
            }
            
            if response.headers.get("content-type", "").startswith("application/json"):
                response_data["body"] = response.json()
            else:
                response_data["body"] = response.text
            
            # Check for errors
            if response.status_code >= 400:
                if retry_on_failure and http_request_task.request.retries < http_request_task.max_retries:
                    raise http_request_task.retry(
                        exc=Exception(f"HTTP {response.status_code}: {response.text}")
                    )
                
            return response_data
            
        except httpx.TimeoutException as e:
            if retry_on_failure:
                raise http_request_task.retry(exc=e)
            raise
        except Exception as e:
            logger.error(f"HTTP request failed: {str(e)}")
            raise


@celery_app.task(
    base=IntegrationTask,
    name="budflow.send_email",
    queue="integrations"
)
def send_email_task(
    to: List[str],
    subject: str,
    body: str,
    from_email: Optional[str] = None,
    cc: Optional[List[str]] = None,
    bcc: Optional[List[str]] = None,
    attachments: Optional[List[Dict[str, Any]]] = None,
    smtp_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Send email via SMTP.
    
    Args:
        to: List of recipient emails
        subject: Email subject
        body: Email body (HTML or plain text)
        from_email: Sender email
        cc: CC recipients
        bcc: BCC recipients
        attachments: List of attachments
        smtp_config: SMTP configuration
    
    Returns:
        Send result
    """
    return asyncio.run(_send_email(
        to, subject, body, from_email, cc, bcc, attachments, smtp_config
    ))


async def _send_email(
    to: List[str],
    subject: str,
    body: str,
    from_email: Optional[str],
    cc: Optional[List[str]],
    bcc: Optional[List[str]],
    attachments: Optional[List[Dict[str, Any]]],
    smtp_config: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Async implementation of email sending."""
    import aiosmtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email import encoders
    
    # Use provided config or default from settings
    config = smtp_config or {
        "host": settings.smtp_host,
        "port": settings.smtp_port,
        "username": settings.smtp_username,
        "password": settings.smtp_password,
        "use_tls": settings.smtp_use_tls
    }
    
    # Create message
    msg = MIMEMultipart()
    msg["From"] = from_email or config.get("from_email", "noreply@budflow.io")
    msg["To"] = ", ".join(to)
    msg["Subject"] = subject
    
    if cc:
        msg["Cc"] = ", ".join(cc)
    
    # Add body
    msg.attach(MIMEText(body, "html" if "<html>" in body else "plain"))
    
    # Add attachments
    if attachments:
        binary_manager = BinaryDataManager()
        for attachment in attachments:
            if "binary_id" in attachment:
                # Fetch binary data
                data = await binary_manager.get(UUID(attachment["binary_id"]))
                part = MIMEBase("application", "octet-stream")
                part.set_payload(data)
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={attachment.get('filename', 'attachment')}"
                )
                msg.attach(part)
    
    # Send email
    all_recipients = to + (cc or []) + (bcc or [])
    
    try:
        await aiosmtplib.send(
            msg,
            hostname=config["host"],
            port=config["port"],
            username=config.get("username"),
            password=config.get("password"),
            use_tls=config.get("use_tls", True)
        )
        
        return {
            "sent": True,
            "recipients": all_recipients,
            "message_id": msg.get("Message-ID"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        raise


@celery_app.task(
    base=IntegrationTask,
    name="budflow.database_query",
    queue="integrations",
    time_limit=300
)
def database_query_task(
    connection_string: str,
    query: str,
    parameters: Optional[Dict[str, Any]] = None,
    fetch_size: int = 1000
) -> Dict[str, Any]:
    """Execute database query.
    
    Args:
        connection_string: Database connection string
        query: SQL query to execute
        parameters: Query parameters
        fetch_size: Number of rows to fetch
    
    Returns:
        Query result
    """
    return asyncio.run(_database_query(
        connection_string, query, parameters, fetch_size
    ))


async def _database_query(
    connection_string: str,
    query: str,
    parameters: Optional[Dict[str, Any]],
    fetch_size: int
) -> Dict[str, Any]:
    """Async implementation of database query."""
    import asyncpg
    
    # Parse connection string to determine database type
    if connection_string.startswith("postgresql://"):
        # PostgreSQL
        conn = await asyncpg.connect(connection_string)
        try:
            # Execute query
            if query.strip().upper().startswith("SELECT"):
                rows = await conn.fetch(query, **(parameters or {}))
                return {
                    "rows": [dict(row) for row in rows[:fetch_size]],
                    "row_count": len(rows),
                    "truncated": len(rows) > fetch_size
                }
            else:
                result = await conn.execute(query, **(parameters or {}))
                return {
                    "affected_rows": result,
                    "success": True
                }
        finally:
            await conn.close()
    else:
        raise ValueError(f"Unsupported database type: {connection_string}")


@celery_app.task(
    base=IntegrationTask,
    name="budflow.sync_credentials",
    queue="integrations"
)
def sync_credentials_task(
    credential_id: str,
    provider: str
) -> Dict[str, Any]:
    """Sync credentials with external provider.
    
    Args:
        credential_id: ID of the credential to sync
        provider: Provider name
    
    Returns:
        Sync result
    """
    return asyncio.run(_sync_credentials(credential_id, provider))


async def _sync_credentials(
    credential_id: str,
    provider: str
) -> Dict[str, Any]:
    """Async implementation of credential sync."""
    async for session in get_db():
        credential_service = CredentialService(session)
        
        # Get credential
        credential = await credential_service.get_credential(UUID(credential_id))
        if not credential:
            raise ValueError(f"Credential {credential_id} not found")
        
        # Sync based on provider
        if provider == "oauth2":
            # Refresh OAuth2 token
            if credential.data.get("refresh_token"):
                # Make token refresh request
                response = await _http_request(
                    url=credential.data["token_url"],
                    method="POST",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    body={
                        "grant_type": "refresh_token",
                        "refresh_token": credential.data["refresh_token"],
                        "client_id": credential.data["client_id"],
                        "client_secret": credential.data["client_secret"]
                    },
                    auth=None,
                    timeout=30,
                    retry_on_failure=True
                )
                
                if response["status_code"] == 200:
                    # Update credential with new tokens
                    credential.data.update(response["body"])
                    await credential_service.update_credential(
                        credential_id=UUID(credential_id),
                        data=credential.data
                    )
                    
                    return {
                        "synced": True,
                        "provider": provider,
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }
        
        return {
            "synced": False,
            "provider": provider,
            "reason": "No sync required"
        }


# Task routing
celery_app.conf.task_routes.update({
    'budflow.process_webhook': {'queue': 'integrations'},
    'budflow.http_request': {'queue': 'integrations'},
    'budflow.send_email': {'queue': 'integrations'},
    'budflow.database_query': {'queue': 'integrations'},
    'budflow.sync_credentials': {'queue': 'integrations'},
})