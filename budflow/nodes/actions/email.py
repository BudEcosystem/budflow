"""Email action node for sending emails."""

import asyncio
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Any, Dict, List, Optional
import uuid

import aiosmtplib
import structlog

from budflow.nodes.base import ActionNode, NodeExecutionContext


logger = structlog.get_logger()


class EmailNode(ActionNode):
    """Node for sending emails via SMTP."""
    
    # Node metadata
    name = "Send Email"
    display_name = "Send Email"
    description = "Send emails via SMTP"
    category = "Communication"
    version = "1.0.0"
    tags = ["email", "smtp", "send", "mail"]
    
    def __init__(self, node_data):
        """Initialize Email node."""
        super().__init__(node_data)
    
    @classmethod
    def get_definition(cls):
        """Get node definition."""
        from budflow.nodes.base import NodeDefinition, NodeParameter, ParameterType, NodeType, NodeCategory
        
        return NodeDefinition(
            type=NodeType.ACTION,
            name="Send Email",
            description="Send emails via SMTP",
            category=NodeCategory.COMMUNICATION,
            parameters=[
                NodeParameter(
                    name="to",
                    display_name="To",
                    type=ParameterType.STRING,
                    required=True,
                    description="Recipient email addresses (comma-separated)"
                ),
                NodeParameter(
                    name="subject",
                    display_name="Subject",
                    type=ParameterType.STRING,
                    required=True,
                    description="Email subject"
                ),
                NodeParameter(
                    name="body",
                    display_name="Body",
                    type=ParameterType.STRING,
                    required=True,
                    multiline=True,
                    description="Email body content"
                ),
                NodeParameter(
                    name="bodyType",
                    display_name="Body Type",
                    type=ParameterType.OPTIONS,
                    required=False,
                    default="text",
                    options=["text", "html"]
                )
            ]
        )
    
    async def execute(self, context: NodeExecutionContext) -> Dict[str, Any]:
        """Execute email sending."""
        try:
            # Get email parameters
            to_emails = self._parse_email_addresses(self.parameters.get("to"))
            cc_emails = self._parse_email_addresses(self.parameters.get("cc", ""))
            bcc_emails = self._parse_email_addresses(self.parameters.get("bcc", ""))
            
            if not to_emails:
                return {
                    "success": False,
                    "error": "At least one recipient email is required"
                }
            
            subject = self.parameters.get("subject", "")
            body = self.parameters.get("body", "")
            body_type = self.parameters.get("bodyType", "text")
            
            # Get SMTP credentials
            smtp_config = self._get_smtp_config(context.credentials)
            if not smtp_config["valid"]:
                return {
                    "success": False,
                    "error": smtp_config["error"]
                }
            
            # Create email message
            message = await self._create_message(
                to_emails=to_emails,
                cc_emails=cc_emails,
                bcc_emails=bcc_emails,
                subject=subject,
                body=body,
                body_type=body_type,
                attachments=self.parameters.get("attachments", []),
                from_email=smtp_config["from_email"]
            )
            
            # Send email
            await self._send_email(message, smtp_config)
            
            # Generate message ID
            message_id = str(uuid.uuid4())
            
            return {
                "success": True,
                "messageId": message_id,
                "recipient": ", ".join(to_emails),
                "subject": subject,
                "attachmentCount": len(self.parameters.get("attachments", []))
            }
            
        except Exception as e:
            logger.error(
                "Email sending failed",
                error=str(e),
                to=self.parameters.get("to"),
                subject=self.parameters.get("subject")
            )
            return {
                "success": False,
                "error": f"Email sending failed: {str(e)}"
            }
    
    def _parse_email_addresses(self, email_str: str) -> List[str]:
        """Parse comma-separated email addresses."""
        if not email_str:
            return []
        
        emails = []
        for email in email_str.split(","):
            email = email.strip()
            if email:
                emails.append(email)
        
        return emails
    
    def _get_smtp_config(self, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Get SMTP configuration from credentials."""
        required_fields = ["smtp_host", "smtp_port", "smtp_username", "smtp_password"]
        
        for field in required_fields:
            if not credentials.get(field):
                return {
                    "valid": False,
                    "error": f"Missing SMTP credential: {field}"
                }
        
        return {
            "valid": True,
            "host": credentials["smtp_host"],
            "port": int(credentials["smtp_port"]),
            "username": credentials["smtp_username"],
            "password": credentials["smtp_password"],
            "use_tls": credentials.get("smtp_use_tls", True),
            "from_email": credentials.get("smtp_from_email", credentials["smtp_username"])
        }
    
    async def _create_message(
        self,
        to_emails: List[str],
        cc_emails: List[str],
        bcc_emails: List[str],
        subject: str,
        body: str,
        body_type: str,
        attachments: List[Dict[str, Any]],
        from_email: str
    ) -> MIMEMultipart:
        """Create email message."""
        # Create message container
        if attachments or cc_emails or bcc_emails:
            message = MIMEMultipart()
        else:
            message = MIMEMultipart("alternative")
        
        # Set headers
        message["From"] = from_email
        message["To"] = ", ".join(to_emails)
        if cc_emails:
            message["Cc"] = ", ".join(cc_emails)
        message["Subject"] = subject
        
        # Add body
        if body_type == "html":
            body_part = MIMEText(body, "html", "utf-8")
        else:
            body_part = MIMEText(body, "plain", "utf-8")
        
        message.attach(body_part)
        
        # Add attachments
        for attachment in attachments:
            await self._add_attachment(message, attachment)
        
        return message
    
    async def _add_attachment(
        self,
        message: MIMEMultipart,
        attachment: Dict[str, Any]
    ) -> None:
        """Add attachment to email message."""
        try:
            filename = attachment.get("filename", "attachment")
            content = attachment.get("content", "")
            content_type = attachment.get("contentType", "application/octet-stream")
            
            # Decode base64 content if needed
            if isinstance(content, str):
                try:
                    file_data = base64.b64decode(content)
                except Exception:
                    file_data = content.encode("utf-8")
            else:
                file_data = content
            
            # Create attachment part
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file_data)
            encoders.encode_base64(part)
            
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}"
            )
            
            message.attach(part)
            
        except Exception as e:
            logger.warning(f"Failed to add attachment {attachment.get('filename')}: {str(e)}")
    
    async def _send_email(
        self,
        message: MIMEMultipart,
        smtp_config: Dict[str, Any]
    ) -> None:
        """Send email via SMTP."""
        # Get all recipients
        recipients = []
        
        # Add To recipients
        to_header = message.get("To", "")
        if to_header:
            recipients.extend([email.strip() for email in to_header.split(",")])
        
        # Add CC recipients
        cc_header = message.get("Cc", "")
        if cc_header:
            recipients.extend([email.strip() for email in cc_header.split(",")])
        
        # Add BCC recipients (from parameters, not in headers)
        bcc_emails = self._parse_email_addresses(self.parameters.get("bcc", ""))
        recipients.extend(bcc_emails)
        
        # Send email
        await aiosmtplib.send(
            message,
            hostname=smtp_config["host"],
            port=smtp_config["port"],
            start_tls=smtp_config["use_tls"],
            username=smtp_config["username"],
            password=smtp_config["password"],
            recipients=recipients
        )
    
    def validate_parameters(self) -> Dict[str, Any]:
        """Validate node parameters."""
        errors = []
        
        # Check required parameters
        if not self.parameters.get("to"):
            errors.append("Recipient email address is required")
        
        if not self.parameters.get("subject"):
            errors.append("Email subject is required")
        
        # Validate email addresses
        to_emails = self._parse_email_addresses(self.parameters.get("to", ""))
        for email in to_emails:
            if "@" not in email or "." not in email.split("@")[1]:
                errors.append(f"Invalid email address: {email}")
        
        # Validate body type
        body_type = self.parameters.get("bodyType", "text")
        if body_type not in ["text", "html"]:
            errors.append(f"Invalid body type: {body_type}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }