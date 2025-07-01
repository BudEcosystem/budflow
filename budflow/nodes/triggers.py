"""Trigger node implementations."""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
import re

from croniter import croniter
import pytz

from budflow.workflows.models import NodeType
from .base import TriggerNode, NodeDefinition, NodeCategory, NodeParameter, ParameterType


class ManualTriggerNode(TriggerNode):
    """Manual trigger node - triggered by user action."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute manual trigger."""
        # Pass through input data if available
        if self.context.input_data:
            return self.context.input_data
        
        # Otherwise return trigger metadata
        return [{
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "trigger_type": "manual",
            "user_id": self.context.execution_data.get_context("user_id")
        }]
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="Manual Trigger",
            type=NodeType.MANUAL,
            category=NodeCategory.TRIGGER,
            description="Manually trigger workflow execution",
            icon="play-circle",
            color="#4A90E2",
            parameters=[],
            inputs=[],
            outputs=["main"]
        )


class WebhookTriggerNode(TriggerNode):
    """Webhook trigger node - triggered by HTTP webhook."""
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute webhook trigger."""
        # Get webhook configuration
        path = self.get_parameter("path", "/webhook")
        method = self.get_parameter("method", "POST")
        response_mode = self.get_parameter("response_mode", "last_node")
        
        # Pass through webhook data
        if self.context.input_data:
            return self.context.input_data
        
        # Return webhook metadata
        return [{
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "trigger_type": "webhook",
            "webhook_path": path,
            "webhook_method": method,
            "response_mode": response_mode
        }]
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="Webhook Trigger",
            type=NodeType.WEBHOOK,
            category=NodeCategory.TRIGGER,
            description="Trigger workflow via HTTP webhook",
            icon="webhook",
            color="#FF6B6B",
            parameters=[
                NodeParameter(
                    name="path",
                    type=ParameterType.STRING,
                    required=True,
                    description="Webhook path",
                    placeholder="/webhook/my-workflow"
                ),
                NodeParameter(
                    name="method",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="POST",
                    options=["GET", "POST", "PUT", "DELETE", "PATCH"],
                    description="HTTP method"
                ),
                NodeParameter(
                    name="response_mode",
                    type=ParameterType.OPTIONS,
                    default="last_node",
                    options=["last_node", "specific_node", "immediate"],
                    description="How to send webhook response"
                ),
                NodeParameter(
                    name="response_node",
                    type=ParameterType.STRING,
                    description="Node to use for response (if response_mode is specific_node)"
                ),
            ],
            inputs=[],
            outputs=["main"]
        )


class ScheduleTriggerNode(TriggerNode):
    """Schedule trigger node - triggered by cron schedule."""
    
    def validate_cron_expression(self, cron: str) -> bool:
        """Validate cron expression."""
        try:
            croniter(cron)
            return True
        except Exception:
            return False
    
    def get_next_run_time(self) -> Optional[datetime]:
        """Get next scheduled run time."""
        cron = self.get_parameter("cron")
        timezone_str = self.get_parameter("timezone", "UTC")
        
        if not cron:
            return None
        
        try:
            # Get timezone
            tz = pytz.timezone(timezone_str)
            now = datetime.now(tz)
            
            # Calculate next run
            cron_obj = croniter(cron, now)
            next_run = cron_obj.get_next(datetime)
            
            return next_run
            
        except Exception as e:
            self.logger.error("Failed to calculate next run", error=str(e))
            return None
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute schedule trigger."""
        cron = self.get_parameter("cron")
        timezone_str = self.get_parameter("timezone", "UTC")
        
        # Validate cron
        if not self.validate_cron_expression(cron):
            raise ValueError(f"Invalid cron expression: {cron}")
        
        # Get next run time
        next_run = self.get_next_run_time()
        
        return [{
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "trigger_type": "schedule",
            "cron": cron,
            "timezone": timezone_str,
            "next_run": next_run.isoformat() if next_run else None
        }]
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get node definition."""
        return NodeDefinition(
            name="Schedule Trigger",
            type=NodeType.SCHEDULE,
            category=NodeCategory.TRIGGER,
            description="Trigger workflow on a schedule",
            icon="clock",
            color="#51CF66",
            parameters=[
                NodeParameter(
                    name="cron",
                    type=ParameterType.STRING,
                    required=True,
                    description="Cron expression",
                    placeholder="0 */2 * * *"
                ),
                NodeParameter(
                    name="timezone",
                    type=ParameterType.STRING,
                    default="UTC",
                    description="Timezone for schedule",
                    placeholder="America/New_York"
                ),
                NodeParameter(
                    name="active",
                    type=ParameterType.BOOLEAN,
                    default=True,
                    description="Is schedule active"
                ),
            ],
            inputs=[],
            outputs=["main"]
        )


# Additional trigger nodes can be added here:
# - EmailTrigger
# - FileTrigger
# - DatabaseTrigger
# - MessageQueueTrigger
# - etc.