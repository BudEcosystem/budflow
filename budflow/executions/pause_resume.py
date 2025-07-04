"""Pause/Resume execution functionality for BudFlow workflows."""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, List, Set, Optional
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import selectinload

from ..workflows.models import Workflow, WorkflowNode, WorkflowExecution, ExecutionStatus, ExecutionMode
from ..workflows.service import WorkflowService
from ..nodes.registry import NodeRegistry


class PauseType(str, Enum):
    """Types of execution pauses."""
    WEBHOOK = "webhook"
    MANUAL = "manual"
    TIMER = "timer"
    APPROVAL = "approval"


@dataclass
class PauseInfo:
    """Information about a pause condition."""
    type: PauseType
    webhook_url: Optional[str] = None
    timeout: Optional[int] = None
    message: Optional[str] = None
    approver: Optional[str] = None
    expected_data_schema: Optional[Dict[str, Any]] = None
    resume_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "type": self.type.value,
            "webhook_url": self.webhook_url,
            "timeout": self.timeout,
            "message": self.message,
            "approver": self.approver,
            "expected_data_schema": self.expected_data_schema,
            "resume_at": self.resume_at.isoformat() if self.resume_at else None
        }


@dataclass
class ExecutionState:
    """Represents the current state of workflow execution."""
    execution_id: str
    workflow: Workflow
    current_node: Optional[str]
    completed_nodes: Set[str]
    node_data: Dict[str, List[Dict[str, Any]]]
    pause_data: Optional[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "execution_id": self.execution_id,
            "workflow": self.serialize_workflow(),
            "current_node": self.current_node,
            "completed_nodes": list(self.completed_nodes),
            "node_data": self.node_data,
            "pause_data": self.pause_data
        }
    
    def serialize_workflow(self) -> Dict[str, Any]:
        """Serialize workflow for state storage."""
        if self.workflow:
            return {
                "id": self.workflow.id,
                "name": self.workflow.name,
                "nodes": self.workflow.nodes,
                "connections": self.workflow.connections,
                "settings": self.workflow.settings
            }
        return None


@dataclass
class ExecutionPauseData:
    """Database model for storing pause data."""
    execution_id: str
    pause_type: PauseType
    pause_node: str
    serialized_state: Dict[str, Any]
    pause_metadata: Dict[str, Any]
    created_at: datetime
    expires_at: Optional[datetime] = None
    resume_data: Optional[Dict[str, Any]] = None
    resumed_at: Optional[datetime] = None
    resume_trigger: Optional[str] = None


@dataclass
class ExecutionResumeWebhook:
    """Database model for resume webhooks."""
    execution_id: str
    webhook_path: str
    expected_method: str = "POST"
    expected_data_schema: Optional[Dict[str, Any]] = None
    created_at: datetime = None
    expires_at: Optional[datetime] = None
    used_at: Optional[datetime] = None


class PauseResumeError(Exception):
    """Base exception for pause/resume errors."""
    pass


class ExecutionNotPausedError(PauseResumeError):
    """Raised when trying to resume non-paused execution."""
    pass


class PauseTimeoutError(PauseResumeError):
    """Raised when pause has expired."""
    pass


class InvalidResumeDataError(PauseResumeError):
    """Raised when resume data is invalid."""
    pass


class PauseResumeEngine:
    """Engine for handling execution pause/resume functionality."""
    
    def __init__(self, db: AsyncSession, node_registry: Optional[NodeRegistry] = None):
        self.db = db
        if node_registry is None:
            node_registry = NodeRegistry()
        self.workflow_service = WorkflowService(db, node_registry)
        
        # In-memory storage for pause data (in production, use database)
        self._pause_data_store: Dict[str, ExecutionPauseData] = {}
        self._webhook_store: Dict[str, ExecutionResumeWebhook] = {}
    
    async def execute_with_pause_support(
        self,
        execution_id: str,
        workflow: Workflow,
        input_data: Dict[str, Any]
    ) -> WorkflowExecution:
        """
        Execute workflow with pause/resume support.
        
        This is the main entry point that integrates with the execution engine.
        """
        execution = await self.get_or_create_execution(execution_id, workflow, input_data)
        
        # Check if this is a resume
        if execution.status == ExecutionStatus.WAITING:
            return await self.resume_execution(execution_id)
        
        # Start fresh execution with pause support
        execution_state = ExecutionState(
            execution_id=execution_id,
            workflow=workflow,
            current_node=None,
            completed_nodes=set(),
            node_data={},
            pause_data=None
        )
        
        try:
            # Process each node in execution order
            execution_order = self.get_execution_order(workflow)
            for node in execution_order:
                execution_state.current_node = node.name
                
                # Check for pause conditions
                pause_info = await self.check_pause_conditions(node, execution_state)
                if pause_info:
                    await self.pause_execution(execution_state, pause_info)
                    # Update execution status and return
                    execution.status = ExecutionStatus.WAITING
                    await self.db.commit()
                    return execution
                
                # Execute node normally
                result = await self.execute_node(node, execution_state)
                execution_state.node_data[node.name] = result
                execution_state.completed_nodes.add(node.name)
                
                # Save progress
                await self.save_execution_progress(execution_state)
            
            # Complete execution
            execution.status = ExecutionStatus.SUCCESS
            execution.finished_at = datetime.now(timezone.utc)
            await self.db.commit()
            
        except Exception as e:
            execution.status = ExecutionStatus.ERROR
            execution.error = {"message": str(e), "type": type(e).__name__}
            execution.finished_at = datetime.now(timezone.utc)
            await self.db.commit()
            raise
        
        return execution
    
    async def check_pause_conditions(
        self, 
        node: WorkflowNode, 
        execution_state: ExecutionState
    ) -> Optional[PauseInfo]:
        """
        Check if execution should pause at this node.
        """
        node_params = node.parameters if hasattr(node, 'parameters') else {}
        node_type = node.type if hasattr(node, 'type') else getattr(node, 'node_type', 'unknown')
        
        if node_type == 'wait':
            wait_config = node_params.get('wait_config', {})
            wait_type = wait_config.get('type', 'time')
            
            if wait_type == 'webhook':
                return PauseInfo(
                    type=PauseType.WEBHOOK,
                    webhook_url=self.generate_resume_webhook_url(execution_state.execution_id),
                    timeout=wait_config.get('timeout', 3600),
                    expected_data_schema=wait_config.get('schema')
                )
            
            elif wait_type == 'manual':
                return PauseInfo(
                    type=PauseType.MANUAL,
                    message=wait_config.get('message', 'Manual intervention required'),
                    timeout=wait_config.get('timeout')
                )
            
            elif wait_type == 'time':
                wait_seconds = wait_config.get('seconds', 0)
                resume_at = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
                return PauseInfo(
                    type=PauseType.TIMER,
                    resume_at=resume_at
                )
        
        # Check for manual intervention points
        if node_params.get('require_approval', False):
            return PauseInfo(
                type=PauseType.APPROVAL,
                approver=node_params.get('approver'),
                message=node_params.get('approval_message', 'Approval required'),
                timeout=node_params.get('approval_timeout')
            )
        
        return None
    
    async def pause_execution(
        self, 
        execution_state: ExecutionState, 
        pause_info: PauseInfo
    ):
        """
        Pause execution and save state.
        """
        # Serialize current execution state
        serialized_state = await self.serialize_execution_state(execution_state)
        
        # Calculate expiration time
        expires_at = None
        if pause_info.timeout:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=pause_info.timeout)
        
        # Store pause data
        pause_data = ExecutionPauseData(
            execution_id=execution_state.execution_id,
            pause_type=pause_info.type,
            pause_node=execution_state.current_node,
            serialized_state=serialized_state,
            pause_metadata=pause_info.to_dict(),
            created_at=datetime.now(timezone.utc),
            expires_at=expires_at
        )
        
        await self.store_pause_data(pause_data)
        
        # Set up resume triggers
        await self.setup_resume_triggers(pause_info, execution_state.execution_id)
        
        # Update execution status
        await self.update_execution_status(
            execution_state.execution_id, 
            ExecutionStatus.WAITING,
            pause_info=pause_info
        )
    
    async def resume_execution(
        self, 
        execution_id: str, 
        resume_data: Optional[Dict[str, Any]] = None
    ) -> WorkflowExecution:
        """
        Resume paused execution.
        """
        # Load pause data
        pause_data = await self.get_pause_data(execution_id)
        if not pause_data:
            raise ExecutionNotPausedError(f"Execution {execution_id} is not paused")
        
        # Validate resume conditions
        await self.validate_resume_conditions(pause_data, resume_data)
        
        # Deserialize execution state
        execution_state = await self.deserialize_execution_state(
            pause_data.serialized_state
        )
        
        # Merge resume data
        if resume_data:
            execution_state = await self.merge_resume_data(
                execution_state, resume_data, pause_data.pause_node
            )
        
        # Clean up pause data
        await self.cleanup_pause_data(execution_id)
        
        # Continue execution from state
        return await self.continue_execution_from_state(execution_state)
    
    async def serialize_execution_state(self, execution_state: ExecutionState) -> Dict[str, Any]:
        """Serialize execution state for storage."""
        return {
            "execution_id": execution_state.execution_id,
            "workflow": execution_state.serialize_workflow(),
            "current_node": execution_state.current_node,
            "completed_nodes": list(execution_state.completed_nodes),
            "node_data": execution_state.node_data,
            "pause_data": execution_state.pause_data,
            "serialized_at": datetime.now(timezone.utc).isoformat()
        }
    
    async def deserialize_execution_state(self, serialized_state: Dict[str, Any]) -> ExecutionState:
        """Deserialize execution state from storage."""
        workflow_data = serialized_state.get("workflow")
        workflow = None
        if workflow_data:
            workflow = self.deserialize_workflow(workflow_data)
        
        return ExecutionState(
            execution_id=serialized_state["execution_id"],
            workflow=workflow,
            current_node=serialized_state.get("current_node"),
            completed_nodes=set(serialized_state.get("completed_nodes", [])),
            node_data=serialized_state.get("node_data", {}),
            pause_data=serialized_state.get("pause_data")
        )
    
    def deserialize_workflow(self, workflow_data: Dict[str, Any]) -> Workflow:
        """Deserialize workflow from stored data."""
        # Create a mock workflow object for testing
        # In production, this would reconstruct the full Workflow object
        workflow = Workflow(
            id=workflow_data["id"],
            name=workflow_data["name"],
            nodes=workflow_data.get("nodes", []),
            connections=workflow_data.get("connections", []),
            settings=workflow_data.get("settings", {})
        )
        return workflow
    
    def serialize_workflow(self, workflow: Workflow) -> Dict[str, Any]:
        """Serialize workflow for storage."""
        return {
            "id": workflow.id,
            "name": workflow.name,
            "nodes": workflow.nodes,
            "connections": workflow.connections,
            "settings": workflow.settings
        }
    
    async def merge_resume_data(
        self, 
        execution_state: ExecutionState, 
        resume_data: Dict[str, Any], 
        pause_node: str
    ) -> ExecutionState:
        """Merge resume data into execution state."""
        # Add resume data as output from the pause node
        execution_state.node_data[pause_node] = [resume_data]
        return execution_state
    
    async def validate_resume_conditions(
        self, 
        pause_data: ExecutionPauseData, 
        resume_data: Optional[Dict[str, Any]]
    ):
        """Validate that resume conditions are met."""
        # Check if pause has expired
        if pause_data.expires_at and pause_data.expires_at < datetime.now(timezone.utc):
            raise PauseTimeoutError("Pause has expired")
        
        # Additional validation can be added here
        # For example, validate resume data against expected schema
        if pause_data.pause_type == PauseType.WEBHOOK:
            expected_schema = pause_data.pause_metadata.get("expected_data_schema")
            if expected_schema and resume_data:
                # In a real implementation, validate against JSON schema
                pass
    
    def generate_resume_webhook_url(self, execution_id: str) -> str:
        """Generate unique webhook URL for resuming execution."""
        return f"https://api.budflow.com/webhook/resume/{execution_id}"
    
    async def setup_resume_triggers(self, pause_info: PauseInfo, execution_id: str):
        """Set up triggers for resuming execution."""
        if pause_info.type == PauseType.WEBHOOK:
            # Store webhook configuration
            webhook_path = f"/webhook/resume/{execution_id}"
            webhook = ExecutionResumeWebhook(
                execution_id=execution_id,
                webhook_path=webhook_path,
                expected_method="POST",
                expected_data_schema=pause_info.expected_data_schema,
                created_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=pause_info.timeout) if pause_info.timeout else None
            )
            await self.store_resume_webhook(webhook)
        
        elif pause_info.type == PauseType.TIMER:
            # In a real implementation, this would schedule a timer
            # For now, we just record the resume time
            pass
        
        elif pause_info.type in (PauseType.MANUAL, PauseType.APPROVAL):
            # These require external triggers (API calls, UI actions)
            pass
    
    async def store_pause_data(self, pause_data: ExecutionPauseData):
        """Store pause data (in-memory for testing, database in production)."""
        self._pause_data_store[pause_data.execution_id] = pause_data
    
    async def get_pause_data(self, execution_id: str) -> Optional[ExecutionPauseData]:
        """Get pause data for execution."""
        return self._pause_data_store.get(execution_id)
    
    async def cleanup_pause_data(self, execution_id: str):
        """Clean up pause data after resume."""
        self._pause_data_store.pop(execution_id, None)
        self._webhook_store.pop(execution_id, None)
    
    async def store_resume_webhook(self, webhook: ExecutionResumeWebhook):
        """Store resume webhook configuration."""
        self._webhook_store[webhook.execution_id] = webhook
    
    async def update_execution_status(
        self, 
        execution_id: str, 
        status: ExecutionStatus, 
        pause_info: Optional[PauseInfo] = None
    ):
        """Update execution status in database."""
        # In a real implementation, this would update the database
        # For testing, we just simulate the operation
        pass
    
    async def continue_execution_from_state(self, execution_state: ExecutionState) -> WorkflowExecution:
        """Continue execution from restored state."""
        # This would integrate with the main execution engine
        # For testing, we simulate completion
        execution = WorkflowExecution(
            id=int(execution_state.execution_id.split('-')[-1]) if execution_state.execution_id.split('-')[-1].isdigit() else 1,
            workflow_id=execution_state.workflow.id if execution_state.workflow else 1,
            mode=ExecutionMode.MANUAL,
            status=ExecutionStatus.SUCCESS,
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            data=execution_state.node_data
        )
        return execution
    
    # Helper methods for testing
    async def get_or_create_execution(
        self, 
        execution_id: str, 
        workflow: Workflow, 
        input_data: Dict[str, Any]
    ) -> WorkflowExecution:
        """Get existing execution or create new one."""
        # For testing, create a new execution
        return WorkflowExecution(
            id=int(execution_id.split('-')[-1]) if execution_id.split('-')[-1].isdigit() else 1,
            workflow_id=workflow.id,
            mode=ExecutionMode.MANUAL,
            status=ExecutionStatus.NEW,
            started_at=datetime.now(timezone.utc),
            data=input_data
        )
    
    def get_execution_order(self, workflow: Workflow) -> List[WorkflowNode]:
        """Get execution order for workflow nodes."""
        # Simple implementation for testing
        nodes = []
        for node_data in workflow.nodes:
            node = WorkflowNode(
                name=node_data.get("name", node_data.get("id")),
                type=node_data.get("type", "unknown"),
                parameters=node_data.get("parameters", {})
            )
            nodes.append(node)
        return nodes
    
    async def execute_node(
        self, 
        node: WorkflowNode, 
        execution_state: ExecutionState
    ) -> List[Dict[str, Any]]:
        """Execute a single node (simulation for testing)."""
        # Simulate node execution
        return [{"executed": True, "node": node.name, "timestamp": datetime.now(timezone.utc).isoformat()}]
    
    async def save_execution_progress(self, execution_state: ExecutionState):
        """Save execution progress."""
        # In production, this would update the database
        pass