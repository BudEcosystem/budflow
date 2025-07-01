"""Execution context classes."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowConnection,
    WorkflowExecution, NodeExecution,
    ExecutionStatus, NodeExecutionStatus
)
from .data import ExecutionData, ExecutionMetrics
from .errors import ExecutionCancelledError

logger = structlog.get_logger()


class NodeExecutionContext:
    """Context for a single node execution."""
    
    def __init__(
        self,
        node: WorkflowNode,
        node_execution: NodeExecution,
        input_data: List[Dict[str, Any]],
        execution_data: ExecutionData,
        credentials: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        self.node = node
        self.node_execution = node_execution
        self.input_data = input_data
        self.execution_data = execution_data
        self.credentials = credentials
        self.parameters = parameters or {}
        
        # Execution state
        self.output_data: List[Dict[str, Any]] = []
        self.error: Optional[Exception] = None
        self.status = NodeExecutionStatus.NEW
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        
        # Retry state
        self.current_try = 1
        self.max_tries = node.max_tries if node.retry_on_fail else 1
        
    def get_parameter(self, key: str, default: Any = None) -> Any:
        """Get a parameter value."""
        return self.parameters.get(key, default)
    
    def set_output(self, data: List[Dict[str, Any]]) -> None:
        """Set node output data."""
        self.output_data = data
    
    def add_output_item(self, item: Dict[str, Any]) -> None:
        """Add a single output item."""
        self.output_data.append(item)
    
    def get_context_value(self, key: str, default: Any = None) -> Any:
        """Get a value from execution context."""
        return self.execution_data.get_context(key, default)
    
    def set_context_value(self, key: str, value: Any) -> None:
        """Set a value in execution context."""
        self.execution_data.set_context(key, value)
    
    @property
    def should_retry(self) -> bool:
        """Check if node should be retried."""
        return (
            self.node.retry_on_fail and
            self.current_try < self.max_tries and
            self.error is not None
        )
    
    @property
    def execution_time_ms(self) -> Optional[int]:
        """Get execution time in milliseconds."""
        if self.started_at and self.finished_at:
            delta = self.finished_at - self.started_at
            return int(delta.total_seconds() * 1000)
        return None


class ExecutionContext:
    """Context for workflow execution."""
    
    def __init__(
        self,
        workflow: Workflow,
        workflow_execution: WorkflowExecution,
        db_session: AsyncSession,
        execution_data: Optional[ExecutionData] = None,
        timeout_seconds: Optional[int] = None,
    ):
        self.workflow = workflow
        self.workflow_execution = workflow_execution
        self.db_session = db_session
        self.execution_data = execution_data or ExecutionData(
            execution_id=workflow_execution.uuid,
            started_at=datetime.now(timezone.utc)
        )
        self.timeout_seconds = timeout_seconds
        
        # Execution state
        self.status = ExecutionStatus.NEW
        self.error: Optional[Exception] = None
        self.cancelled = False
        self.cancel_event = asyncio.Event()
        
        # Metrics
        self.metrics = ExecutionMetrics()
        
        # Node tracking
        self.executed_nodes: Set[int] = set()
        self.failed_nodes: Set[int] = set()
        self.node_executions: Dict[int, NodeExecution] = {}
        
        # Workflow graph
        self.nodes_by_id: Dict[int, WorkflowNode] = {}
        self.connections_by_target: Dict[int, List[WorkflowConnection]] = {}
        self.connections_by_source: Dict[int, List[WorkflowConnection]] = {}
        
        # Logger with context
        self.logger = logger.bind(
            workflow_id=workflow.id,
            execution_id=workflow_execution.uuid
        )
    
    async def initialize(self) -> None:
        """Initialize execution context."""
        # Build node index
        for node in self.workflow.nodes:
            self.nodes_by_id[node.id] = node
        
        # Build connection indices
        for connection in self.workflow.connections:
            # Index by target
            if connection.target_node_id not in self.connections_by_target:
                self.connections_by_target[connection.target_node_id] = []
            self.connections_by_target[connection.target_node_id].append(connection)
            
            # Index by source
            if connection.source_node_id not in self.connections_by_source:
                self.connections_by_source[connection.source_node_id] = []
            self.connections_by_source[connection.source_node_id].append(connection)
    
    def get_node(self, node_id: int) -> Optional[WorkflowNode]:
        """Get node by ID."""
        return self.nodes_by_id.get(node_id)
    
    def get_incoming_connections(self, node_id: int) -> List[WorkflowConnection]:
        """Get incoming connections for a node."""
        return self.connections_by_target.get(node_id, [])
    
    def get_outgoing_connections(self, node_id: int) -> List[WorkflowConnection]:
        """Get outgoing connections from a node."""
        return self.connections_by_source.get(node_id, [])
    
    def get_trigger_nodes(self) -> List[WorkflowNode]:
        """Get all trigger nodes in workflow."""
        return [
            node for node in self.workflow.nodes
            if node.is_trigger_node and not node.disabled
        ]
    
    def get_start_nodes(self) -> List[WorkflowNode]:
        """Get nodes with no incoming connections (start nodes)."""
        start_nodes = []
        for node in self.workflow.nodes:
            if not node.disabled and node.id not in self.connections_by_target:
                start_nodes.append(node)
        return start_nodes
    
    def get_next_nodes(self, node_id: int) -> List[WorkflowNode]:
        """Get next nodes to execute after a given node."""
        next_nodes = []
        outgoing = self.get_outgoing_connections(node_id)
        
        for connection in outgoing:
            target_node = self.get_node(connection.target_node_id)
            if target_node and not target_node.disabled:
                next_nodes.append(target_node)
        
        return next_nodes
    
    def is_node_ready(self, node_id: int) -> bool:
        """Check if all dependencies of a node have been executed."""
        incoming = self.get_incoming_connections(node_id)
        
        for connection in incoming:
            if connection.source_node_id not in self.executed_nodes:
                # Check if source node is disabled
                source_node = self.get_node(connection.source_node_id)
                if source_node and not source_node.disabled:
                    return False
        
        return True
    
    def mark_node_executed(self, node_id: int, success: bool = True) -> None:
        """Mark a node as executed."""
        self.executed_nodes.add(node_id)
        if not success:
            self.failed_nodes.add(node_id)
    
    def cancel(self) -> None:
        """Cancel the execution."""
        self.cancelled = True
        self.cancel_event.set()
        self.logger.info("Execution cancelled")
    
    def check_cancelled(self) -> None:
        """Check if execution was cancelled and raise if so."""
        if self.cancelled:
            raise ExecutionCancelledError()
    
    async def save_execution_state(self) -> None:
        """Save current execution state to database."""
        self.workflow_execution.status = self.status
        if self.error:
            self.workflow_execution.error = {
                "message": str(self.error),
                "type": type(self.error).__name__,
            }
        
        if self.status in (ExecutionStatus.SUCCESS, ExecutionStatus.ERROR, ExecutionStatus.CANCELED):
            self.workflow_execution.finished_at = datetime.now(timezone.utc)
            if self.workflow_execution.started_at:
                delta = self.workflow_execution.finished_at - self.workflow_execution.started_at
                self.workflow_execution.duration_ms = int(delta.total_seconds() * 1000)
        
        await self.db_session.commit()
    
    def create_node_execution_context(
        self,
        node: WorkflowNode,
        node_execution: NodeExecution,
        input_data: List[Dict[str, Any]],
        credentials: Optional[Dict[str, Any]] = None,
    ) -> NodeExecutionContext:
        """Create execution context for a node."""
        return NodeExecutionContext(
            node=node,
            node_execution=node_execution,
            input_data=input_data,
            execution_data=self.execution_data,
            credentials=credentials,
            parameters=node.parameters,
        )