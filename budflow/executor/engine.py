"""Main workflow execution engine."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowConnection,
    WorkflowExecution, NodeExecution,
    WorkflowStatus, ExecutionStatus, NodeExecutionStatus,
    ExecutionMode
)
from .context import ExecutionContext
from .data import ExecutionData
from .errors import (
    WorkflowExecutionError,
    ExecutionTimeoutError,
    ExecutionCancelledError,
    CircularDependencyError
)
from .runner import NodeRunner

logger = structlog.get_logger()


class WorkflowExecutionEngine:
    """Main workflow execution engine."""
    
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.node_runner = NodeRunner()
        self.logger = logger.bind(component="execution_engine")
    
    async def execute_workflow(
        self,
        workflow_id: int,
        mode: ExecutionMode = ExecutionMode.MANUAL,
        input_data: Optional[Dict[str, Any]] = None,
        user_id: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> WorkflowExecution:
        """Execute a workflow."""
        self.logger.info(
            "Starting workflow execution",
            workflow_id=workflow_id,
            mode=mode.value
        )
        
        # Load workflow with all relationships
        workflow = await self._load_workflow(workflow_id)
        if not workflow:
            raise WorkflowExecutionError(
                f"Workflow {workflow_id} not found",
                workflow_id=workflow_id
            )
        
        # Check workflow status
        if workflow.status != WorkflowStatus.ACTIVE:
            raise WorkflowExecutionError(
                f"Workflow is not active (status: {workflow.status.value})",
                workflow_id=workflow_id,
                error_code="WORKFLOW_NOT_ACTIVE"
            )
        
        # Create workflow execution record
        workflow_execution = await self._create_workflow_execution(
            workflow, mode, input_data, user_id
        )
        
        # Create execution context
        execution_data = ExecutionData(
            execution_id=workflow_execution.uuid,
            input_data=input_data,
            started_at=datetime.now(timezone.utc)
        )
        
        context = ExecutionContext(
            workflow=workflow,
            workflow_execution=workflow_execution,
            db_session=self.db_session,
            execution_data=execution_data,
            timeout_seconds=timeout_seconds
        )
        
        try:
            # Initialize context
            await context.initialize()
            
            # Validate workflow (check for cycles, etc.)
            self._validate_workflow(context)
            
            # Start execution
            workflow_execution.status = ExecutionStatus.RUNNING
            workflow_execution.started_at = datetime.now(timezone.utc)
            await self.db_session.commit()
            
            # Execute workflow
            if timeout_seconds:
                await asyncio.wait_for(
                    self._execute_workflow_internal(context),
                    timeout=timeout_seconds
                )
            else:
                await self._execute_workflow_internal(context)
            
            # Mark as successful if no errors
            if not context.error:
                context.status = ExecutionStatus.SUCCESS
                workflow_execution.status = ExecutionStatus.SUCCESS
                
                # Update workflow statistics
                workflow.total_executions += 1
                workflow.successful_executions += 1
                workflow.last_execution_at = datetime.now(timezone.utc)
            
        except asyncio.TimeoutError:
            context.error = ExecutionTimeoutError(
                f"Workflow execution timed out after {timeout_seconds} seconds",
                timeout_seconds=timeout_seconds
            )
            context.status = ExecutionStatus.ERROR
            workflow_execution.status = ExecutionStatus.ERROR
            
            # Update workflow statistics
            workflow.total_executions += 1
            workflow.failed_executions += 1
            workflow.last_execution_at = datetime.now(timezone.utc)
            
        except ExecutionCancelledError as e:
            context.error = e
            context.status = ExecutionStatus.CANCELED
            workflow_execution.status = ExecutionStatus.CANCELED
            
        except Exception as e:
            self.logger.error(
                "Workflow execution failed",
                workflow_id=workflow_id,
                execution_id=workflow_execution.uuid,
                error=str(e),
                error_type=type(e).__name__
            )
            
            context.error = e
            context.status = ExecutionStatus.ERROR
            workflow_execution.status = ExecutionStatus.ERROR
            
            # Update workflow statistics
            workflow.total_executions += 1
            workflow.failed_executions += 1
            workflow.last_execution_at = datetime.now(timezone.utc)
        
        finally:
            # Save final state
            await context.save_execution_state()
            await self.db_session.commit()
            
            self.logger.info(
                "Workflow execution completed",
                workflow_id=workflow_id,
                execution_id=workflow_execution.uuid,
                status=context.status.value,
                metrics=context.metrics.model_dump()
            )
        
        return workflow_execution
    
    async def _load_workflow(self, workflow_id: int) -> Optional[Workflow]:
        """Load workflow with all relationships."""
        result = await self.db_session.execute(
            select(Workflow)
            .options(
                selectinload(Workflow.nodes),
                selectinload(Workflow.connections)
            )
            .where(Workflow.id == workflow_id)
        )
        return result.scalar_one_or_none()
    
    async def _create_workflow_execution(
        self,
        workflow: Workflow,
        mode: ExecutionMode,
        input_data: Optional[Dict[str, Any]],
        user_id: Optional[int]
    ) -> WorkflowExecution:
        """Create workflow execution record."""
        workflow_execution = WorkflowExecution(
            workflow_id=workflow.id,
            user_id=user_id,
            mode=mode,
            status=ExecutionStatus.NEW,
            data=input_data
        )
        
        self.db_session.add(workflow_execution)
        await self.db_session.commit()
        await self.db_session.refresh(workflow_execution)
        
        return workflow_execution
    
    def _validate_workflow(self, context: ExecutionContext) -> None:
        """Validate workflow structure."""
        # Check for cycles
        cycles = self._detect_cycles(context)
        if cycles:
            raise CircularDependencyError(
                "Workflow contains circular dependencies",
                cycle_path=cycles[0]
            )
        
        # Check for disconnected nodes (optional)
        # For now, we allow disconnected nodes as they might be trigger nodes
    
    def _detect_cycles(self, context: ExecutionContext) -> List[List[int]]:
        """Detect cycles in workflow using DFS."""
        cycles = []
        visited = set()
        rec_stack = set()
        
        def dfs(node_id: int, path: List[int]) -> None:
            visited.add(node_id)
            rec_stack.add(node_id)
            path.append(node_id)
            
            # Check all outgoing connections
            for conn in context.get_outgoing_connections(node_id):
                target_id = conn.target_node_id
                
                if target_id not in visited:
                    dfs(target_id, path[:])
                elif target_id in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(target_id)
                    cycle = path[cycle_start:] + [target_id]
                    cycles.append(cycle)
            
            path.pop()
            rec_stack.remove(node_id)
        
        # Check all nodes
        for node in context.workflow.nodes:
            if node.id not in visited:
                dfs(node.id, [])
        
        return cycles
    
    async def _execute_workflow_internal(self, context: ExecutionContext) -> None:
        """Internal workflow execution logic."""
        # Get start nodes (triggers or nodes with no incoming connections)
        start_nodes = context.get_trigger_nodes() or context.get_start_nodes()
        
        if not start_nodes:
            self.logger.warning("No start nodes found in workflow")
            return
        
        # Queue for nodes to execute
        execution_queue: asyncio.Queue[WorkflowNode] = asyncio.Queue()
        
        # Add start nodes to queue
        for node in start_nodes:
            await execution_queue.put(node)
        
        # Track nodes in queue to avoid duplicates
        queued_nodes: Set[int] = {node.id for node in start_nodes}
        
        # Execute nodes sequentially
        while not execution_queue.empty():
            # Check for cancellation
            context.check_cancelled()
            
            # Get next node
            node = await execution_queue.get()
            
            # Skip if already executed
            if node.id in context.executed_nodes:
                continue
            
            # Skip if dependencies not met
            if not context.is_node_ready(node.id):
                # Re-queue for later
                await execution_queue.put(node)
                continue
            
            # Execute node
            try:
                await self._execute_node(context, node)
                
                # Mark as executed
                context.mark_node_executed(node.id, success=True)
                context.metrics.increment_executed()
                context.metrics.increment_successful()
                
                # Queue next nodes
                next_nodes = context.get_next_nodes(node.id)
                for next_node in next_nodes:
                    if next_node.id not in queued_nodes:
                        await execution_queue.put(next_node)
                        queued_nodes.add(next_node.id)
                
            except Exception as e:
                # Mark as failed
                context.mark_node_executed(node.id, success=False)
                context.metrics.increment_executed()
                context.metrics.increment_failed()
                
                # Log error
                self.logger.error(
                    "Node execution failed",
                    node_id=node.id,
                    node_name=node.name,
                    error=str(e)
                )
                
                # Decide whether to continue or fail workflow
                if node.always_output_data:
                    # Continue with empty output
                    context.execution_data.set_node_output(
                        str(node.id),
                        [],
                        "main"
                    )
                    
                    # Still queue next nodes
                    next_nodes = context.get_next_nodes(node.id)
                    for next_node in next_nodes:
                        if next_node.id not in queued_nodes:
                            await execution_queue.put(next_node)
                            queued_nodes.add(next_node.id)
                else:
                    # Fail the workflow
                    raise WorkflowExecutionError(
                        f"Workflow failed at node '{node.name}'",
                        workflow_id=context.workflow.id,
                        execution_id=context.workflow_execution.id,
                        details={"node_id": node.id, "error": str(e)}
                    )
    
    async def _execute_node(
        self,
        context: ExecutionContext,
        node: WorkflowNode
    ) -> None:
        """Execute a single node."""
        context.logger.info(
            "Executing node",
            node_id=node.id,
            node_name=node.name,
            node_type=node.type.value
        )
        
        # Create node execution record
        node_execution = NodeExecution(
            workflow_execution_id=context.workflow_execution.id,
            node_id=node.id,
            status=NodeExecutionStatus.NEW
        )
        self.db_session.add(node_execution)
        await self.db_session.commit()
        await self.db_session.refresh(node_execution)
        
        # Store in context
        context.node_executions[node.id] = node_execution
        
        # Get input data for node
        incoming_connections = [
            {
                "source_node_id": str(conn.source_node_id),
                "source_output": conn.source_output,
                "target_input": conn.target_input,
            }
            for conn in context.get_incoming_connections(node.id)
        ]
        
        input_data = context.execution_data.get_input_data_for_node(
            str(node.id),
            incoming_connections
        )
        
        # Log input data
        node_execution.input_data = {
            "items": input_data,
            "count": len(input_data)
        }
        
        # Create node execution context
        node_context = context.create_node_execution_context(
            node=node,
            node_execution=node_execution,
            input_data=input_data,
            credentials=node.credentials  # TODO: Decrypt credentials
        )
        
        # Run the node
        await self.node_runner.run_node(
            node_context,
            timeout_seconds=node.parameters.get("timeout")
        )
        
        # Save node execution state
        await self.db_session.commit()
        
        # Update metrics
        context.metrics.add_items_processed(len(node_context.output_data))
    
    async def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running execution."""
        # This would need to track running executions in a registry
        # For now, return False
        self.logger.warning("Execution cancellation not fully implemented")
        return False
    
    async def get_execution_status(
        self,
        execution_id: str
    ) -> Optional[WorkflowExecution]:
        """Get execution status."""
        result = await self.db_session.execute(
            select(WorkflowExecution)
            .options(selectinload(WorkflowExecution.node_executions))
            .where(WorkflowExecution.uuid == execution_id)
        )
        return result.scalar_one_or_none()
    
    async def resume_execution(
        self,
        execution_id: str,
        push_ref: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Resume a workflow execution (used by queue worker).
        
        Args:
            execution_id: ID of the execution to resume
            push_ref: Reference for real-time updates (WebSocket/SSE)
            
        Returns:
            Execution result data
        """
        # Get execution with workflow
        result = await self.db_session.execute(
            select(WorkflowExecution)
            .options(
                selectinload(WorkflowExecution.workflow).selectinload(Workflow.nodes),
                selectinload(WorkflowExecution.workflow).selectinload(Workflow.connections),
                selectinload(WorkflowExecution.node_executions)
            )
            .where(WorkflowExecution.uuid == execution_id)
        )
        execution = result.scalar_one_or_none()
        
        if not execution:
            raise WorkflowExecutionError(
                f"Execution {execution_id} not found",
                workflow_id=None,
                execution_id=execution_id
            )
        
        # Check if already finished
        if execution.status in [ExecutionStatus.SUCCESS, ExecutionStatus.ERROR, ExecutionStatus.CANCELED]:
            self.logger.warning(
                f"Execution {execution_id} already finished with status {execution.status}"
            )
            return {
                "execution_id": execution.uuid,
                "status": execution.status.value,
                "skipped": True
            }
        
        # Update execution status
        execution.status = ExecutionStatus.RUNNING
        
        # Store push_ref in execution data for real-time updates
        if push_ref:
            if not execution.data:
                execution.data = {}
            execution.data["push_ref"] = push_ref
        
        await self.db_session.commit()
        
        # Create execution context
        context = ExecutionContext(
            workflow=execution.workflow,
            workflow_execution=execution,
            db_session=self.db_session
        )
        await context.initialize()
        
        # Set up initial data if provided
        if execution.data and "input_data" in execution.data:
            context.execution_data = ExecutionData(
                execution_id=execution.uuid,
                input_data=execution.data["input_data"]
            )
        
        # Resume execution from where it left off
        # Check which nodes have already been executed
        executed_node_ids = {
            ne.node_id for ne in execution.node_executions
            if ne.status == NodeExecutionStatus.SUCCESS
        }
        
        for node_id in executed_node_ids:
            context.mark_node_executed(node_id, success=True)
        
        try:
            # Continue execution
            await self._execute_workflow(context)
            
            # Update execution status
            execution.status = ExecutionStatus.SUCCESS
            execution.finished_at = datetime.now(timezone.utc)
            
            if execution.started_at:
                execution.duration_ms = int(
                    (execution.finished_at - execution.started_at).total_seconds() * 1000
                )
                execution.execution_time_ms = execution.duration_ms
            
            # Store final data
            execution.data = execution.data or {}
            execution.data["resultData"] = {
                "runData": context.execution_data.to_dict(),
                "lastNodeExecuted": max(context.executed_nodes) if context.executed_nodes else None
            }
            
            await self.db_session.commit()
            
            return {
                "execution_id": execution.uuid,
                "status": "success",
                "data": execution.data["resultData"],
                "duration_ms": execution.duration_ms
            }
            
        except Exception as e:
            # Mark execution as failed
            execution.status = ExecutionStatus.ERROR
            execution.finished_at = datetime.now(timezone.utc)
            execution.error = {
                "message": str(e),
                "name": type(e).__name__,
                "stack": None  # Could add traceback here
            }
            
            await self.db_session.commit()
            
            self.logger.error(
                f"Execution {execution_id} failed: {e}",
                exc_info=True
            )
            
            raise