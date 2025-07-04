"""Main workflow execution engine."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import structlog
import networkx as nx
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
    CircularDependencyError,
    InvalidStartNodeError
)
from .runner import NodeRunner
from .directed_graph import DirectedGraphExecutor

logger = structlog.get_logger()


class WorkflowExecutionEngine:
    """Main workflow execution engine."""
    
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.node_runner = NodeRunner()
        self.directed_graph_executor = DirectedGraphExecutor(db_session)
        self.logger = logger.bind(component="execution_engine")
    
    async def execute_workflow(
        self,
        workflow_id: int,
        mode: ExecutionMode = ExecutionMode.MANUAL,
        input_data: Optional[Dict[str, Any]] = None,
        user_id: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        parent_execution: Optional[WorkflowExecution] = None,
        execution_depth: int = 0,
        execution_stack: Optional[List[int]] = None,
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
            workflow, mode, input_data, user_id, parent_execution, execution_depth, execution_stack
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
                selectinload(Workflow.workflow_nodes),
                selectinload(Workflow.workflow_connections)
            )
            .where(Workflow.id == workflow_id)
        )
        return result.scalar_one_or_none()
    
    async def _create_workflow_execution(
        self,
        workflow: Workflow,
        mode: ExecutionMode,
        input_data: Optional[Dict[str, Any]],
        user_id: Optional[int],
        parent_execution: Optional[WorkflowExecution] = None,
        execution_depth: int = 0,
        execution_stack: Optional[List[int]] = None
    ) -> WorkflowExecution:
        """Create workflow execution record."""
        workflow_execution = WorkflowExecution(
            workflow_id=workflow.id,
            user_id=user_id,
            mode=mode,
            status=ExecutionStatus.NEW,
            data=input_data,
            parent_execution_id=parent_execution.id if parent_execution else None,
            execution_depth=execution_depth,
            execution_stack=execution_stack or []
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
        for node in context.workflow.workflow_nodes:
            if node.id not in visited:
                dfs(node.id, [])
        
        return cycles
    
    async def _execute_workflow_internal(self, context: ExecutionContext) -> None:
        """Internal workflow execution logic using directed graph algorithm."""
        # Create execution plan using directed graph executor
        execution_plan = await self.directed_graph_executor.create_execution_plan(context.workflow)
        
        self.logger.info(
            "Created execution plan",
            execution_order=execution_plan.execution_order,
            parallel_groups=len(execution_plan.parallel_groups),
            critical_path_length=len(execution_plan.critical_path)
        )
        
        # Execute nodes according to the optimized plan
        await self._execute_nodes_with_plan(context, execution_plan)
    
    async def _execute_nodes_with_plan(self, context: ExecutionContext, execution_plan) -> None:
        """Execute nodes according to the directed graph execution plan."""
        # Track executed nodes
        executed_nodes: Set[int] = set()
        
        # Execute parallel groups in order
        for group in execution_plan.parallel_groups:
            # Check for cancellation
            context.check_cancelled()
            
            if len(group) == 1:
                # Single node - execute sequentially
                node_id = group[0]
                if node_id not in executed_nodes:
                    node = context.get_node(node_id)
                    if node and not node.disabled:
                        await self._execute_single_node_with_dependencies(
                            context, node, executed_nodes
                        )
                        executed_nodes.add(node_id)
            else:
                # Multiple nodes - execute in parallel
                await self._execute_parallel_group(context, group, executed_nodes)
    
    async def _execute_single_node_with_dependencies(
        self,
        context: ExecutionContext,
        node: WorkflowNode,
        executed_nodes: Set[int]
    ) -> None:
        """Execute a single node after ensuring all dependencies are met."""
        # Check if all dependencies are satisfied
        incoming = context.get_incoming_connections(node.id)
        for connection in incoming:
            if connection.source_node_id not in executed_nodes:
                source_node = context.get_node(connection.source_node_id)
                if source_node and not source_node.disabled:
                    # Dependency not met - this should not happen with proper planning
                    self.logger.warning(
                        "Dependency not met during execution",
                        node_id=node.id,
                        missing_dependency=connection.source_node_id
                    )
                    return
        
        # Execute the node
        try:
            await self._execute_node(context, node)
            
            # Mark as executed
            context.mark_node_executed(node.id, success=True)
            context.metrics.increment_executed()
            context.metrics.increment_successful()
            
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
            else:
                # Fail the workflow
                raise WorkflowExecutionError(
                    f"Workflow failed at node '{node.name}'",
                    workflow_id=context.workflow.id,
                    execution_id=context.workflow_execution.id,
                    details={"node_id": node.id, "error": str(e)}
                )
    
    async def _execute_parallel_group(
        self,
        context: ExecutionContext,
        group: List[int],
        executed_nodes: Set[int]
    ) -> None:
        """Execute a group of nodes in parallel."""
        # Filter out already executed and disabled nodes
        nodes_to_execute = []
        for node_id in group:
            if node_id not in executed_nodes:
                node = context.get_node(node_id)
                if node and not node.disabled:
                    # Check if dependencies are met
                    if self._are_dependencies_met(context, node_id, executed_nodes):
                        nodes_to_execute.append(node)
        
        if not nodes_to_execute:
            return
        
        self.logger.info(
            "Executing parallel group",
            group_size=len(nodes_to_execute),
            node_ids=[n.id for n in nodes_to_execute]
        )
        
        # Create tasks for parallel execution
        tasks = []
        for node in nodes_to_execute:
            task = asyncio.create_task(
                self._execute_single_node_with_dependencies(context, node, executed_nodes)
            )
            tasks.append((node.id, task))
        
        # Wait for all tasks to complete
        for node_id, task in tasks:
            try:
                await task
                executed_nodes.add(node_id)
            except Exception as e:
                self.logger.error(
                    "Parallel node execution failed",
                    node_id=node_id,
                    error=str(e)
                )
                # Continue with other nodes unless this is a critical failure
                executed_nodes.add(node_id)  # Mark as executed even if failed
    
    def _are_dependencies_met(
        self,
        context: ExecutionContext,
        node_id: int,
        executed_nodes: Set[int]
    ) -> bool:
        """Check if all dependencies for a node are met."""
        incoming = context.get_incoming_connections(node_id)
        for connection in incoming:
            if connection.source_node_id not in executed_nodes:
                source_node = context.get_node(connection.source_node_id)
                if source_node and not source_node.disabled:
                    return False
        return True
    
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
        
        # Decrypt credentials if present
        decrypted_credentials = None
        if node.credentials:
            from budflow.credentials.encryption import decrypt_credential
            try:
                decrypted_credentials = decrypt_credential(node.credentials)
            except Exception as e:
                self.logger.error(f"Failed to decrypt credentials for node {node.id}: {str(e)}")
                raise WorkflowExecutionError(f"Credential decryption failed for node {node.id}")
        
        # Create node execution context
        node_context = context.create_node_execution_context(
            node=node,
            node_execution=node_execution,
            input_data=input_data,
            credentials=decrypted_credentials
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
    
    async def execute_partial_workflow(
        self,
        workflow_id: int,
        start_node_id: int,
        previous_node_data: Dict[str, List[Dict[str, Any]]],
        mode: ExecutionMode = ExecutionMode.MANUAL,
        run_data_override: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        user_id: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> WorkflowExecution:
        """
        Execute workflow starting from a specific node (partial execution).
        
        Args:
            workflow_id: ID of the workflow to execute
            start_node_id: ID of the node to start execution from
            previous_node_data: Output data from nodes that executed before the start node
            mode: Execution mode
            run_data_override: Optional data to override specific node outputs
            user_id: User ID initiating the execution
            timeout_seconds: Optional timeout for the execution
            
        Returns:
            WorkflowExecution record
        """
        self.logger.info(
            "Starting partial workflow execution",
            workflow_id=workflow_id,
            start_node_id=start_node_id,
            mode=mode.value
        )
        
        # Load workflow
        workflow = await self._load_workflow(workflow_id)
        if not workflow:
            raise WorkflowExecutionError(
                f"Workflow {workflow_id} not found",
                workflow_id=workflow_id
            )
        
        # Validate start node exists
        start_node = next((n for n in workflow.workflow_nodes if n.id == start_node_id), None)
        if not start_node:
            raise InvalidStartNodeError(
                f"Node {start_node_id} not found in workflow {workflow_id}",
                node_id=start_node_id,
                workflow_id=workflow_id
            )
        
        # Build partial execution graph using directed graph executor
        partial_execution_plan = await self.directed_graph_executor.create_subgraph_execution_plan(
            workflow=workflow,
            start_nodes=[start_node_id]
        )
        
        # Validate required input data is present
        await self._validate_partial_execution_inputs(
            workflow, start_node_id, previous_node_data, partial_execution_plan
        )
        
        # Create workflow execution record with partial flag
        workflow_execution = await self._create_partial_workflow_execution(
            workflow=workflow,
            mode=mode,
            input_data={"previous_node_data": previous_node_data},
            user_id=user_id,
            execution_type="partial",
            start_node=start_node.name
        )
        
        # Create execution context
        execution_data = ExecutionData(
            execution_id=workflow_execution.uuid,
            input_data=previous_node_data,
            started_at=datetime.now(timezone.utc)
        )
        
        # Pre-populate execution data with previous node outputs
        for node_name, data in previous_node_data.items():
            # Find node by name to get ID
            node = next((n for n in workflow.workflow_nodes if n.name == node_name), None)
            if node:
                execution_data.set_node_output(str(node.id), data, "main")
        
        # Apply run data overrides if provided
        if run_data_override:
            for node_name, data in run_data_override.items():
                node = next((n for n in workflow.workflow_nodes if n.name == node_name), None)
                if node:
                    execution_data.set_node_output(str(node.id), data, "main")
        
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
            
            # Mark nodes before start node as already executed (historical)
            nodes_before_start = await self._get_nodes_before_start(
                workflow, start_node_id, partial_execution_plan
            )
            
            for node_id in nodes_before_start:
                context.mark_node_executed(node_id, success=True)
                
                # Create historical node execution records
                node = next(n for n in workflow.workflow_nodes if n.id == node_id)
                await self._save_node_execution(
                    context=context,
                    node=node,
                    is_historical=True,
                    data=previous_node_data.get(node.name, [])
                )
            
            # Execute partial workflow
            workflow_execution.status = ExecutionStatus.RUNNING
            workflow_execution.started_at = datetime.now(timezone.utc)
            await self.db_session.commit()
            
            if timeout_seconds:
                await asyncio.wait_for(
                    self._execute_partial_workflow_internal(context, start_node_id, partial_execution_plan),
                    timeout=timeout_seconds
                )
            else:
                await self._execute_partial_workflow_internal(context, start_node_id, partial_execution_plan)
            
            # Mark as successful
            context.status = ExecutionStatus.SUCCESS
            workflow_execution.status = ExecutionStatus.SUCCESS
            
        except Exception as e:
            self.logger.error(
                "Partial workflow execution failed",
                workflow_id=workflow_id,
                start_node_id=start_node_id,
                execution_id=workflow_execution.uuid,
                error=str(e)
            )
            
            context.error = e
            context.status = ExecutionStatus.ERROR
            workflow_execution.status = ExecutionStatus.ERROR
            raise
        
        finally:
            # Save final state
            await context.save_execution_state()
            await self.db_session.commit()
            
            self.logger.info(
                "Partial workflow execution completed",
                workflow_id=workflow_id,
                start_node_id=start_node_id,
                execution_id=workflow_execution.uuid,
                status=context.status.value
            )
        
        return workflow_execution
    
    async def _build_partial_execution_graph(
        self,
        workflow: Workflow,
        start_node_id: int
    ) -> nx.DiGraph:
        """Build execution graph starting from specific node."""
        graph = nx.DiGraph()
        
        # Add all nodes
        for node in workflow.workflow_nodes:
            graph.add_node(node.id, data=node)
        
        # Add all connections
        for conn in workflow.workflow_connections:
            graph.add_edge(
                conn.source_node_id,
                conn.target_node_id,
                data=conn
            )
        
        # Find all nodes reachable from start node
        reachable_nodes = nx.descendants(graph, start_node_id)
        reachable_nodes.add(start_node_id)
        
        # Create subgraph with only reachable nodes
        partial_graph = graph.subgraph(reachable_nodes).copy()
        
        # Check for cycles in partial graph
        if not nx.is_directed_acyclic_graph(partial_graph):
            cycles = list(nx.simple_cycles(partial_graph))
            if cycles:
                raise CircularDependencyError(
                    f"Circular dependency detected in partial execution graph",
                    cycle_path=cycles[0]
                )
        
        return partial_graph
    
    async def _validate_partial_execution_inputs(
        self,
        workflow: Workflow,
        start_node_id: int,
        previous_node_data: Dict[str, List[Dict[str, Any]]],
        partial_execution_plan: Dict[str, Any]
    ) -> None:
        """Validate that required input data is present for partial execution."""
        # Build full graph to check dependencies
        full_graph = await self.directed_graph_executor.build_execution_graph(workflow)
        
        # Get immediate predecessors of start node
        predecessors = list(full_graph.predecessors(start_node_id))
        
        if predecessors:
            # Map node IDs to names
            node_id_to_name = {n.id: n.name for n in workflow.workflow_nodes}
            
            # Check if we have data for required predecessors
            missing_inputs = []
            for pred_id in predecessors:
                pred_name = node_id_to_name.get(pred_id, f"Node_{pred_id}")
                if pred_name not in previous_node_data:
                    missing_inputs.append(pred_name)
            
            if missing_inputs:
                raise WorkflowExecutionError(
                    f"Missing required input data from nodes: {', '.join(missing_inputs)}",
                    workflow_id=workflow.id,
                    error_code="MISSING_INPUT_DATA"
                )
    
    async def _get_nodes_before_start(
        self,
        workflow: Workflow,
        start_node_id: int,
        partial_execution_plan: Dict[str, Any]
    ) -> Set[int]:
        """Get all nodes that execute before the start node."""
        # Build full workflow graph
        full_graph = await self.directed_graph_executor.build_execution_graph(workflow)
        
        # Find all ancestors of start node
        ancestors = nx.ancestors(full_graph, start_node_id)
        
        return ancestors
    
    async def _save_node_execution(
        self,
        context: ExecutionContext,
        node: WorkflowNode,
        is_historical: bool = False,
        data: Optional[List[Dict[str, Any]]] = None
    ) -> NodeExecution:
        """Save node execution record."""
        # For historical nodes (executed before partial execution start),
        # we create a record showing they were already completed
        node_execution = NodeExecution(
            workflow_execution_id=context.workflow_execution.id,
            node_id=node.id,
            status=NodeExecutionStatus.SUCCESS if is_historical else NodeExecutionStatus.NEW,
            started_at=datetime.now(timezone.utc) if not is_historical else None,
            finished_at=datetime.now(timezone.utc) if is_historical else None,
            output_data={"items": data or [], "count": len(data or [])} if is_historical else None,
            duration_ms=0 if is_historical else None
        )
        
        self.db_session.add(node_execution)
        await self.db_session.commit()
        await self.db_session.refresh(node_execution)
        
        return node_execution
    
    async def _execute_partial_workflow_internal(
        self,
        context: ExecutionContext,
        start_node_id: int,
        partial_execution_plan: Dict[str, Any]
    ) -> None:
        """Execute partial workflow starting from specific node using optimized plan."""
        # Get the execution order from the plan
        execution_order = partial_execution_plan.get("execution_order", [])
        parallel_groups = partial_execution_plan.get("parallel_groups", [])
        
        # Track executed nodes
        executed_nodes: Set[int] = set()
        
        # Execute nodes according to the partial execution plan
        if parallel_groups:
            # Use parallel group execution
            for group in parallel_groups:
                # Check for cancellation
                context.check_cancelled()
                
                if len(group) == 1:
                    # Single node
                    node_id = group[0]
                    if node_id not in executed_nodes:
                        await self._execute_partial_node(context, node_id, executed_nodes)
                        executed_nodes.add(node_id)
                else:
                    # Parallel group
                    await self._execute_partial_parallel_group(context, group, executed_nodes)
        else:
            # Fall back to sequential execution based on order
            for node_id in execution_order:
                context.check_cancelled()
                if node_id not in executed_nodes:
                    await self._execute_partial_node(context, node_id, executed_nodes)
                    executed_nodes.add(node_id)
    
    async def _execute_partial_node(
        self,
        context: ExecutionContext,
        node_id: int,
        executed_nodes: Set[int]
    ) -> None:
        """Execute a single node in partial workflow execution."""
        # Get node
        node = next((n for n in context.workflow.workflow_nodes if n.id == node_id), None)
        if not node or node.disabled:
            return
        
        # Check if node has override data
        if context.execution_data.has_node_output(str(node_id)):
            # Node has override data, mark as executed
            context.mark_node_executed(node_id, success=True)
            self.logger.info(
                f"Skipping node {node.name} - using override data"
            )
        else:
            # Execute node normally
            await self._execute_single_node_with_dependencies(context, node, executed_nodes)
    
    async def _execute_partial_parallel_group(
        self,
        context: ExecutionContext,
        group: List[int],
        executed_nodes: Set[int]
    ) -> None:
        """Execute a group of nodes in parallel during partial execution."""
        # Filter nodes that need execution
        nodes_to_execute = []
        for node_id in group:
            if node_id not in executed_nodes:
                node = next((n for n in context.workflow.workflow_nodes if n.id == node_id), None)
                if node and not node.disabled:
                    nodes_to_execute.append(node)
        
        if not nodes_to_execute:
            return
        
        # Execute nodes in parallel
        tasks = []
        for node in nodes_to_execute:
            task = asyncio.create_task(
                self._execute_partial_node(context, node.id, executed_nodes)
            )
            tasks.append((node.id, task))
        
        # Wait for completion
        for node_id, task in tasks:
            try:
                await task
                executed_nodes.add(node_id)
            except Exception as e:
                self.logger.error(
                    "Partial parallel execution failed",
                    node_id=node_id,
                    error=str(e)
                )
                executed_nodes.add(node_id)  # Mark as executed even if failed
    
    async def _create_partial_workflow_execution(
        self,
        workflow: Workflow,
        mode: ExecutionMode,
        input_data: Optional[Dict[str, Any]],
        user_id: Optional[int],
        parent_execution: Optional[WorkflowExecution] = None,
        execution_depth: int = 0,
        execution_stack: Optional[List[int]] = None,
        execution_type: str = "full",
        start_node: Optional[str] = None
    ) -> WorkflowExecution:
        """Create workflow execution record with partial execution support."""
        workflow_execution = WorkflowExecution(
            workflow_id=workflow.id,
            user_id=user_id,
            mode=mode,
            status=ExecutionStatus.NEW,
            data=input_data,
            parent_execution_id=parent_execution.id if parent_execution else None,
            execution_depth=execution_depth,
            execution_stack=execution_stack or [],
            execution_type=execution_type,
            start_node=start_node
        )
        
        self.db_session.add(workflow_execution)
        await self.db_session.commit()
        await self.db_session.refresh(workflow_execution)
        
        return workflow_execution