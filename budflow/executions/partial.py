"""Partial execution engine for BudFlow workflows."""

import asyncio
from typing import Dict, Any, List, Set, Optional, Tuple
from uuid import uuid4
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..workflows.models import Workflow, WorkflowExecution, ExecutionStatus, ExecutionMode
from ..workflows.service import WorkflowService
from ..nodes.registry import NodeRegistry


class PartialExecutionError(Exception):
    """Base exception for partial execution errors."""
    pass


class NodeNotFoundError(PartialExecutionError):
    """Raised when start node is not found in workflow."""
    pass


class CircularDependencyError(PartialExecutionError):
    """Raised when circular dependency is detected in workflow."""
    pass


class MaxExecutionDepthError(PartialExecutionError):
    """Raised when maximum execution depth is exceeded."""
    pass


class DependencyGraph:
    """Represents workflow node dependencies."""
    
    def __init__(self):
        self.forward_dependencies: Dict[str, List[str]] = {}
        self.reverse_dependencies: Dict[str, List[str]] = {}
    
    def add_node(self, node_id: str):
        """Add a node to the graph."""
        if node_id not in self.forward_dependencies:
            self.forward_dependencies[node_id] = []
        if node_id not in self.reverse_dependencies:
            self.reverse_dependencies[node_id] = []
    
    def add_dependency(self, source: str, target: str):
        """Add a dependency from source to target node."""
        self.add_node(source)
        self.add_node(target)
        
        if target not in self.forward_dependencies[source]:
            self.forward_dependencies[source].append(target)
        if source not in self.reverse_dependencies[target]:
            self.reverse_dependencies[target].append(source)
    
    def get_downstream_nodes(self, node_id: str) -> Set[str]:
        """Get all downstream nodes from given node."""
        visited = set()
        queue = [node_id]
        
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
                
            visited.add(current)
            
            # Add downstream nodes to queue
            for downstream in self.forward_dependencies.get(current, []):
                if downstream not in visited:
                    queue.append(downstream)
        
        return visited
    
    def detect_cycles(self) -> bool:
        """Detect if there are cycles in the dependency graph."""
        visited = set()
        rec_stack = set()
        
        def has_cycle(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in self.forward_dependencies.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        for node in self.forward_dependencies:
            if node not in visited:
                if has_cycle(node):
                    return True
        
        return False


class PartialExecutionEngine:
    """Engine for executing workflows starting from specific nodes."""
    
    def __init__(self, db: AsyncSession, node_registry: Optional[NodeRegistry] = None):
        self.db = db
        # Use provided node_registry or create a default one for testing
        if node_registry is None:
            node_registry = NodeRegistry()
        self.workflow_service = WorkflowService(db, node_registry)
    
    async def execute_partial(
        self,
        workflow_id: str,
        start_node: str,
        input_data: Optional[Dict[str, Any]] = None,
        previous_execution_id: Optional[str] = None,
        execution_mode: ExecutionMode = ExecutionMode.MANUAL
    ) -> WorkflowExecution:
        """
        Execute workflow starting from a specific node.
        
        Args:
            workflow_id: ID of the workflow to execute
            start_node: Name of the node to start execution from
            input_data: Input data for the start node
            previous_execution_id: ID of previous execution to get data from
            execution_mode: Mode for execution
            
        Returns:
            WorkflowExecution object with results
        """
        
        # Get and validate workflow
        workflow = await self.workflow_service.get_workflow(int(workflow_id))
        if not workflow:
            raise PartialExecutionError(f"Workflow {workflow_id} not found")
        
        # Validate start node exists
        await self.validate_start_node(workflow_id, start_node)
        
        # Build dependency graph
        dependency_graph = self.build_dependency_graph(workflow)
        
        # Check for circular dependencies
        if dependency_graph.detect_cycles():
            raise CircularDependencyError("Circular dependency detected in workflow")
        
        # Determine nodes to execute
        nodes_to_execute = self.get_nodes_to_execute(dependency_graph, start_node)
        
        # Prepare execution data
        execution_data = await self.prepare_execution_data(
            workflow_id, start_node, input_data, previous_execution_id
        )
        
        # Create execution record
        execution = WorkflowExecution(
            workflow_id=int(workflow_id),
            mode=execution_mode,
            start_node=start_node,
            execution_type="partial",
            status=ExecutionStatus.NEW,
            data=execution_data,
            started_at=datetime.now(timezone.utc)
        )
        
        self.db.add(execution)
        await self.db.commit()
        await self.db.refresh(execution)
        
        # Execute filtered workflow
        try:
            execution.status = ExecutionStatus.RUNNING
            await self.db.commit()
            
            # For now, simulate execution by returning the filtered nodes
            # In a real implementation, this would use the execution engine
            result = await self.simulate_node_execution(
                workflow=workflow,
                nodes_to_execute=nodes_to_execute,
                execution_data=execution_data,
                start_node=start_node
            )
            
            # Update execution with results
            execution.status = ExecutionStatus.SUCCESS
            execution.finished_at = datetime.now(timezone.utc)
            execution.data = result
            
            await self.db.commit()
            
            return execution
            
        except Exception as e:
            # Handle execution failure
            execution.status = ExecutionStatus.ERROR
            execution.finished_at = datetime.now(timezone.utc)
            execution.error = {"message": str(e), "type": type(e).__name__}
            
            await self.db.commit()
            raise PartialExecutionError(f"Partial execution failed: {e}") from e
    
    async def validate_start_node(self, workflow_id: str, start_node: str):
        """Validate that start node exists in workflow."""
        workflow = await self.workflow_service.get_workflow(int(workflow_id))
        if not workflow:
            raise PartialExecutionError(f"Workflow {workflow_id} not found")
        
        # Check if start node exists in workflow nodes (check both id and name)
        node_ids = [node.get("id", node.get("name")) for node in workflow.nodes]
        node_names = [node.get("name", node.get("id")) for node in workflow.nodes]
        if start_node not in node_ids and start_node not in node_names:
            raise NodeNotFoundError(f"Start node '{start_node}' not found in workflow")
    
    def build_dependency_graph(self, workflow: Workflow) -> DependencyGraph:
        """Build dependency graph from workflow definition."""
        graph = DependencyGraph()
        
        # Add all nodes using id (since connections reference by id)
        for node in workflow.nodes:
            node_id = node.get("id", node.get("name"))
            graph.add_node(node_id)
        
        # Add connections as dependencies
        for connection in workflow.connections:
            source = connection.get("source")
            target = connection.get("target")
            
            if source and target:
                graph.add_dependency(source, target)
        
        return graph
    
    def get_nodes_to_execute(self, graph: DependencyGraph, start_node: str) -> Set[str]:
        """Get all nodes that need to be executed starting from start_node."""
        return graph.get_downstream_nodes(start_node)
    
    async def prepare_execution_data(
        self,
        workflow_id: str,
        start_node: str,
        input_data: Optional[Dict[str, Any]],
        previous_execution_id: Optional[str]
    ) -> Dict[str, Any]:
        """Prepare input data for partial execution."""
        execution_data = {}
        
        if input_data is not None:
            # Use provided input data
            execution_data[start_node] = input_data
        elif previous_execution_id:
            # Get data from previous execution
            previous_data = await self.get_execution_results(previous_execution_id)
            execution_data.update(previous_data)
        else:
            # Create empty input for start node
            execution_data[start_node] = []
        
        return execution_data
    
    async def get_execution_results(self, execution_id: str) -> Dict[str, Any]:
        """Get results from a previous execution."""
        # Query execution results from database
        result = await self.db.execute(
            select(WorkflowExecution).where(WorkflowExecution.id == execution_id)
        )
        execution = result.scalar_one_or_none()
        
        if not execution:
            raise PartialExecutionError(f"Previous execution {execution_id} not found")
        
        # Return execution data
        return execution.data or {}
    
    async def extract_relevant_data(
        self, 
        previous_results: Dict[str, Any], 
        start_node: str
    ) -> Dict[str, Any]:
        """Extract data relevant to the start node from previous results."""
        # For now, return all previous data
        # In a more sophisticated implementation, we would analyze dependencies
        # and only include data that the start_node actually needs
        return previous_results
    
    async def simulate_node_execution(
        self,
        workflow: Workflow,
        nodes_to_execute: Set[str],
        execution_data: Dict[str, Any],
        start_node: str
    ) -> Dict[str, Any]:
        """
        Simulate node execution for testing purposes.
        In a real implementation, this would call the actual execution engine.
        """
        result = {}
        
        # Simulate execution of each node in the filtered set
        for node_name in nodes_to_execute:
            # Find the node definition
            node_def = None
            for node in workflow.nodes:
                if node.get("name", node.get("id")) == node_name:
                    node_def = node
                    break
            
            if node_def:
                # Simulate node output based on type
                node_type = node_def.get("type", "unknown")
                result[node_name] = [
                    {
                        "simulated": True,
                        "node_type": node_type,
                        "executed_at": datetime.now(timezone.utc).isoformat(),
                        "input_data": execution_data.get(node_name, {}),
                        "output": f"Simulated output from {node_name}"
                    }
                ]
        
        return result