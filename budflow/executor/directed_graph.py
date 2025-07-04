"""
Directed graph execution algorithm for optimal workflow execution order.

This module implements N8N-compatible directed graph execution with topological
sorting, dependency resolution, and execution optimization based on workflow structure.
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from collections import defaultdict, deque

import structlog
import networkx as nx
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.workflows.models import (
    Workflow, WorkflowNode, WorkflowConnection,
    WorkflowExecution, NodeExecution,
    WorkflowStatus, ExecutionStatus, NodeExecutionStatus,
    ExecutionMode, NodeType
)
from .errors import (
    WorkflowExecutionError,
    CircularDependencyError,
    InvalidWorkflowStructureError,
)

logger = structlog.get_logger()


class ExecutionPlan:
    """Represents an optimized execution plan for a workflow."""
    
    def __init__(
        self,
        execution_order: List[int],
        parallel_groups: List[List[int]],
        critical_path: List[int],
        levels: Dict[int, int],
        bottlenecks: List[Dict[str, Any]] = None,
        total_weight: float = 0.0
    ):
        self.execution_order = execution_order
        self.parallel_groups = parallel_groups
        self.critical_path = critical_path
        self.levels = levels
        self.bottlenecks = bottlenecks or []
        self.total_weight = total_weight
        self.created_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert execution plan to dictionary."""
        return {
            "execution_order": self.execution_order,
            "parallel_groups": self.parallel_groups,
            "critical_path": self.critical_path,
            "levels": self.levels,
            "bottlenecks": self.bottlenecks,
            "total_weight": self.total_weight,
            "created_at": self.created_at.isoformat()
        }


class DirectedGraphExecutor:
    """
    Directed graph execution engine for optimal workflow scheduling.
    
    This class implements N8N-compatible directed graph execution algorithms
    including topological sorting, critical path analysis, and parallel execution
    optimization.
    """
    
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.logger = logger.bind(component="directed_graph_executor")
    
    async def build_execution_graph(self, workflow: Workflow) -> nx.DiGraph:
        """
        Build directed graph from workflow structure.
        
        Args:
            workflow: Workflow to build graph from
            
        Returns:
            NetworkX directed graph representing the workflow
        """
        graph = nx.DiGraph()
        
        # Add nodes with metadata (avoid storing SQLAlchemy objects)
        for node in workflow.workflow_nodes:
            graph.add_node(
                node.id,
                name=node.name,
                type=node.type.value if hasattr(node.type, 'value') else str(node.type),
                position=node.position,
                parameters=node.parameters,
                disabled=getattr(node, 'disabled', False)
            )
        
        # Add edges (connections)
        for conn in workflow.workflow_connections:
            graph.add_edge(
                conn.source_node_id,
                conn.target_node_id,
                source_output=conn.source_output,
                target_input=conn.target_input
            )
        
        return graph
    
    async def get_topological_execution_order(self, graph: nx.DiGraph) -> List[int]:
        """
        Get topological execution order for nodes.
        
        Args:
            graph: Directed graph of workflow
            
        Returns:
            List of node IDs in topological order
            
        Raises:
            CircularDependencyError: If circular dependencies exist
        """
        try:
            # Use Kahn's algorithm for topological sorting
            return list(nx.topological_sort(graph))
        except nx.NetworkXError as e:
            # Detect cycles for better error reporting
            cycles = list(nx.simple_cycles(graph))
            if cycles:
                raise CircularDependencyError(
                    f"Circular dependency detected in workflow",
                    cycle_path=cycles[0]
                )
            raise InvalidWorkflowStructureError(f"Invalid workflow structure: {e}")
    
    async def get_parallel_execution_groups(self, graph: nx.DiGraph) -> List[List[int]]:
        """
        Identify groups of nodes that can execute in parallel.
        
        Args:
            graph: Directed graph of workflow
            
        Returns:
            List of lists, where each inner list contains node IDs that can run in parallel
        """
        levels = await self.calculate_execution_levels(graph)
        
        # Group nodes by execution level
        level_groups = defaultdict(list)
        for node_id, level in levels.items():
            level_groups[level].append(node_id)
        
        # Convert to sorted list of groups
        parallel_groups = []
        for level in sorted(level_groups.keys()):
            nodes_at_level = level_groups[level]
            if len(nodes_at_level) > 1:
                # Multiple nodes at same level can run in parallel
                parallel_groups.append(nodes_at_level)
            else:
                # Single node groups are still valid execution groups
                parallel_groups.append(nodes_at_level)
        
        return parallel_groups
    
    async def calculate_execution_levels(self, graph: nx.DiGraph) -> Dict[int, int]:
        """
        Calculate execution levels for each node.
        
        Execution level represents the minimum depth from start nodes,
        determining when a node can earliest be executed.
        
        Args:
            graph: Directed graph of workflow
            
        Returns:
            Dictionary mapping node ID to execution level
        """
        levels = {}
        
        # Find nodes with no incoming edges (start nodes)
        start_nodes = [node for node in graph.nodes() if graph.in_degree(node) == 0]
        
        # Initialize start nodes at level 0
        for node in start_nodes:
            levels[node] = 0
        
        # Use BFS to calculate levels
        queue = deque(start_nodes)
        visited = set(start_nodes)
        
        while queue:
            current_node = queue.popleft()
            current_level = levels[current_node]
            
            # Process all successors
            for successor in graph.successors(current_node):
                # Calculate minimum level for successor
                predecessor_levels = [
                    levels.get(pred, 0) for pred in graph.predecessors(successor)
                    if pred in levels
                ]
                
                if predecessor_levels:
                    min_level = max(predecessor_levels) + 1
                else:
                    min_level = current_level + 1
                
                # Update level if this path gives a higher level
                if successor not in levels or levels[successor] < min_level:
                    levels[successor] = min_level
                
                # Add to queue if not visited
                if successor not in visited:
                    queue.append(successor)
                    visited.add(successor)
        
        return levels
    
    async def find_critical_path(self, graph: nx.DiGraph, node_weights: Dict[int, float] = None) -> List[int]:
        """
        Find the critical path through the workflow.
        
        The critical path is the longest path from start to end nodes,
        which determines the minimum execution time.
        
        Args:
            graph: Directed graph of workflow
            node_weights: Optional weights for nodes (execution time estimates)
            
        Returns:
            List of node IDs representing the critical path
        """
        if node_weights is None:
            node_weights = {node: 1.0 for node in graph.nodes()}
        
        # Find start and end nodes
        start_nodes = [node for node in graph.nodes() if graph.in_degree(node) == 0]
        end_nodes = [node for node in graph.nodes() if graph.out_degree(node) == 0]
        
        if not start_nodes or not end_nodes:
            return []
        
        # Calculate longest path using dynamic programming
        longest_distances = {}
        predecessors = {}
        
        # Topological sort for processing order
        topo_order = await self.get_topological_execution_order(graph)
        
        # Initialize distances
        for node in graph.nodes():
            longest_distances[node] = float('-inf')
        
        for start in start_nodes:
            longest_distances[start] = node_weights.get(start, 1.0)
        
        # Calculate longest distances
        for node in topo_order:
            for successor in graph.successors(node):
                new_distance = longest_distances[node] + node_weights.get(successor, 1.0)
                if new_distance > longest_distances[successor]:
                    longest_distances[successor] = new_distance
                    predecessors[successor] = node
        
        # Find end node with maximum distance
        max_distance = float('-inf')
        critical_end = None
        for end in end_nodes:
            if longest_distances[end] > max_distance:
                max_distance = longest_distances[end]
                critical_end = end
        
        if critical_end is None:
            return []
        
        # Reconstruct critical path
        critical_path = []
        current = critical_end
        while current is not None:
            critical_path.append(current)
            current = predecessors.get(current)
        
        critical_path.reverse()
        return critical_path
    
    async def get_ready_nodes(self, graph: nx.DiGraph, executed_nodes: Set[int]) -> Set[int]:
        """
        Get nodes that are ready to execute based on current state.
        
        Args:
            graph: Directed graph of workflow
            executed_nodes: Set of already executed node IDs
            
        Returns:
            Set of node IDs ready for execution
        """
        ready_nodes = set()
        
        for node in graph.nodes():
            if node in executed_nodes:
                continue
            
            # Check if all predecessors have been executed
            predecessors = set(graph.predecessors(node))
            if predecessors.issubset(executed_nodes):
                # Also check if node is not disabled
                node_data = graph.nodes[node]
                if not node_data.get('disabled', False):
                    ready_nodes.add(node)
        
        return ready_nodes
    
    async def validate_dependencies(self, graph: nx.DiGraph) -> Dict[str, Any]:
        """
        Validate workflow dependencies and structure.
        
        Args:
            graph: Directed graph of workflow
            
        Returns:
            Dictionary with validation results
        """
        validation_result = {
            "is_valid": True,
            "circular_dependencies": [],
            "disconnected_components": [],
            "unreachable_nodes": [],
            "start_nodes": [],
            "end_nodes": []
        }
        
        # Check for circular dependencies
        try:
            cycles = list(nx.simple_cycles(graph))
            validation_result["circular_dependencies"] = cycles
            if cycles:
                validation_result["is_valid"] = False
        except Exception as e:
            self.logger.error(f"Error detecting cycles: {e}")
            validation_result["is_valid"] = False
        
        # Check for disconnected components
        if not graph.nodes():
            validation_result["is_valid"] = False
        else:
            # Convert to undirected for component analysis
            undirected = graph.to_undirected()
            components = list(nx.connected_components(undirected))
            if len(components) > 1:
                validation_result["disconnected_components"] = [list(comp) for comp in components]
        
        # Find start and end nodes
        start_nodes = [node for node in graph.nodes() if graph.in_degree(node) == 0]
        end_nodes = [node for node in graph.nodes() if graph.out_degree(node) == 0]
        
        validation_result["start_nodes"] = start_nodes
        validation_result["end_nodes"] = end_nodes
        
        # Check for unreachable nodes
        if start_nodes:
            reachable = set()
            for start in start_nodes:
                reachable.update(nx.descendants(graph, start))
                reachable.add(start)
            
            unreachable = set(graph.nodes()) - reachable
            validation_result["unreachable_nodes"] = list(unreachable)
            if unreachable:
                validation_result["is_valid"] = False
        
        return validation_result
    
    async def analyze_execution_paths(self, workflow: Workflow) -> Dict[str, Any]:
        """
        Analyze all execution paths and identify bottlenecks.
        
        Args:
            workflow: Workflow to analyze
            
        Returns:
            Dictionary with path analysis results
        """
        graph = await self.build_execution_graph(workflow)
        
        # Find all simple paths from start to end nodes
        start_nodes = [node for node in graph.nodes() if graph.in_degree(node) == 0]
        end_nodes = [node for node in graph.nodes() if graph.out_degree(node) == 0]
        
        all_paths = []
        for start in start_nodes:
            for end in end_nodes:
                try:
                    paths = list(nx.all_simple_paths(graph, start, end))
                    all_paths.extend(paths)
                except nx.NetworkXNoPath:
                    continue
        
        # Find critical path
        critical_path = await self.find_critical_path(graph)
        
        # Identify bottlenecks (nodes with high in-degree)
        bottlenecks = []
        for node in graph.nodes():
            in_degree = graph.in_degree(node)
            if in_degree > 2:  # Nodes with more than 2 inputs are potential bottlenecks
                bottlenecks.append({
                    "node_id": node,
                    "node_name": graph.nodes[node].get('name', f'Node_{node}'),
                    "in_degree": in_degree,
                    "predecessors": list(graph.predecessors(node))
                })
        
        # Identify parallelizable segments
        parallel_groups = await self.get_parallel_execution_groups(graph)
        parallelizable_segments = [group for group in parallel_groups if len(group) > 1]
        
        return {
            "all_paths": all_paths,
            "critical_path": critical_path,
            "bottlenecks": bottlenecks,
            "parallelizable_segments": parallelizable_segments,
            "total_paths": len(all_paths),
            "max_parallelism": max(len(group) for group in parallel_groups) if parallel_groups else 1
        }
    
    async def create_execution_plan(self, workflow: Workflow) -> ExecutionPlan:
        """
        Create optimized execution plan for workflow.
        
        Args:
            workflow: Workflow to create plan for
            
        Returns:
            ExecutionPlan object with optimized execution strategy
            
        Raises:
            CircularDependencyError: If workflow has circular dependencies
            InvalidWorkflowStructureError: If workflow structure is invalid
        """
        # Build execution graph
        graph = await self.build_execution_graph(workflow)
        
        # Validate workflow structure
        validation = await self.validate_dependencies(graph)
        if not validation["is_valid"]:
            if validation["circular_dependencies"]:
                raise CircularDependencyError(
                    "Workflow contains circular dependencies",
                    cycle_path=validation["circular_dependencies"][0]
                )
            raise InvalidWorkflowStructureError("Invalid workflow structure")
        
        # Calculate execution components
        execution_order = await self.get_topological_execution_order(graph)
        parallel_groups = await self.get_parallel_execution_groups(graph)
        critical_path = await self.find_critical_path(graph)
        levels = await self.calculate_execution_levels(graph)
        
        # Analyze for bottlenecks
        path_analysis = await self.analyze_execution_paths(workflow)
        bottlenecks = path_analysis["bottlenecks"]
        
        return ExecutionPlan(
            execution_order=execution_order,
            parallel_groups=parallel_groups,
            critical_path=critical_path,
            levels=levels,
            bottlenecks=bottlenecks
        )
    
    async def create_subgraph_execution_plan(
        self,
        workflow: Workflow,
        start_nodes: List[int],
        end_nodes: List[int] = None
    ) -> Dict[str, Any]:
        """
        Create execution plan for a subgraph of the workflow.
        
        Args:
            workflow: Complete workflow
            start_nodes: Node IDs to start subgraph from
            end_nodes: Optional node IDs to end subgraph at
            
        Returns:
            Dictionary with subgraph execution plan
        """
        # Build full graph
        full_graph = await self.build_execution_graph(workflow)
        
        # Find all nodes reachable from start nodes
        reachable_nodes = set()
        for start_node in start_nodes:
            reachable_nodes.update(nx.descendants(full_graph, start_node))
            reachable_nodes.add(start_node)
        
        # If end nodes specified, limit to nodes that can reach end nodes
        if end_nodes:
            reverse_graph = full_graph.reverse()
            reachable_from_end = set()
            for end_node in end_nodes:
                reachable_from_end.update(nx.descendants(reverse_graph, end_node))
                reachable_from_end.add(end_node)
            
            # Intersection gives us the subgraph
            included_nodes = reachable_nodes.intersection(reachable_from_end)
        else:
            included_nodes = reachable_nodes
        
        # Create subgraph
        subgraph = full_graph.subgraph(included_nodes).copy()
        
        # Create execution plan for subgraph
        execution_order = await self.get_topological_execution_order(subgraph)
        parallel_groups = await self.get_parallel_execution_groups(subgraph)
        levels = await self.calculate_execution_levels(subgraph)
        
        return {
            "execution_order": execution_order,
            "parallel_groups": parallel_groups,
            "levels": levels,
            "included_nodes": list(included_nodes),
            "start_nodes": start_nodes,
            "end_nodes": end_nodes or []
        }
    
    async def create_weighted_execution_plan(
        self,
        workflow: Workflow,
        node_weights: Dict[int, float]
    ) -> Dict[str, Any]:
        """
        Create execution plan considering node execution weights/costs.
        
        Args:
            workflow: Workflow to plan
            node_weights: Dictionary mapping node ID to execution weight
            
        Returns:
            Dictionary with weighted execution plan
        """
        graph = await self.build_execution_graph(workflow)
        
        # Find critical path with weights
        critical_path = await self.find_critical_path(graph, node_weights)
        
        # Calculate total weight of critical path
        total_weight = sum(node_weights.get(node_id, 1.0) for node_id in critical_path)
        
        # Identify parallelization opportunities
        parallel_groups = await self.get_parallel_execution_groups(graph)
        parallelization_opportunities = []
        
        for group in parallel_groups:
            if len(group) > 1:
                group_weight = sum(node_weights.get(node_id, 1.0) for node_id in group)
                max_node_weight = max(node_weights.get(node_id, 1.0) for node_id in group)
                time_savings = group_weight - max_node_weight
                
                parallelization_opportunities.append({
                    "nodes": group,
                    "total_weight": group_weight,
                    "parallel_weight": max_node_weight,
                    "time_savings": time_savings,
                    "savings_percentage": (time_savings / group_weight) * 100 if group_weight > 0 else 0
                })
        
        return {
            "critical_path": critical_path,
            "total_weight": total_weight,
            "parallelization_opportunities": parallelization_opportunities,
            "weighted_levels": await self.calculate_execution_levels(graph)
        }
    
    async def update_execution_plan(
        self,
        workflow: Workflow,
        runtime_conditions: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update execution plan based on runtime conditions.
        
        Args:
            workflow: Workflow being executed
            runtime_conditions: Current runtime state (failed nodes, completed nodes, etc.)
            
        Returns:
            Updated execution plan
        """
        graph = await self.build_execution_graph(workflow)
        
        failed_nodes = set(runtime_conditions.get("failed_nodes", []))
        delayed_nodes = set(runtime_conditions.get("delayed_nodes", []))
        completed_nodes = set(runtime_conditions.get("completed_nodes", []))
        
        # Remove failed nodes from graph
        graph_copy = graph.copy()
        graph_copy.remove_nodes_from(failed_nodes)
        
        # Find remaining nodes to execute
        all_nodes = set(graph.nodes())
        remaining_nodes = all_nodes - completed_nodes - failed_nodes
        
        # Create subgraph with remaining nodes
        remaining_graph = graph_copy.subgraph(remaining_nodes).copy()
        
        # Find alternative paths avoiding failed nodes
        alternative_paths = []
        start_nodes = [node for node in remaining_graph.nodes() if remaining_graph.in_degree(node) == 0]
        end_nodes = [node for node in remaining_graph.nodes() if remaining_graph.out_degree(node) == 0]
        
        for start in start_nodes:
            for end in end_nodes:
                try:
                    paths = list(nx.all_simple_paths(remaining_graph, start, end))
                    alternative_paths.extend(paths)
                except nx.NetworkXNoPath:
                    continue
        
        # Recovery options
        recovery_options = []
        if failed_nodes:
            for failed_node in failed_nodes:
                # Find nodes that depend on the failed node
                dependent_nodes = list(nx.descendants(graph, failed_node))
                recovery_options.append({
                    "failed_node": failed_node,
                    "affected_nodes": dependent_nodes,
                    "can_skip": len(dependent_nodes) == 0,
                    "alternative_start_points": [
                        node for node in dependent_nodes 
                        if set(graph.predecessors(node)) - {failed_node} <= completed_nodes
                    ]
                })
        
        return {
            "remaining_nodes": list(remaining_nodes),
            "alternative_paths": alternative_paths,
            "recovery_options": recovery_options,
            "updated_execution_order": await self.get_topological_execution_order(remaining_graph) if remaining_graph.nodes() else [],
            "delayed_impact": {
                "delayed_nodes": list(delayed_nodes),
                "affected_downstream": [
                    list(nx.descendants(graph, delayed_node)) 
                    for delayed_node in delayed_nodes
                ]
            }
        }
    
    async def serialize_execution_plan(self, execution_plan: ExecutionPlan) -> str:
        """
        Serialize execution plan to JSON string.
        
        Args:
            execution_plan: ExecutionPlan to serialize
            
        Returns:
            JSON string representation
        """
        return json.dumps(execution_plan.to_dict(), indent=2)
    
    async def deserialize_execution_plan(self, serialized_plan: str) -> Dict[str, Any]:
        """
        Deserialize execution plan from JSON string.
        
        Args:
            serialized_plan: JSON string representation
            
        Returns:
            Dictionary representation of execution plan
        """
        plan_dict = json.loads(serialized_plan)
        
        # Convert string keys back to integers for levels
        if "levels" in plan_dict:
            plan_dict["levels"] = {int(k): v for k, v in plan_dict["levels"].items()}
        
        return plan_dict