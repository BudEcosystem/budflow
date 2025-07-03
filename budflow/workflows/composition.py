"""Workflow composition and dependency management."""

from typing import Dict, List, Set, Optional, Any
import asyncio
from collections import defaultdict, deque

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.workflows.models import Workflow, WorkflowExecution
from budflow.executor.errors import CircularWorkflowError, WorkflowValidationError

logger = structlog.get_logger()


class WorkflowCompositionService:
    """Service for managing workflow composition and dependencies."""
    
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.logger = logger.bind(component="workflow_composition")
    
    async def validate_workflow_composition(
        self,
        workflow_id: int,
        max_depth: int = 10
    ) -> Dict[str, Any]:
        """Validate workflow composition for circular dependencies and depth limits."""
        visited = set()
        stack = []
        dependencies = defaultdict(set)
        
        async def check_workflow(wf_id: int, depth: int = 0):
            if wf_id in stack:
                # Circular dependency detected
                cycle_path = stack[stack.index(wf_id):] + [wf_id]
                raise CircularWorkflowError(
                    f"Circular workflow dependency detected: {' -> '.join(map(str, cycle_path))}",
                    workflow_path=cycle_path
                )
            
            if depth > max_depth:
                raise WorkflowValidationError(
                    f"Maximum workflow nesting depth ({max_depth}) exceeded at workflow {wf_id}",
                    validation_errors=[f"Depth limit exceeded at level {depth}"]
                )
            
            if wf_id in visited:
                return
            
            visited.add(wf_id)
            stack.append(wf_id)
            
            # Load workflow
            workflow = await self._get_workflow(wf_id)
            if not workflow:
                stack.pop()
                return
            
            # Check all SubWorkflow nodes
            for node in workflow.nodes or []:
                if node.get("type") == "subworkflow":
                    sub_wf_id = node.get("parameters", {}).get("workflowId")
                    if sub_wf_id:
                        try:
                            sub_id = int(sub_wf_id)
                            dependencies[wf_id].add(sub_id)
                            await check_workflow(sub_id, depth + 1)
                        except (ValueError, TypeError):
                            self.logger.warning(f"Invalid workflow ID in subworkflow node: {sub_wf_id}")
            
            stack.pop()
        
        try:
            await check_workflow(workflow_id)
            
            return {
                "valid": True,
                "workflow_id": workflow_id,
                "total_workflows": len(visited),
                "max_depth": self._calculate_max_depth(dependencies, workflow_id),
                "dependencies": dict(dependencies)
            }
        except (CircularWorkflowError, WorkflowValidationError) as e:
            return {
                "valid": False,
                "workflow_id": workflow_id,
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    async def get_workflow_dependencies(
        self,
        workflow_id: int,
        recursive: bool = True
    ) -> List[Dict[str, Any]]:
        """Get all workflows that this workflow depends on."""
        dependencies = []
        visited = set()
        
        async def collect_dependencies(wf_id: int):
            if wf_id in visited:
                return
            visited.add(wf_id)
            
            workflow = await self._get_workflow(wf_id)
            if not workflow:
                return
            
            for node in workflow.nodes or []:
                if node.get("type") == "subworkflow":
                    sub_wf_id = node.get("parameters", {}).get("workflowId")
                    if sub_wf_id:
                        try:
                            sub_id = int(sub_wf_id)
                            sub_workflow = await self._get_workflow(sub_id)
                            if sub_workflow:
                                dependencies.append({
                                    "id": sub_workflow.id,
                                    "name": sub_workflow.name,
                                    "status": sub_workflow.status.value,
                                    "node_id": node.get("id"),
                                    "node_name": node.get("name")
                                })
                                
                                if recursive:
                                    await collect_dependencies(sub_id)
                        except (ValueError, TypeError):
                            pass
        
        await collect_dependencies(workflow_id)
        return dependencies
    
    async def get_workflow_dependents(
        self,
        workflow_id: int
    ) -> List[Dict[str, Any]]:
        """Get all workflows that depend on this workflow."""
        # Query all workflows
        result = await self.db_session.execute(select(Workflow))
        all_workflows = result.scalars().all()
        
        dependents = []
        
        for workflow in all_workflows:
            if workflow.nodes:
                for node in workflow.nodes:
                    if node.get("type") == "subworkflow":
                        sub_wf_id = node.get("parameters", {}).get("workflowId")
                        if sub_wf_id and str(sub_wf_id) == str(workflow_id):
                            dependents.append({
                                "id": workflow.id,
                                "name": workflow.name,
                                "status": workflow.status.value,
                                "node_id": node.get("id"),
                                "node_name": node.get("name")
                            })
                            break
        
        return dependents
    
    async def get_workflow_composition_graph(
        self,
        workflow_id: int
    ) -> Dict[str, Any]:
        """Get the complete workflow composition graph."""
        nodes = []
        edges = []
        visited = set()
        
        async def build_graph(wf_id: int, parent_id: Optional[str] = None):
            if wf_id in visited:
                return
            visited.add(wf_id)
            
            workflow = await self._get_workflow(wf_id)
            if not workflow:
                return
            
            # Add workflow node
            node_id = f"workflow_{wf_id}"
            nodes.append({
                "id": node_id,
                "type": "workflow",
                "data": {
                    "id": workflow.id,
                    "name": workflow.name,
                    "status": workflow.status.value
                }
            })
            
            # Add edge from parent if exists
            if parent_id:
                edges.append({
                    "source": parent_id,
                    "target": node_id
                })
            
            # Process sub-workflows
            for node in workflow.nodes or []:
                if node.get("type") == "subworkflow":
                    sub_wf_id = node.get("parameters", {}).get("workflowId")
                    if sub_wf_id:
                        try:
                            sub_id = int(sub_wf_id)
                            await build_graph(sub_id, node_id)
                        except (ValueError, TypeError):
                            pass
        
        await build_graph(workflow_id)
        
        return {
            "nodes": nodes,
            "edges": edges,
            "stats": {
                "total_workflows": len(nodes),
                "total_connections": len(edges)
            }
        }
    
    async def analyze_workflow_composition(
        self,
        workflow_id: int
    ) -> Dict[str, Any]:
        """Analyze workflow composition for optimization opportunities."""
        dependencies = defaultdict(set)
        node_count = defaultdict(int)
        execution_paths = []
        
        # Build dependency graph
        graph = await self.get_workflow_composition_graph(workflow_id)
        
        # Analyze critical path
        critical_path = self._find_critical_path(graph)
        
        # Find parallelization opportunities
        parallel_groups = self._find_parallel_groups(graph)
        
        return {
            "workflow_id": workflow_id,
            "total_nodes": sum(node_count.values()),
            "total_workflows": len(graph["nodes"]),
            "max_depth": len(critical_path) if critical_path else 0,
            "critical_path": critical_path,
            "parallelization_opportunities": len(parallel_groups),
            "parallel_groups": parallel_groups
        }
    
    async def get_child_executions(
        self,
        parent_execution_id: int
    ) -> List[WorkflowExecution]:
        """Get all child executions of a parent execution."""
        result = await self.db_session.execute(
            select(WorkflowExecution)
            .where(WorkflowExecution.parent_execution_id == parent_execution_id)
            .order_by(WorkflowExecution.created_at)
        )
        return result.scalars().all()
    
    async def _get_workflow(self, workflow_id: int) -> Optional[Workflow]:
        """Get workflow by ID."""
        result = await self.db_session.execute(
            select(Workflow).where(Workflow.id == workflow_id)
        )
        return result.scalar_one_or_none()
    
    def _calculate_max_depth(
        self,
        dependencies: Dict[int, Set[int]],
        start_id: int
    ) -> int:
        """Calculate maximum depth of workflow composition."""
        memo = {}
        
        def dfs(wf_id: int) -> int:
            if wf_id in memo:
                return memo[wf_id]
            
            if wf_id not in dependencies or not dependencies[wf_id]:
                memo[wf_id] = 0
                return 0
            
            max_child_depth = 0
            for child_id in dependencies[wf_id]:
                child_depth = dfs(child_id)
                max_child_depth = max(max_child_depth, child_depth)
            
            memo[wf_id] = max_child_depth + 1
            return memo[wf_id]
        
        return dfs(start_id)
    
    def _find_critical_path(self, graph: Dict[str, Any]) -> List[str]:
        """Find the critical path in the workflow graph."""
        # Build adjacency list
        adj = defaultdict(list)
        for edge in graph["edges"]:
            adj[edge["source"]].append(edge["target"])
        
        # Find longest path (critical path)
        longest_path = []
        
        def dfs(node: str, path: List[str]):
            nonlocal longest_path
            path.append(node)
            
            if node not in adj:
                if len(path) > len(longest_path):
                    longest_path = path.copy()
            else:
                for neighbor in adj[node]:
                    dfs(neighbor, path)
            
            path.pop()
        
        # Start from all root nodes
        root_nodes = set(n["id"] for n in graph["nodes"])
        for edge in graph["edges"]:
            root_nodes.discard(edge["target"])
        
        for root in root_nodes:
            dfs(root, [])
        
        return longest_path
    
    def _find_parallel_groups(self, graph: Dict[str, Any]) -> List[List[str]]:
        """Find groups of workflows that can be executed in parallel."""
        # Build adjacency lists
        adj = defaultdict(list)
        in_degree = defaultdict(int)
        
        for edge in graph["edges"]:
            adj[edge["source"]].append(edge["target"])
            in_degree[edge["target"]] += 1
        
        # Topological sort with level tracking
        levels = []
        queue = deque()
        
        # Find all nodes with no incoming edges
        all_nodes = set(n["id"] for n in graph["nodes"])
        for node in all_nodes:
            if in_degree[node] == 0:
                queue.append(node)
        
        while queue:
            level = []
            next_queue = deque()
            
            while queue:
                node = queue.popleft()
                level.append(node)
                
                for neighbor in adj[node]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_queue.append(neighbor)
            
            if level:
                levels.append(level)
            queue = next_queue
        
        # Filter out levels with single nodes
        return [level for level in levels if len(level) > 1]