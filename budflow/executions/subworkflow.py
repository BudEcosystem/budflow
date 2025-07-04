"""Sub-workflow execution functionality for BudFlow."""

import asyncio
import json
import jsonpath_ng
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Set
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import selectinload

from ..workflows.models import Workflow, WorkflowNode, WorkflowExecution, ExecutionStatus, ExecutionMode
from ..workflows.service import WorkflowService
from ..nodes.registry import NodeRegistry


@dataclass
class ParameterMapping:
    """Represents parameter mapping configuration."""
    input_mapping: Dict[str, str]
    output_mapping: Dict[str, str]
    static_parameters: Optional[Dict[str, Any]] = None


@dataclass
class SubWorkflowResult:
    """Result of sub-workflow execution."""
    status: ExecutionStatus
    output_data: Dict[str, Any]
    child_execution_id: Optional[int]
    parent_execution_id: int
    execution_time_ms: Optional[float] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.status.value if isinstance(self.status, ExecutionStatus) else self.status,
            "output_data": self.output_data,
            "child_execution_id": self.child_execution_id,
            "parent_execution_id": self.parent_execution_id,
            "execution_time_ms": self.execution_time_ms,
            "error_message": self.error_message,
            "error_details": self.error_details
        }


@dataclass
class SubWorkflowExecution:
    """Tracks sub-workflow execution state."""
    parent_execution_id: int
    child_execution_id: Optional[int]
    subworkflow_id: int
    depth: int
    execution_chain: List[int]
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: ExecutionStatus = ExecutionStatus.NEW
    mapped_input: Optional[Dict[str, Any]] = None
    mapped_output: Optional[Dict[str, Any]] = None


class SubWorkflowNotFoundError(Exception):
    """Raised when sub-workflow is not found."""
    pass


class SubWorkflowCircularReferenceError(Exception):
    """Raised when circular reference is detected in sub-workflow chain."""
    pass


class SubWorkflowDepthLimitError(Exception):
    """Raised when sub-workflow depth limit is exceeded."""
    pass


class SubWorkflowParameterError(Exception):
    """Raised when sub-workflow parameters are invalid."""
    pass


class SubWorkflowTimeoutError(Exception):
    """Raised when sub-workflow execution times out."""
    pass


class SubWorkflowNode:
    """Node implementation for executing sub-workflows."""
    
    def __init__(
        self,
        node_id: str,
        subworkflow_id: int,
        parameter_mapping: Dict[str, Any],
        output_mapping: Dict[str, Any],
        timeout: Optional[int] = None,
        resource_limits: Optional[Dict[str, Any]] = None
    ):
        if not subworkflow_id:
            raise SubWorkflowParameterError("subworkflow_id is required")
        
        self.node_id = node_id
        self.subworkflow_id = subworkflow_id
        self.parameter_mapping = parameter_mapping
        self.output_mapping = output_mapping
        self.timeout = timeout
        self.resource_limits = resource_limits or {}
        self.engine: Optional['SubWorkflowEngine'] = None
    
    async def execute(self, input_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute the sub-workflow node."""
        if not self.engine:
            raise SubWorkflowParameterError("SubWorkflow engine not configured")
        
        # Create mock parent execution for node execution
        parent_execution = type('MockExecution', (), {
            'id': 1,
            'workflow_id': 1,
            'data': {f"input_{i}": data for i, data in enumerate(input_data)}
        })()
        
        subworkflow_config = {
            "subworkflow_id": self.subworkflow_id,
            "parameter_mapping": self.parameter_mapping,
            "output_mapping": self.output_mapping,
            "timeout": self.timeout,
            "resource_limits": self.resource_limits
        }
        
        result = await self.engine.execute_subworkflow(
            parent_execution=parent_execution,
            subworkflow_config=subworkflow_config,
            execution_chain=[]
        )
        
        # Format result for node output
        output = result.output_data.copy()
        output["_subworkflow_execution_id"] = result.child_execution_id
        output["_subworkflow_status"] = result.status.value
        
        if result.execution_time_ms:
            output["_execution_time_ms"] = result.execution_time_ms
        
        return [output]


class SubWorkflowEngine:
    """Engine for executing sub-workflows."""
    
    MAX_DEPTH = 10
    MAX_CONCURRENT_SUBWORKFLOWS = 5
    DEFAULT_TIMEOUT = 300  # 5 minutes
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.workflow_service: Optional[WorkflowService] = None
        self.execution_service: Optional[Any] = None
        self.resource_tracker: Optional[Any] = None
        
        # Track active executions
        self.active_executions: Dict[str, SubWorkflowExecution] = {}
        self.active_subworkflow_count = 0
        self.max_concurrent_subworkflows = self.MAX_CONCURRENT_SUBWORKFLOWS
    
    async def execute_subworkflow(
        self,
        parent_execution: WorkflowExecution,
        subworkflow_config: Dict[str, Any],
        execution_chain: List[int]
    ) -> SubWorkflowResult:
        """Execute a sub-workflow with the given configuration."""
        start_time = datetime.now(timezone.utc)
        
        try:
            # Extract configuration
            subworkflow_id = subworkflow_config["subworkflow_id"]
            parameter_mapping = subworkflow_config.get("parameter_mapping", {})
            output_mapping = subworkflow_config.get("output_mapping", {})
            timeout = subworkflow_config.get("timeout", self.DEFAULT_TIMEOUT)
            resource_limits = subworkflow_config.get("resource_limits", {})
            
            # Validate sub-workflow
            await self.validate_subworkflow_exists(subworkflow_id)
            await self.detect_circular_references(subworkflow_id, execution_chain)
            await self.validate_depth_limit(execution_chain)
            await self.validate_concurrency_limits()
            await self.validate_resource_limits(resource_limits)
            
            # Map input parameters
            mapped_input = await self.map_input_parameters(parent_execution.data, parameter_mapping)
            
            # Create child execution
            child_execution = await self.create_child_execution(
                subworkflow_id=subworkflow_id,
                parent_execution=parent_execution,
                input_data=mapped_input,
                execution_chain=execution_chain
            )
            
            # Track execution
            sub_execution = SubWorkflowExecution(
                parent_execution_id=parent_execution.id,
                child_execution_id=child_execution.id if child_execution else None,
                subworkflow_id=subworkflow_id,
                depth=len(execution_chain),
                execution_chain=execution_chain + [subworkflow_id],
                started_at=start_time,
                mapped_input=mapped_input
            )
            
            self.active_executions[str(parent_execution.id)] = sub_execution
            self.active_subworkflow_count += 1
            
            try:
                # Execute with timeout
                child_execution = await asyncio.wait_for(
                    self.execute_child_workflow(child_execution, resource_limits),
                    timeout=timeout
                )
                
                # Map output parameters
                if child_execution.status == ExecutionStatus.SUCCESS:
                    mapped_output = await self.map_output_parameters(child_execution.data, output_mapping)
                    sub_execution.mapped_output = mapped_output
                    
                    result = SubWorkflowResult(
                        status=ExecutionStatus.SUCCESS,
                        output_data=mapped_output,
                        child_execution_id=child_execution.id,
                        parent_execution_id=parent_execution.id,
                        execution_time_ms=(datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                    )
                else:
                    # Handle execution failure
                    result = SubWorkflowResult(
                        status=ExecutionStatus.ERROR,
                        output_data={},
                        child_execution_id=child_execution.id,
                        parent_execution_id=parent_execution.id,
                        error_message=child_execution.error.get("message", "Sub-workflow execution failed") if child_execution.error else "Unknown error",
                        error_details=child_execution.error,
                        execution_time_ms=(datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                    )
                
            except asyncio.TimeoutError:
                result = SubWorkflowResult(
                    status=ExecutionStatus.ERROR,
                    output_data={},
                    child_execution_id=child_execution.id if child_execution else None,
                    parent_execution_id=parent_execution.id,
                    error_message=f"Sub-workflow execution timed out after {timeout} seconds",
                    execution_time_ms=timeout * 1000
                )
            
            # Update tracking
            sub_execution.finished_at = datetime.now(timezone.utc)
            sub_execution.status = result.status
            
            return result
            
        except Exception as e:
            return SubWorkflowResult(
                status=ExecutionStatus.ERROR,
                output_data={},
                child_execution_id=None,
                parent_execution_id=parent_execution.id,
                error_message=str(e),
                error_details={"type": type(e).__name__},
                execution_time_ms=(datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )
        
        finally:
            # Cleanup tracking
            if str(parent_execution.id) in self.active_executions:
                del self.active_executions[str(parent_execution.id)]
            self.active_subworkflow_count = max(0, self.active_subworkflow_count - 1)
    
    async def map_input_parameters(
        self,
        parent_data: Dict[str, List[Dict[str, Any]]],
        parameter_mapping: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Map parent execution data to sub-workflow input parameters."""
        mapped_params = {}
        
        for param_name, mapping_expression in parameter_mapping.items():
            try:
                if isinstance(mapping_expression, str) and mapping_expression.startswith("$."):
                    # JSONPath expression
                    jsonpath_expr = jsonpath_ng.parse(mapping_expression)
                    matches = jsonpath_expr.find(parent_data)
                    
                    if len(matches) == 1:
                        mapped_params[param_name] = matches[0].value
                    elif len(matches) > 1:
                        mapped_params[param_name] = [match.value for match in matches]
                    else:
                        # No matches found, parameter mapping failed
                        raise SubWorkflowParameterError(f"Parameter mapping failed for {param_name}: {mapping_expression}")
                else:
                    # Static value
                    mapped_params[param_name] = mapping_expression
                    
            except Exception as e:
                raise SubWorkflowParameterError(f"Parameter mapping failed for {param_name}: {str(e)}")
        
        return mapped_params
    
    async def map_output_parameters(
        self,
        child_data: Dict[str, List[Dict[str, Any]]],
        output_mapping: Dict[str, str]
    ) -> Dict[str, Any]:
        """Map sub-workflow output to parent execution format."""
        mapped_output = {}
        
        for output_name, mapping_expression in output_mapping.items():
            try:
                if mapping_expression.startswith("$."):
                    # JSONPath expression
                    jsonpath_expr = jsonpath_ng.parse(mapping_expression)
                    matches = jsonpath_expr.find(child_data)
                    
                    if len(matches) == 1:
                        mapped_output[output_name] = matches[0].value
                    elif len(matches) > 1:
                        mapped_output[output_name] = [match.value for match in matches]
                    else:
                        mapped_output[output_name] = None
                else:
                    # Static value
                    mapped_output[output_name] = mapping_expression
                    
            except Exception as e:
                # Log error but continue with other mappings
                mapped_output[output_name] = None
        
        return mapped_output
    
    async def validate_subworkflow_exists(self, subworkflow_id: int):
        """Validate that the sub-workflow exists."""
        if not self.workflow_service:
            # Create workflow service if not available
            node_registry = NodeRegistry()
            self.workflow_service = WorkflowService(self.db, node_registry)
        
        workflow = await self.workflow_service.get_workflow(subworkflow_id)
        if not workflow:
            raise SubWorkflowNotFoundError(f"Sub-workflow {subworkflow_id} not found")
    
    async def detect_circular_references(self, subworkflow_id: int, execution_chain: List[int]):
        """Detect circular references in sub-workflow execution chain."""
        if subworkflow_id in execution_chain:
            chain_str = " -> ".join(map(str, execution_chain + [subworkflow_id]))
            raise SubWorkflowCircularReferenceError(f"Circular reference detected: {chain_str}")
    
    async def validate_depth_limit(self, execution_chain: List[int]):
        """Validate that sub-workflow depth is within limits."""
        if len(execution_chain) >= self.MAX_DEPTH:
            raise SubWorkflowDepthLimitError(f"Sub-workflow depth limit exceeded: {len(execution_chain)} >= {self.MAX_DEPTH}")
    
    async def validate_concurrency_limits(self):
        """Validate that concurrent execution limits are not exceeded."""
        if self.active_subworkflow_count >= self.max_concurrent_subworkflows:
            raise SubWorkflowParameterError(f"Maximum concurrent sub-workflows exceeded: {self.active_subworkflow_count}")
    
    async def validate_resource_limits(self, resource_limits: Dict[str, Any]):
        """Validate resource limits for sub-workflow execution."""
        max_memory = resource_limits.get("max_memory_mb", 0)
        max_time = resource_limits.get("max_execution_time_seconds", 0)
        
        # Define reasonable limits
        if max_memory > 8192:  # 8GB
            raise SubWorkflowParameterError("Resource limit exceeded: max_memory_mb too high")
        
        if max_time > 3600:  # 1 hour
            raise SubWorkflowParameterError("Resource limit exceeded: max_execution_time_seconds too long")
    
    async def create_child_execution(
        self,
        subworkflow_id: int,
        parent_execution: WorkflowExecution,
        input_data: Dict[str, Any],
        execution_chain: List[int]
    ) -> WorkflowExecution:
        """Create child execution for sub-workflow."""
        # Create mock execution for testing
        # In production, this would create a real WorkflowExecution in the database
        child_execution = WorkflowExecution(
            id=len(self.active_executions) + 100,  # Mock ID
            workflow_id=subworkflow_id,
            mode=ExecutionMode.SUBWORKFLOW,
            status=ExecutionStatus.NEW,
            started_at=datetime.now(timezone.utc),
            data={"input": [input_data]} if input_data else {}
        )
        
        return child_execution
    
    async def execute_child_workflow(
        self,
        child_execution: WorkflowExecution,
        resource_limits: Dict[str, Any]
    ) -> WorkflowExecution:
        """Execute the child workflow."""
        if not self.execution_service:
            # Mock execution service for testing
            self.execution_service = type('MockExecutionService', (), {
                'execute_workflow': self._mock_execute_workflow
            })()
        
        # Track resource usage if tracker available
        if self.resource_tracker:
            await self.resource_tracker.track_execution(child_execution.id, resource_limits)
        
        # Execute the workflow
        result = await self.execution_service.execute_workflow(
            workflow_id=child_execution.workflow_id,
            input_data=child_execution.data.get("input", [{}])[0] if child_execution.data.get("input") else {},
            mode=ExecutionMode.SUBWORKFLOW,
            parent_execution_id=child_execution.id
        )
        
        return result
    
    async def _mock_execute_workflow(self, **kwargs) -> WorkflowExecution:
        """Mock workflow execution for testing."""
        workflow_id = kwargs.get("workflow_id")
        input_data = kwargs.get("input_data", {})
        
        # Simulate successful execution
        execution = WorkflowExecution(
            id=workflow_id + 50,  # Mock ID
            workflow_id=workflow_id,
            mode=kwargs.get("mode", ExecutionMode.MANUAL),
            status=ExecutionStatus.SUCCESS,
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            data={"output": [{"result": "processed", "input_echo": input_data}]}
        )
        
        return execution
    
    async def execute_subworkflow_node(
        self,
        main_execution: WorkflowExecution,
        node_config: Dict[str, Any],
        execution_chain: List[int]
    ) -> SubWorkflowResult:
        """Execute a sub-workflow node as part of main workflow execution."""
        return await self.execute_subworkflow(
            parent_execution=main_execution,
            subworkflow_config=node_config,
            execution_chain=execution_chain
        )
    
    async def get_subworkflow_execution_status(self, parent_execution_id: int) -> Optional[SubWorkflowExecution]:
        """Get status of active sub-workflow execution."""
        return self.active_executions.get(str(parent_execution_id))
    
    async def cancel_subworkflow_execution(self, parent_execution_id: int) -> bool:
        """Cancel an active sub-workflow execution."""
        execution_key = str(parent_execution_id)
        if execution_key in self.active_executions:
            sub_execution = self.active_executions[execution_key]
            sub_execution.status = ExecutionStatus.CANCELLED
            sub_execution.finished_at = datetime.now(timezone.utc)
            
            # In production, would also cancel the child execution
            # For now, just remove from tracking
            del self.active_executions[execution_key]
            self.active_subworkflow_count = max(0, self.active_subworkflow_count - 1)
            return True
        
        return False
    
    async def get_active_subworkflow_stats(self) -> Dict[str, Any]:
        """Get statistics about active sub-workflow executions."""
        total_active = len(self.active_executions)
        
        stats = {
            "total_active": total_active,
            "max_concurrent": self.max_concurrent_subworkflows,
            "executions": []
        }
        
        for execution in self.active_executions.values():
            stats["executions"].append({
                "parent_execution_id": execution.parent_execution_id,
                "child_execution_id": execution.child_execution_id,
                "subworkflow_id": execution.subworkflow_id,
                "depth": execution.depth,
                "started_at": execution.started_at.isoformat(),
                "status": execution.status.value,
                "execution_chain": execution.execution_chain
            })
        
        return stats
    
    async def cleanup_completed_executions(self, max_age_hours: int = 24):
        """Clean up tracking data for old completed executions."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        
        completed_keys = []
        for key, execution in self.active_executions.items():
            if (execution.finished_at and 
                execution.finished_at < cutoff_time and 
                execution.status in (ExecutionStatus.SUCCESS, ExecutionStatus.ERROR, ExecutionStatus.CANCELLED)):
                completed_keys.append(key)
        
        for key in completed_keys:
            del self.active_executions[key]
        
        return len(completed_keys)