"""Execution schemas for API requests and responses."""

from datetime import datetime
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from ..workflows.models import ExecutionMode, ExecutionStatus


class PartialExecutionRequest(BaseModel):
    """Request schema for partial execution."""
    
    start_node: str = Field(..., description="Name of the node to start execution from")
    input_data: Optional[Dict[str, Any]] = Field(None, description="Input data for the start node")
    previous_execution_id: Optional[str] = Field(None, description="ID of previous execution to get data from")
    wait_till_completion: bool = Field(False, description="Whether to wait for execution completion")
    execution_mode: ExecutionMode = Field(ExecutionMode.MANUAL, description="Execution mode")


class PartialExecutionResponse(BaseModel):
    """Response schema for partial execution."""
    
    execution_id: str = Field(..., description="ID of the created execution")
    status: ExecutionStatus = Field(..., description="Current execution status")
    start_node: str = Field(..., description="Starting node")
    execution_type: str = Field(..., description="Type of execution (partial/full)")
    nodes_executed: List[str] = Field(..., description="List of nodes that were executed")
    started_at: datetime = Field(..., description="Execution start time")
    finished_at: Optional[datetime] = Field(None, description="Execution completion time")
    data: Optional[Dict[str, Any]] = Field(None, description="Execution results")
    error: Optional[Dict[str, Any]] = Field(None, description="Error details if execution failed")


class ExecutionSummary(BaseModel):
    """Summary of execution for listing purposes."""
    
    id: str = Field(..., description="Execution ID")
    workflow_id: int = Field(..., description="Workflow ID")
    status: ExecutionStatus = Field(..., description="Execution status")
    mode: ExecutionMode = Field(..., description="Execution mode")
    execution_type: str = Field(..., description="Type of execution")
    start_node: Optional[str] = Field(None, description="Starting node for partial execution")
    started_at: datetime = Field(..., description="Start time")
    finished_at: Optional[datetime] = Field(None, description="Completion time")
    duration_seconds: Optional[float] = Field(None, description="Execution duration in seconds")


class DependencyGraphResponse(BaseModel):
    """Response schema for dependency graph analysis."""
    
    workflow_id: int = Field(..., description="Workflow ID")
    nodes: List[str] = Field(..., description="All nodes in the workflow")
    dependencies: Dict[str, List[str]] = Field(..., description="Forward dependencies (node -> downstream nodes)")
    reverse_dependencies: Dict[str, List[str]] = Field(..., description="Reverse dependencies (node -> upstream nodes)")
    has_cycles: bool = Field(..., description="Whether the workflow has circular dependencies")


class NodeExecutionPlan(BaseModel):
    """Execution plan for a specific starting node."""
    
    start_node: str = Field(..., description="Starting node")
    nodes_to_execute: List[str] = Field(..., description="Nodes that will be executed")
    execution_order: List[str] = Field(..., description="Suggested execution order")
    estimated_duration: Optional[float] = Field(None, description="Estimated execution time in seconds")


class PartialExecutionPreview(BaseModel):
    """Preview of what would happen in a partial execution."""
    
    workflow_id: int = Field(..., description="Workflow ID")
    start_node: str = Field(..., description="Starting node")
    execution_plan: NodeExecutionPlan = Field(..., description="Execution plan")
    dependency_graph: DependencyGraphResponse = Field(..., description="Workflow dependency graph")
    warnings: List[str] = Field(default_factory=list, description="Warnings about the execution plan")


class ExecutionCreate(BaseModel):
    """Schema for creating a new execution."""
    
    workflow_id: int = Field(..., description="Workflow ID")
    mode: ExecutionMode = Field(ExecutionMode.MANUAL, description="Execution mode")
    start_node: Optional[str] = Field(None, description="Starting node for partial execution")
    data: Optional[Dict[str, Any]] = Field(None, description="Input data")


class ExecutionBulkDelete(BaseModel):
    """Schema for bulk deleting executions."""
    
    execution_ids: List[str] = Field(..., description="List of execution IDs to delete")


class ExecutionResume(BaseModel):
    """Schema for resuming an execution."""
    
    data: Optional[Dict[str, Any]] = Field(None, description="Resume data")


class ExecutionMetrics(BaseModel):
    """Schema for execution metrics."""
    
    totalExecutions: int = Field(..., description="Total number of executions")
    successfulExecutions: int = Field(..., description="Number of successful executions")
    failedExecutions: int = Field(..., description="Number of failed executions")
    successRate: float = Field(..., description="Success rate percentage")
    averageExecutionTime: float = Field(..., description="Average execution time in ms")
    minExecutionTime: int = Field(..., description="Minimum execution time in ms")
    maxExecutionTime: int = Field(..., description="Maximum execution time in ms")
    startDate: datetime = Field(..., description="Start date of the metrics period")
    endDate: datetime = Field(..., description="End date of the metrics period")