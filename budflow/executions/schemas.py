"""Execution API schemas."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from budflow.workflows.models import ExecutionStatus


class ExecutionCreate(BaseModel):
    """Schema for creating an execution."""
    workflow_id: UUID
    mode: str = "manual"
    data: Optional[Dict[str, Any]] = None
    start_node: Optional[str] = None


class ExecutionResume(BaseModel):
    """Schema for resuming a waiting execution."""
    data: Dict[str, Any]
    node_id: Optional[str] = None


class ExecutionBulkDelete(BaseModel):
    """Schema for bulk deleting executions."""
    execution_ids: List[UUID]


class NodeExecutionResponse(BaseModel):
    """Schema for node execution response."""
    node_id: str = Field(alias="nodeId")
    node_name: str = Field(alias="nodeName")
    status: str
    input_data: Optional[Dict[str, Any]] = Field(alias="inputData")
    output_data: Optional[Dict[str, Any]] = Field(alias="outputData")
    error: Optional[Dict[str, Any]] = None
    started_at: Optional[datetime] = Field(alias="startedAt")
    finished_at: Optional[datetime] = Field(alias="finishedAt")
    execution_time: Optional[int] = Field(alias="executionTime")

    class Config:
        populate_by_name = True


class ExecutionResponse(BaseModel):
    """Schema for execution response."""
    id: UUID
    workflow_id: UUID = Field(alias="workflowId")
    user_id: Optional[UUID] = Field(alias="userId")
    mode: str
    status: ExecutionStatus
    data: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    started_at: Optional[datetime] = Field(alias="startedAt")
    finished_at: Optional[datetime] = Field(alias="finishedAt")
    execution_time_ms: Optional[int] = Field(alias="executionTimeMs")
    retry_of: Optional[UUID] = Field(alias="retryOf")
    retry_count: int = Field(alias="retryCount", default=0)
    created_at: datetime = Field(alias="createdAt")

    class Config:
        populate_by_name = True
        from_attributes = True


class ExecutionListResponse(BaseModel):
    """Schema for execution list response."""
    items: List[ExecutionResponse]
    total: int
    limit: int
    offset: int


class ExecutionDataResponse(BaseModel):
    """Schema for execution data response."""
    input: Optional[Dict[str, Any]] = None
    output: Dict[str, Any] = {}
    error: Optional[Dict[str, Any]] = None


class ExecutionLogEntry(BaseModel):
    """Schema for execution log entry."""
    timestamp: str
    level: str
    message: str
    node_id: Optional[str] = Field(alias="nodeId", default=None)
    details: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True


class ExecutionLogsResponse(BaseModel):
    """Schema for execution logs response."""
    logs: List[ExecutionLogEntry]


class ExecutionNodesResponse(BaseModel):
    """Schema for execution nodes response."""
    nodes: List[NodeExecutionResponse]


class ExecutionMetrics(BaseModel):
    """Schema for execution metrics."""
    total_executions: int = Field(alias="totalExecutions")
    successful_executions: int = Field(alias="successfulExecutions")
    failed_executions: int = Field(alias="failedExecutions")
    success_rate: float = Field(alias="successRate")
    average_execution_time: float = Field(alias="averageExecutionTime")
    min_execution_time: int = Field(alias="minExecutionTime")
    max_execution_time: int = Field(alias="maxExecutionTime")
    start_date: datetime = Field(alias="startDate")
    end_date: datetime = Field(alias="endDate")

    class Config:
        populate_by_name = True


class ExecutionSearchResponse(BaseModel):
    """Schema for execution search response."""
    items: List[ExecutionResponse]
    total: int
    limit: int
    offset: int
    query: str