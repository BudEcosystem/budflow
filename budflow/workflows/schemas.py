"""Workflow Pydantic schemas."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, ConfigDict

from .models import WorkflowStatus, NodeType, ExecutionMode, ExecutionStatus, NodeExecutionStatus


# Base schemas
class WorkflowBase(BaseModel):
    """Base workflow schema."""
    name: str = Field(..., min_length=1, max_length=255, description="Workflow name")
    description: Optional[str] = Field(None, description="Workflow description")
    settings: Dict[str, Any] = Field(default_factory=dict, description="Workflow settings")
    tags: List[str] = Field(default_factory=list, description="Workflow tags")
    timezone: str = Field(default="UTC", description="Workflow timezone")


class WorkflowNodeBase(BaseModel):
    """Base workflow node schema."""
    name: str = Field(..., min_length=1, max_length=255, description="Node name")
    type: NodeType = Field(..., description="Node type")
    type_version: str = Field(default="1.0", description="Node type version")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Node parameters")
    credentials: Optional[Dict[str, Any]] = Field(None, description="Node credentials")
    position: Dict[str, float] = Field(
        default_factory=lambda: {"x": 0, "y": 0}, 
        description="Node position in UI"
    )
    disabled: bool = Field(default=False, description="Whether node is disabled")
    always_output_data: bool = Field(default=False, description="Always output data")
    retry_on_fail: bool = Field(default=False, description="Retry on failure")
    max_tries: int = Field(default=3, ge=1, le=10, description="Maximum retry attempts")
    wait_between_tries: int = Field(default=1000, ge=0, description="Wait between retries (ms)")
    execute_once: bool = Field(default=False, description="Execute only once")
    notes: Optional[str] = Field(None, description="Node notes")


class WorkflowConnectionBase(BaseModel):
    """Base workflow connection schema."""
    source_output: str = Field(default="main", description="Source output port")
    target_input: str = Field(default="main", description="Target input port") 
    type: str = Field(default="main", description="Connection type")


# Create schemas
class WorkflowCreate(WorkflowBase):
    """Schema for creating a workflow."""
    status: WorkflowStatus = Field(default=WorkflowStatus.DRAFT, description="Workflow status")


class WorkflowNodeCreate(WorkflowNodeBase):
    """Schema for creating a workflow node."""
    uuid: Optional[str] = Field(None, description="Node UUID (optional)")


class WorkflowConnectionCreate(WorkflowConnectionBase):
    """Schema for creating a workflow connection."""
    source_node_uuid: str = Field(..., description="Source node UUID")
    target_node_uuid: str = Field(..., description="Target node UUID")


class WorkflowExecutionCreate(BaseModel):
    """Schema for creating a workflow execution."""
    mode: ExecutionMode = Field(..., description="Execution mode")
    data: Optional[Dict[str, Any]] = Field(None, description="Input data")


# Update schemas
class WorkflowUpdate(BaseModel):
    """Schema for updating a workflow."""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="Workflow name")
    description: Optional[str] = Field(None, description="Workflow description")
    status: Optional[WorkflowStatus] = Field(None, description="Workflow status")
    settings: Optional[Dict[str, Any]] = Field(None, description="Workflow settings")
    tags: Optional[List[str]] = Field(None, description="Workflow tags")
    timezone: Optional[str] = Field(None, description="Workflow timezone")
    error_workflow_id: Optional[int] = Field(None, description="Error workflow ID")


class WorkflowNodeUpdate(BaseModel):
    """Schema for updating a workflow node."""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="Node name")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Node parameters")
    credentials: Optional[Dict[str, Any]] = Field(None, description="Node credentials")
    position: Optional[Dict[str, float]] = Field(None, description="Node position")
    disabled: Optional[bool] = Field(None, description="Whether node is disabled")
    always_output_data: Optional[bool] = Field(None, description="Always output data")
    retry_on_fail: Optional[bool] = Field(None, description="Retry on failure")
    max_tries: Optional[int] = Field(None, ge=1, le=10, description="Maximum retry attempts")
    wait_between_tries: Optional[int] = Field(None, ge=0, description="Wait between retries (ms)")
    execute_once: Optional[bool] = Field(None, description="Execute only once")
    notes: Optional[str] = Field(None, description="Node notes")


# Response schemas
class WorkflowNodeResponse(WorkflowNodeBase):
    """Response schema for workflow nodes."""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    uuid: str
    workflow_id: int
    created_at: datetime
    updated_at: datetime


class WorkflowConnectionResponse(WorkflowConnectionBase):
    """Response schema for workflow connections."""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    workflow_id: int
    source_node_id: int
    target_node_id: int
    created_at: datetime


class WorkflowResponse(WorkflowBase):
    """Response schema for workflows."""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    status: WorkflowStatus
    user_id: int
    version: int
    hash: Optional[str]
    created_at: datetime
    updated_at: datetime
    activated_at: Optional[datetime]
    total_executions: int
    successful_executions: int
    failed_executions: int
    success_rate: float
    last_execution_at: Optional[datetime]
    error_workflow_id: Optional[int]


class WorkflowDetailResponse(WorkflowResponse):
    """Detailed response schema for workflows with nodes and connections."""
    nodes: List[WorkflowNodeResponse] = Field(default_factory=list)
    connections: List[WorkflowConnectionResponse] = Field(default_factory=list)


class NodeExecutionResponse(BaseModel):
    """Response schema for node executions."""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    workflow_execution_id: int
    node_id: int
    status: NodeExecutionStatus
    input_data: Optional[Dict[str, Any]]
    output_data: Optional[Dict[str, Any]]
    error: Optional[Dict[str, Any]]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    duration_ms: Optional[int]
    try_number: int
    created_at: datetime


class WorkflowExecutionResponse(BaseModel):
    """Response schema for workflow executions."""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    uuid: str
    workflow_id: int
    user_id: Optional[int]
    mode: ExecutionMode
    status: ExecutionStatus
    data: Optional[Dict[str, Any]]
    error: Optional[Dict[str, Any]]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    duration_ms: Optional[int]
    retry_of: Optional[int]
    retry_count: int
    success_rate: float
    created_at: datetime


class WorkflowExecutionDetailResponse(WorkflowExecutionResponse):
    """Detailed response schema for workflow executions with node executions."""
    node_executions: List[NodeExecutionResponse] = Field(default_factory=list)


# List schemas
class WorkflowListResponse(BaseModel):
    """Response schema for workflow lists."""
    workflows: List[WorkflowResponse]
    total: int
    page: int
    size: int
    pages: int


class WorkflowExecutionListResponse(BaseModel):
    """Response schema for workflow execution lists."""
    executions: List[WorkflowExecutionResponse]
    total: int
    page: int
    size: int
    pages: int


# Complex workflow operations
class WorkflowDefinition(BaseModel):
    """Complete workflow definition schema."""
    workflow: WorkflowCreate
    nodes: List[WorkflowNodeCreate]
    connections: List[WorkflowConnectionCreate]
    
    @field_validator('connections')
    @classmethod
    def validate_connections(cls, v, info):
        """Validate that all connections reference existing nodes."""
        nodes_data = info.data.get('nodes')
        if not nodes_data:
            return v
        
        node_uuids = {node.uuid for node in nodes_data if node.uuid}
        
        for conn in v:
            if conn.source_node_uuid not in node_uuids:
                raise ValueError(f"Source node {conn.source_node_uuid} not found in nodes")
            if conn.target_node_uuid not in node_uuids:
                raise ValueError(f"Target node {conn.target_node_uuid} not found in nodes")
        
        return v


class WorkflowImport(BaseModel):
    """Schema for importing workflows."""
    workflow: WorkflowCreate
    nodes: List[WorkflowNodeCreate]
    connections: List[WorkflowConnectionCreate]
    replace_credentials: bool = Field(
        default=True, 
        description="Replace credential references with placeholders"
    )


class WorkflowExport(BaseModel):
    """Schema for exporting workflows."""
    workflow: WorkflowResponse
    nodes: List[WorkflowNodeResponse]
    connections: List[WorkflowConnectionResponse]
    export_credentials: bool = Field(
        default=False, 
        description="Include credentials in export (security risk)"
    )


# Workflow operation schemas
class WorkflowRun(BaseModel):
    """Schema for running a workflow."""
    mode: ExecutionMode = Field(ExecutionMode.MANUAL, description="Execution mode")
    data: Optional[Dict[str, Any]] = Field(None, description="Input data for workflow")
    
    class Config:
        schema_extra = {
            "example": {
                "mode": "manual",
                "data": {"key": "value"}
            }
        }


class WorkflowShare(BaseModel):
    """Schema for sharing workflows."""
    user_ids: List[int] = Field(..., description="User IDs to share with")
    permissions: List[str] = Field(
        default=["read", "execute"],
        description="Permissions to grant"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "user_ids": [2, 3],
                "permissions": ["read", "execute"]
            }
        }


# Statistics schemas
class WorkflowStatistics(BaseModel):
    """Workflow statistics schema."""
    total_workflows: int
    active_workflows: int
    total_executions: int
    successful_executions: int
    failed_executions: int
    average_execution_time: Optional[float]
    success_rate: float


class NodeStatistics(BaseModel):
    """Node statistics schema."""
    node_id: int
    node_name: str
    node_type: NodeType
    total_executions: int
    successful_executions: int
    failed_executions: int
    average_execution_time: Optional[float]
    success_rate: float


# Validation schemas
class WorkflowValidationError(BaseModel):
    """Workflow validation error schema."""
    type: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    node_id: Optional[int] = Field(None, description="Related node ID")
    connection_id: Optional[int] = Field(None, description="Related connection ID")


class WorkflowValidationResult(BaseModel):
    """Workflow validation result schema."""
    valid: bool = Field(..., description="Whether workflow is valid")
    errors: List[WorkflowValidationError] = Field(default_factory=list)
    warnings: List[WorkflowValidationError] = Field(default_factory=list)


# Execution control schemas
class WorkflowExecutionControl(BaseModel):
    """Schema for controlling workflow execution."""
    action: str = Field(..., description="Action to perform (stop, pause, resume)")
    reason: Optional[str] = Field(None, description="Reason for action")


class WorkflowBulkOperation(BaseModel):
    """Schema for bulk workflow operations."""
    workflow_ids: List[int] = Field(..., description="List of workflow IDs")
    operation: str = Field(..., description="Operation to perform")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Operation parameters")


# Search and filter schemas
class WorkflowFilter(BaseModel):
    """Schema for filtering workflows."""
    status: Optional[List[WorkflowStatus]] = Field(None, description="Filter by status")
    tags: Optional[List[str]] = Field(None, description="Filter by tags")
    user_id: Optional[int] = Field(None, description="Filter by user")
    created_after: Optional[datetime] = Field(None, description="Created after date")
    created_before: Optional[datetime] = Field(None, description="Created before date")
    search: Optional[str] = Field(None, description="Search in name and description")


class WorkflowExecutionFilter(BaseModel):
    """Schema for filtering workflow executions."""
    workflow_id: Optional[int] = Field(None, description="Filter by workflow")
    status: Optional[List[ExecutionStatus]] = Field(None, description="Filter by status")
    mode: Optional[List[ExecutionMode]] = Field(None, description="Filter by mode")
    user_id: Optional[int] = Field(None, description="Filter by user")
    started_after: Optional[datetime] = Field(None, description="Started after date")
    started_before: Optional[datetime] = Field(None, description="Started before date")


# Message schemas
class MessageResponse(BaseModel):
    """Generic message response schema."""
    message: str = Field(..., description="Response message")


class ErrorResponse(BaseModel):
    """Error response schema."""
    error: str = Field(..., description="Error type")
    detail: str = Field(..., description="Error details")
    code: Optional[str] = Field(None, description="Error code")