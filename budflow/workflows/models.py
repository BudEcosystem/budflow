"""Workflow data models."""

import json
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

from sqlalchemy import (
    Boolean, Column, DateTime, Enum as SQLEnum, ForeignKey, Integer, 
    JSON, String, Text, UniqueConstraint, Index, CheckConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from budflow.database import Base


class WorkflowStatus(str, Enum):
    """Workflow status enumeration."""
    DRAFT = "draft"
    ACTIVE = "active"
    INACTIVE = "inactive"
    ARCHIVED = "archived"


class NodeType(str, Enum):
    """Node type enumeration."""
    TRIGGER = "trigger"
    ACTION = "action"
    CONDITION = "condition"
    LOOP = "loop"
    MERGE = "merge"
    SET = "set"
    FUNCTION = "function"
    WEBHOOK = "webhook"
    SCHEDULE = "schedule"
    MANUAL = "manual"


class ExecutionMode(str, Enum):
    """Execution mode enumeration."""
    TRIGGER = "trigger"
    MANUAL = "manual"
    WEBHOOK = "webhook"
    SCHEDULE = "schedule"
    TEST = "test"


class ExecutionStatus(str, Enum):
    """Execution status enumeration."""
    NEW = "new"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    CANCELED = "canceled"
    CRASHED = "crashed"
    WAITING = "waiting"


class NodeExecutionStatus(str, Enum):
    """Node execution status enumeration."""
    NEW = "new"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    DISABLED = "disabled"
    WAITING = "waiting"


class WorkflowPermission(str, Enum):
    """Workflow permission levels."""
    READ = "read"
    EXECUTE = "execute"
    UPDATE = "update"
    DELETE = "delete"
    SHARE = "share"
    OWNER = "owner"


class Workflow(Base):
    """Workflow model representing an automation workflow."""
    
    __tablename__ = "workflows"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[WorkflowStatus] = mapped_column(
        SQLEnum(WorkflowStatus), 
        default=WorkflowStatus.DRAFT,
        nullable=False
    )
    
    # User relationship
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    user: Mapped["User"] = relationship("User", back_populates="workflows")
    
    # Project and folder relationships
    project_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("projects.id"))
    folder_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("folders.id"))
    
    project: Mapped[Optional["Project"]] = relationship("Project", back_populates="workflows")
    folder: Mapped[Optional["Folder"]] = relationship("Folder", back_populates="workflows")
    
    # Workflow configuration
    settings: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    nodes: Mapped[List[Dict[str, Any]]] = mapped_column(JSON, default=list)
    connections: Mapped[List[Dict[str, Any]]] = mapped_column(JSON, default=list)
    
    # Flags
    active: Mapped[bool] = mapped_column(Boolean, default=False)
    archived: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Execution settings
    timezone: Mapped[str] = mapped_column(String(50), default="UTC")
    error_workflow_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("workflows.id", ondelete="SET NULL")
    )
    
    # Versioning
    version: Mapped[int] = mapped_column(Integer, default=1)
    version_id: Mapped[str] = mapped_column(String(36), default=lambda: str(uuid4()))
    hash: Mapped[Optional[str]] = mapped_column(String(64))  # SHA-256 of workflow definition
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    activated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # Statistics
    total_executions: Mapped[int] = mapped_column(Integer, default=0)
    successful_executions: Mapped[int] = mapped_column(Integer, default=0)
    failed_executions: Mapped[int] = mapped_column(Integer, default=0)
    last_execution_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # Relationships (using JSON fields for node storage like n8n)
    workflow_nodes: Mapped[List["WorkflowNode"]] = relationship(
        "WorkflowNode", back_populates="workflow", cascade="all, delete-orphan"
    )
    workflow_connections: Mapped[List["WorkflowConnection"]] = relationship(
        "WorkflowConnection", back_populates="workflow", cascade="all, delete-orphan"
    )
    executions: Mapped[List["WorkflowExecution"]] = relationship(
        "WorkflowExecution", back_populates="workflow", cascade="all, delete-orphan"
    )
    error_workflow: Mapped[Optional["Workflow"]] = relationship(
        "Workflow", remote_side=[id], back_populates="error_workflows"
    )
    error_workflows: Mapped[List["Workflow"]] = relationship(
        "Workflow", back_populates="error_workflow"
    )
    shared_with: Mapped[List["SharedWorkflow"]] = relationship(
        "SharedWorkflow", back_populates="workflow", cascade="all, delete-orphan"
    )
    workflow_tags: Mapped[List["WorkflowTag"]] = relationship(
        "WorkflowTag", back_populates="workflow", cascade="all, delete-orphan"
    )
    
    # Indexes
    __table_args__ = (
        Index("ix_workflows_user_id", "user_id"),
        Index("ix_workflows_status", "status"),
        Index("ix_workflows_updated_at", "updated_at"),
    )
    
    @hybrid_property
    def is_active(self) -> bool:
        """Check if workflow is active."""
        return self.status == WorkflowStatus.ACTIVE
    
    @property
    def tags(self) -> List["WorkflowTag"]:
        """Alias for workflow_tags for compatibility."""
        return self.workflow_tags
    
    @hybrid_property
    def success_rate(self) -> float:
        """Calculate workflow success rate."""
        if self.total_executions == 0:
            return 0.0
        return (self.successful_executions / self.total_executions) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert workflow to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "status": self.status.value,
            "user_id": self.user_id,
            "settings": self.settings,
            "nodes": self.nodes,
            "connections": self.connections,
            "active": self.active,
            "archived": self.archived,
            "version_id": self.version_id,
            "timezone": self.timezone,
            "version": self.version,
            "hash": self.hash,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "activated_at": self.activated_at.isoformat() if self.activated_at else None,
            "total_executions": self.total_executions,
            "successful_executions": self.successful_executions,
            "failed_executions": self.failed_executions,
            "success_rate": self.success_rate,
            "last_execution_at": self.last_execution_at.isoformat() if self.last_execution_at else None,
        }
    
    def __repr__(self) -> str:
        return f"<Workflow(id={self.id}, name='{self.name}', status='{self.status.value}')>"


class WorkflowNode(Base):
    """Workflow node model representing individual steps in a workflow."""
    
    __tablename__ = "workflow_nodes"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(String(36), default=lambda: str(uuid4()), unique=True)
    
    # Workflow relationship
    workflow_id: Mapped[int] = mapped_column(Integer, ForeignKey("workflows.id"), nullable=False)
    workflow: Mapped["Workflow"] = relationship("Workflow", back_populates="nodes")
    
    # Node configuration
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped[NodeType] = mapped_column(SQLEnum(NodeType), nullable=False)
    type_version: Mapped[str] = mapped_column(String(20), default="1.0")
    
    # Node definition
    parameters: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    credentials: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    
    # UI positioning
    position: Mapped[Dict[str, float]] = mapped_column(JSON, default=lambda: {"x": 0, "y": 0})
    
    # Node settings
    disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    always_output_data: Mapped[bool] = mapped_column(Boolean, default=False)
    retry_on_fail: Mapped[bool] = mapped_column(Boolean, default=False)
    max_tries: Mapped[int] = mapped_column(Integer, default=3)
    wait_between_tries: Mapped[int] = mapped_column(Integer, default=1000)  # milliseconds
    
    # Conditional execution
    execute_once: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Notes and documentation
    notes: Mapped[Optional[str]] = mapped_column(Text)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    outgoing_connections: Mapped[List["WorkflowConnection"]] = relationship(
        "WorkflowConnection", 
        foreign_keys="WorkflowConnection.source_node_id",
        back_populates="source_node",
        cascade="all, delete-orphan"
    )
    incoming_connections: Mapped[List["WorkflowConnection"]] = relationship(
        "WorkflowConnection",
        foreign_keys="WorkflowConnection.target_node_id", 
        back_populates="target_node"
    )
    executions: Mapped[List["NodeExecution"]] = relationship(
        "NodeExecution", back_populates="node", cascade="all, delete-orphan"
    )
    
    # Indexes
    __table_args__ = (
        Index("ix_workflow_nodes_workflow_id", "workflow_id"),
        Index("ix_workflow_nodes_type", "type"),
        Index("ix_workflow_nodes_uuid", "uuid"),
        UniqueConstraint("workflow_id", "uuid", name="uq_workflow_nodes_workflow_uuid"),
    )
    
    @hybrid_property
    def is_trigger_node(self) -> bool:
        """Check if node is a trigger node."""
        return self.type == NodeType.TRIGGER
    
    @hybrid_property
    def has_credentials(self) -> bool:
        """Check if node has credentials configured."""
        return self.credentials is not None and bool(self.credentials)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert node to dictionary."""
        return {
            "id": self.id,
            "uuid": self.uuid,
            "workflow_id": self.workflow_id,
            "name": self.name,
            "type": self.type.value,
            "type_version": self.type_version,
            "parameters": self.parameters,
            "credentials": self.credentials,
            "position": self.position,
            "disabled": self.disabled,
            "always_output_data": self.always_output_data,
            "retry_on_fail": self.retry_on_fail,
            "max_tries": self.max_tries,
            "wait_between_tries": self.wait_between_tries,
            "execute_once": self.execute_once,
            "notes": self.notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
    
    def __repr__(self) -> str:
        return f"<WorkflowNode(id={self.id}, uuid='{self.uuid}', name='{self.name}', type='{self.type.value}')>"


class WorkflowConnection(Base):
    """Workflow connection model representing edges between nodes."""
    
    __tablename__ = "workflow_connections"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    # Workflow relationship
    workflow_id: Mapped[int] = mapped_column(Integer, ForeignKey("workflows.id"), nullable=False)
    workflow: Mapped["Workflow"] = relationship("Workflow", back_populates="connections")
    
    # Connection endpoints
    source_node_id: Mapped[int] = mapped_column(Integer, ForeignKey("workflow_nodes.id"), nullable=False)
    target_node_id: Mapped[int] = mapped_column(Integer, ForeignKey("workflow_nodes.id"), nullable=False)
    
    # Connection configuration
    source_output: Mapped[str] = mapped_column(String(255), default="main")
    target_input: Mapped[str] = mapped_column(String(255), default="main")
    
    # Connection type (main, error, success, etc.)
    type: Mapped[str] = mapped_column(String(50), default="main")
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    
    # Relationships
    source_node: Mapped["WorkflowNode"] = relationship(
        "WorkflowNode", 
        foreign_keys=[source_node_id],
        back_populates="outgoing_connections"
    )
    target_node: Mapped["WorkflowNode"] = relationship(
        "WorkflowNode",
        foreign_keys=[target_node_id], 
        back_populates="incoming_connections"
    )
    
    # Constraints and indexes
    __table_args__ = (
        Index("ix_workflow_connections_workflow_id", "workflow_id"),
        Index("ix_workflow_connections_source_node_id", "source_node_id"),
        Index("ix_workflow_connections_target_node_id", "target_node_id"),
        UniqueConstraint(
            "workflow_id", "source_node_id", "target_node_id", "source_output", "target_input", "type",
            name="uq_workflow_connections_unique"
        ),
        CheckConstraint("source_node_id != target_node_id", name="ck_no_self_connection"),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert connection to dictionary."""
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "source_output": self.source_output,
            "target_input": self.target_input,
            "type": self.type,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
    
    def __repr__(self) -> str:
        return f"<WorkflowConnection(id={self.id}, {self.source_node_id}->{self.target_node_id})>"


class WorkflowExecution(Base):
    """Workflow execution model representing a single workflow run."""
    
    __tablename__ = "workflow_executions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    uuid: Mapped[str] = mapped_column(String(36), default=lambda: str(uuid4()), unique=True)
    
    # Workflow relationship
    workflow_id: Mapped[int] = mapped_column(Integer, ForeignKey("workflows.id"), nullable=False)
    workflow: Mapped["Workflow"] = relationship("Workflow", back_populates="executions")
    
    # User relationship (who triggered the execution)
    user_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("users.id"))
    user: Mapped[Optional["User"]] = relationship("User")
    
    # Execution details
    mode: Mapped[ExecutionMode] = mapped_column(SQLEnum(ExecutionMode), nullable=False)
    status: Mapped[ExecutionStatus] = mapped_column(
        SQLEnum(ExecutionStatus), 
        default=ExecutionStatus.NEW,
        nullable=False
    )
    
    # Execution data
    data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)  # Input data
    error: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)  # Error details
    
    # Queue integration
    job_id: Mapped[Optional[str]] = mapped_column(String(36))  # Queue job ID for tracking
    
    # Timing
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer)  # Execution duration in milliseconds
    execution_time_ms: Mapped[Optional[int]] = mapped_column(Integer)  # Alias for duration_ms
    
    # Retry information
    retry_of: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("workflow_executions.id", ondelete="SET NULL")
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    
    # Relationships
    node_executions: Mapped[List["NodeExecution"]] = relationship(
        "NodeExecution", back_populates="workflow_execution", cascade="all, delete-orphan"
    )
    retries: Mapped[List["WorkflowExecution"]] = relationship(
        "WorkflowExecution", back_populates="original_execution"
    )
    original_execution: Mapped[Optional["WorkflowExecution"]] = relationship(
        "WorkflowExecution", remote_side=[id], back_populates="retries"
    )
    
    # Indexes
    __table_args__ = (
        Index("ix_workflow_executions_workflow_id", "workflow_id"),
        Index("ix_workflow_executions_status", "status"),
        Index("ix_workflow_executions_created_at", "created_at"),
        Index("ix_workflow_executions_uuid", "uuid"),
    )
    
    @hybrid_property
    def is_running(self) -> bool:
        """Check if execution is currently running."""
        return self.status == ExecutionStatus.RUNNING
    
    @hybrid_property
    def is_finished(self) -> bool:
        """Check if execution is finished."""
        return self.status in (ExecutionStatus.SUCCESS, ExecutionStatus.ERROR, ExecutionStatus.CANCELED)
    
    @hybrid_property
    def success_rate(self) -> float:
        """Calculate node execution success rate."""
        if not self.node_executions:
            return 0.0
        successful = sum(1 for ne in self.node_executions if ne.status == NodeExecutionStatus.SUCCESS)
        return (successful / len(self.node_executions)) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert execution to dictionary."""
        return {
            "id": self.id,
            "uuid": self.uuid,
            "workflow_id": self.workflow_id,
            "user_id": self.user_id,
            "mode": self.mode.value,
            "status": self.status.value,
            "data": self.data,
            "error": self.error,
            "job_id": self.job_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_ms": self.duration_ms,
            "retry_of": self.retry_of,
            "retry_count": self.retry_count,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "success_rate": self.success_rate,
        }
    
    def __repr__(self) -> str:
        return f"<WorkflowExecution(id={self.id}, uuid='{self.uuid}', status='{self.status.value}')>"


class NodeExecution(Base):
    """Node execution model representing execution of a single node."""
    
    __tablename__ = "node_executions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    # Relationships
    workflow_execution_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("workflow_executions.id"), nullable=False
    )
    workflow_execution: Mapped["WorkflowExecution"] = relationship(
        "WorkflowExecution", back_populates="node_executions"
    )
    
    node_id: Mapped[int] = mapped_column(Integer, ForeignKey("workflow_nodes.id"), nullable=False)
    node: Mapped["WorkflowNode"] = relationship("WorkflowNode", back_populates="executions")
    
    # Execution details
    status: Mapped[NodeExecutionStatus] = mapped_column(
        SQLEnum(NodeExecutionStatus), 
        default=NodeExecutionStatus.NEW,
        nullable=False
    )
    
    # Data
    input_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    output_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    error: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    
    # Timing
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer)
    execution_time_ms: Mapped[Optional[int]] = mapped_column(Integer)  # Alias for duration_ms
    
    # Retry information
    try_number: Mapped[int] = mapped_column(Integer, default=1)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    
    # Indexes
    __table_args__ = (
        Index("ix_node_executions_workflow_execution_id", "workflow_execution_id"),
        Index("ix_node_executions_node_id", "node_id"),
        Index("ix_node_executions_status", "status"),
        Index("ix_node_executions_created_at", "created_at"),
    )
    
    @hybrid_property
    def is_running(self) -> bool:
        """Check if node execution is currently running."""
        return self.status == NodeExecutionStatus.RUNNING
    
    @hybrid_property
    def is_finished(self) -> bool:
        """Check if node execution is finished."""
        return self.status in (NodeExecutionStatus.SUCCESS, NodeExecutionStatus.ERROR)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert node execution to dictionary."""
        return {
            "id": self.id,
            "workflow_execution_id": self.workflow_execution_id,
            "node_id": self.node_id,
            "status": self.status.value,
            "input_data": self.input_data,
            "output_data": self.output_data,
            "error": self.error,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_ms": self.duration_ms,
            "try_number": self.try_number,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
    
    def __repr__(self) -> str:
        return f"<NodeExecution(id={self.id}, node_id={self.node_id}, status='{self.status.value}')>"


class SharedWorkflow(Base):
    """Model for workflow sharing permissions."""
    
    __tablename__ = "shared_workflows"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    # Workflow relationship
    workflow_id: Mapped[int] = mapped_column(Integer, ForeignKey("workflows.id"), nullable=False)
    workflow: Mapped["Workflow"] = relationship("Workflow")
    
    # User relationship
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False)
    user: Mapped["User"] = relationship("User")
    
    # Permission level
    permission: Mapped[WorkflowPermission] = mapped_column(
        SQLEnum(WorkflowPermission),
        default=WorkflowPermission.READ,
        nullable=False
    )
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint("workflow_id", "user_id", name="uq_shared_workflows_workflow_user"),
        Index("ix_shared_workflows_workflow_id", "workflow_id"),
        Index("ix_shared_workflows_user_id", "user_id"),
    )
    
    def __repr__(self) -> str:
        return f"<SharedWorkflow(workflow_id={self.workflow_id}, user_id={self.user_id}, permission='{self.permission.value}')>"


class WorkflowTag(Base):
    """Model for workflow tags."""
    
    __tablename__ = "workflow_tags"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    # Workflow relationship
    workflow_id: Mapped[int] = mapped_column(Integer, ForeignKey("workflows.id"), nullable=False)
    workflow: Mapped["Workflow"] = relationship("Workflow")
    
    # Tag name
    tag: Mapped[str] = mapped_column(String(50), nullable=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint("workflow_id", "tag", name="uq_workflow_tags_workflow_tag"),
        Index("ix_workflow_tags_workflow_id", "workflow_id"),
        Index("ix_workflow_tags_tag", "tag"),
    )
    
    def __repr__(self) -> str:
        return f"<WorkflowTag(workflow_id={self.workflow_id}, tag='{self.tag}')>"


# Alias for compatibility
Execution = WorkflowExecution