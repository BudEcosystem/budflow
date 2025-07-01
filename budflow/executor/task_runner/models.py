"""Task runner data models."""

import json
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, Dict, Any, List
from uuid import uuid4


class TaskStatus(str, Enum):
    """Task execution status."""
    
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    RESOURCE_LIMIT = "resource_limit"
    SECURITY_VIOLATION = "security_violation"


class MessageType(str, Enum):
    """Task message types."""
    
    # Runner -> Broker
    RUNNER_READY = "runner:ready"
    RUNNER_TASK_OFFER = "runner:taskoffer"
    RUNNER_TASK_ACCEPTED = "runner:taskaccepted"
    RUNNER_TASK_REJECTED = "runner:taskrejected"
    RUNNER_TASK_DONE = "runner:taskdone"
    RUNNER_TASK_ERROR = "runner:taskerror"
    RUNNER_TASK_PROGRESS = "runner:taskprogress"
    RUNNER_HEARTBEAT = "runner:heartbeat"
    RUNNER_SHUTDOWN = "runner:shutdown"
    
    # Broker -> Runner
    BROKER_TASK_OFFER_ACCEPT = "broker:taskofferaccept"
    BROKER_TASK_SETTINGS = "broker:tasksettings"
    BROKER_TASK_CANCEL = "broker:taskcancel"
    BROKER_SHUTDOWN = "broker:shutdown"
    
    # Runner -> Main (data requests)
    RUNNER_DATA_REQUEST = "runner:datarequest"
    RUNNER_RPC_CALL = "runner:rpccall"


@dataclass
class TaskRequest:
    """Task execution request."""
    
    task_id: str = field(default_factory=lambda: str(uuid4()))
    code: str = ""
    context: Dict[str, Any] = field(default_factory=dict)
    required_modules: List[str] = field(default_factory=list)
    timeout: Optional[int] = None  # Override default timeout
    priority: int = 1000  # Lower number = higher priority
    chunked: bool = False  # Whether to process in chunks
    chunk_size: Optional[int] = None
    
    def __post_init__(self):
        """Validate task request."""
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("Timeout must be positive")
        
        # Check payload size (simplified check)
        payload_size = sys.getsizeof(json.dumps(self.context))
        max_size = 1024 * 1024 * 1024  # 1GB
        if payload_size > max_size:
            raise ValueError(f"Payload size {payload_size} exceeds maximum {max_size}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskRequest":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class TaskResult:
    """Task execution result."""
    
    task_id: str
    status: TaskStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    execution_time_ms: Optional[int] = None
    resource_usage: Optional[Dict[str, Any]] = None
    logs: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data["status"] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskResult":
        """Create from dictionary."""
        data["status"] = TaskStatus(data["status"])
        return cls(**data)
    
    @property
    def is_success(self) -> bool:
        """Check if task succeeded."""
        return self.status == TaskStatus.COMPLETED
    
    @property
    def is_failure(self) -> bool:
        """Check if task failed."""
        return self.status in [
            TaskStatus.FAILED,
            TaskStatus.TIMEOUT,
            TaskStatus.CANCELLED,
            TaskStatus.RESOURCE_LIMIT,
            TaskStatus.SECURITY_VIOLATION,
        ]


@dataclass
class TaskOffer:
    """Task execution offer from runner."""
    
    offer_id: str = field(default_factory=lambda: str(uuid4()))
    runner_id: str = ""
    valid_until: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc) + timedelta(seconds=60)
    )
    capabilities: List[str] = field(default_factory=list)
    available_memory_mb: Optional[int] = None
    available_cpu_percent: Optional[float] = None
    
    def is_valid(self) -> bool:
        """Check if offer is still valid."""
        return datetime.now(timezone.utc) < self.valid_until
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data["valid_until"] = self.valid_until.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskOffer":
        """Create from dictionary."""
        data["valid_until"] = datetime.fromisoformat(data["valid_until"])
        return cls(**data)


@dataclass
class TaskMessage:
    """Base class for task messages."""
    
    type: Optional[MessageType] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        if self.type is not None:
            data["type"] = self.type.value
        data["timestamp"] = self.timestamp.isoformat()
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())


@dataclass
class RunnerReadyMessage(TaskMessage):
    """Runner ready message."""
    
    runner_id: str = ""
    capabilities: List[str] = field(default_factory=list)
    max_concurrency: int = 1
    
    def __post_init__(self):
        self.type = MessageType.RUNNER_READY


@dataclass
class TaskOfferMessage(TaskMessage):
    """Task offer message."""
    
    offer: Optional[TaskOffer] = None
    
    def __post_init__(self):
        self.type = MessageType.RUNNER_TASK_OFFER
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        if self.offer is not None:
            data["offer"] = self.offer.to_dict()
        return data


@dataclass
class TaskOfferAcceptMessage(TaskMessage):
    """Task offer accept message."""
    
    offer_id: str = ""
    task: Optional[TaskRequest] = None
    
    def __post_init__(self):
        self.type = MessageType.BROKER_TASK_OFFER_ACCEPT
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        if self.task is not None:
            data["task"] = self.task.to_dict()
        return data


@dataclass
class TaskAcceptedMessage(TaskMessage):
    """Task accepted message."""
    
    task_id: str = ""
    runner_id: str = ""
    
    def __post_init__(self):
        self.type = MessageType.RUNNER_TASK_ACCEPTED


@dataclass
class TaskDoneMessage(TaskMessage):
    """Task done message."""
    
    result: Optional[TaskResult] = None
    
    def __post_init__(self):
        self.type = MessageType.RUNNER_TASK_DONE
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = super().to_dict()
        if self.result is not None:
            data["result"] = self.result.to_dict()
        return data


@dataclass
class TaskErrorMessage(TaskMessage):
    """Task error message."""
    
    task_id: str = ""
    error: str = ""
    error_type: str = ""
    traceback: Optional[str] = None
    
    def __post_init__(self):
        self.type = MessageType.RUNNER_TASK_ERROR


@dataclass
class TaskProgressMessage(TaskMessage):
    """Task progress message."""
    
    task_id: str = ""
    progress: float = 0.0  # 0.0 to 1.0
    message: Optional[str] = None
    
    def __post_init__(self):
        self.type = MessageType.RUNNER_TASK_PROGRESS


@dataclass
class HeartbeatMessage(TaskMessage):
    """Heartbeat message."""
    
    runner_id: str = ""
    running_tasks: List[str] = field(default_factory=list)
    resource_usage: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        self.type = MessageType.RUNNER_HEARTBEAT


@dataclass
class DataRequestMessage(TaskMessage):
    """Data request message."""
    
    task_id: str = ""
    request_id: str = field(default_factory=lambda: str(uuid4()))
    data_path: str = ""  # Path to requested data
    
    def __post_init__(self):
        self.type = MessageType.RUNNER_DATA_REQUEST


@dataclass
class RPCCallMessage(TaskMessage):
    """RPC call message."""
    
    task_id: str = ""
    call_id: str = field(default_factory=lambda: str(uuid4()))
    method: str = ""
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        self.type = MessageType.RUNNER_RPC_CALL