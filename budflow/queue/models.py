"""Queue system data models."""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, Dict, Any, List
import json
from uuid import uuid4


class JobStatus(str, Enum):
    """Job status enumeration."""
    
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    STALLED = "stalled"
    CANCELLED = "cancelled"


class MessageType(str, Enum):
    """Job message types."""
    
    RESPOND_TO_WEBHOOK = "respondToWebhook"
    JOB_FINISHED = "jobFinished"
    JOB_FAILED = "jobFailed"
    ABORT_JOB = "abortJob"
    SEND_CHUNK = "sendChunk"
    JOB_PROGRESS = "jobProgress"


class QueueEvents(str, Enum):
    """Queue event types."""
    
    JOB_ENQUEUED = "job-enqueued"
    JOB_DEQUEUED = "job-dequeued"
    JOB_FINISHED = "job-finished"
    JOB_FAILED = "job-failed"
    JOB_STALLED = "job-stalled"
    JOB_PROGRESS = "job-progress"


@dataclass
class JobData:
    """Job data structure."""
    
    workflow_id: str
    execution_id: str
    load_static_data: bool = False
    push_ref: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobData":
        """Create from dictionary."""
        return cls(**data)
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "JobData":
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class JobOptions:
    """Job execution options."""
    
    priority: int = 1000  # 1 = highest, higher numbers = lower priority
    remove_on_complete: bool = True
    remove_on_fail: bool = True
    delay: Optional[timedelta] = None
    retries: int = 0  # No automatic retries (uses stalled mechanism)
    
    def to_celery_options(self) -> Dict[str, Any]:
        """Convert to Celery task options."""
        options = {
            "priority": self.priority,
            "expires": None,  # No expiration
        }
        
        if self.delay:
            # Calculate ETA for delayed execution
            eta = datetime.now(timezone.utc) + self.delay
            options["eta"] = eta
        
        return options


class JobMessage:
    """Base class for job messages."""
    
    def __init__(self):
        self.timestamp = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self) if hasattr(self, "__dataclass_fields__") else self.__dict__.copy()
        data["timestamp"] = self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else self.timestamp
        if hasattr(self, "type"):
            data["type"] = self.type.value if hasattr(self.type, "value") else self.type
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())


@dataclass
class JobProgressMessage(JobMessage):
    """Progress update message."""
    
    execution_id: str
    progress: float  # 0.0 to 1.0
    message: Optional[str] = None
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.JOB_PROGRESS


@dataclass
class RespondToWebhookMessage(JobMessage):
    """Webhook response message."""
    
    webhook_id: str
    response_data: Dict[str, Any]
    response_code: int = 200
    response_headers: Optional[Dict[str, str]] = None
    binary_data: Optional[str] = None  # Base64 encoded
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.RESPOND_TO_WEBHOOK


@dataclass
class JobFinishedMessage(JobMessage):
    """Job completion message."""
    
    execution_id: str
    success: bool
    result_data: Optional[Dict[str, Any]] = None
    execution_time_ms: Optional[int] = None
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.JOB_FINISHED


@dataclass
class JobFailedMessage(JobMessage):
    """Job failure message."""
    
    execution_id: str
    error_message: str
    error_details: Optional[Dict[str, Any]] = None
    stack_trace: Optional[str] = None
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.JOB_FAILED


@dataclass
class AbortJobMessage(JobMessage):
    """Job abort request message."""
    
    job_id: str
    reason: str
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.ABORT_JOB


@dataclass
class SendChunkMessage(JobMessage):
    """Execution chunk message for streaming."""
    
    execution_id: str
    chunk_data: Dict[str, Any]
    chunk_index: int
    is_final: bool = False
    
    def __post_init__(self):
        super().__init__()
        self.type = MessageType.SEND_CHUNK


@dataclass
class QueueMetrics:
    """Queue metrics data."""
    
    active_count: int = 0
    waiting_count: int = 0
    completed_count: int = 0
    failed_count: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Additional metrics
    avg_processing_time_ms: Optional[float] = None
    max_processing_time_ms: Optional[int] = None
    min_processing_time_ms: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()
        return data
    
    def to_prometheus_format(self) -> str:
        """Convert to Prometheus text format."""
        lines = [
            "# HELP budflow_queue_active_jobs Number of active jobs",
            "# TYPE budflow_queue_active_jobs gauge",
            f"budflow_queue_active_jobs {self.active_count}",
            "",
            "# HELP budflow_queue_waiting_jobs Number of waiting jobs",
            "# TYPE budflow_queue_waiting_jobs gauge",
            f"budflow_queue_waiting_jobs {self.waiting_count}",
            "",
            "# HELP budflow_queue_completed_jobs_total Total completed jobs",
            "# TYPE budflow_queue_completed_jobs_total counter",
            f"budflow_queue_completed_jobs_total {self.completed_count}",
            "",
            "# HELP budflow_queue_failed_jobs_total Total failed jobs",
            "# TYPE budflow_queue_failed_jobs_total counter",
            f"budflow_queue_failed_jobs_total {self.failed_count}",
        ]
        
        if self.avg_processing_time_ms is not None:
            lines.extend([
                "",
                "# HELP budflow_queue_processing_time_ms Job processing time in milliseconds",
                "# TYPE budflow_queue_processing_time_ms summary",
                f"budflow_queue_processing_time_ms_sum {self.avg_processing_time_ms * (self.completed_count or 1)}",
                f"budflow_queue_processing_time_ms_count {self.completed_count}",
            ])
        
        return "\n".join(lines)


@dataclass
class JobHeartbeat:
    """Job heartbeat data for stalled detection."""
    
    job_id: str
    worker_id: str
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    stalled_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "job_id": self.job_id,
            "worker_id": self.worker_id,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "stalled_count": self.stalled_count,
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "JobHeartbeat":
        """Create from JSON string."""
        data = json.loads(json_str)
        data["last_heartbeat"] = datetime.fromisoformat(data["last_heartbeat"])
        return cls(**data)