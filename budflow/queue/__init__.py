"""
BudFlow Queue System

A Python-based distributed job queue system for workflow execution,
designed as a drop-in replacement for n8n's Bull/Redis queue system.

Key Features:
- Celery-based job processing with Redis backend
- Automatic stalled job detection and recovery
- Graceful worker shutdown with job completion
- Comprehensive metrics collection and export
- Multi-main instance support with leader election
- Real-time job progress updates
- Webhook response handling
- Binary data support

Usage:
    from budflow.queue import QueueManager, QueueConfig, WorkerManager
    
    # Initialize queue
    config = QueueConfig.from_env()
    queue = QueueManager(config)
    await queue.initialize()
    
    # Enqueue a job
    job_data = JobData(
        workflow_id="wf-123",
        execution_id="exec-456"
    )
    job_id = await queue.enqueue_job(job_data)
    
    # Start worker
    worker = WorkerManager(config)
    await worker.start()
"""

from .config import QueueConfig
from .exceptions import (
    QueueException,
    QueueConnectionError,
    QueueInitializationError,
    JobProcessingError,
    StalledJobError,
    MaxStalledCountError,
    QueueRecoveryError,
    WorkerShutdownError,
)
from .manager import QueueManager, EventEmitter
from .metrics import MetricsCollector, MetricsExporter, create_metrics_server
from .models import (
    JobData,
    JobOptions,
    JobStatus,
    JobMessage,
    JobProgressMessage,
    RespondToWebhookMessage,
    JobFinishedMessage,
    JobFailedMessage,
    AbortJobMessage,
    SendChunkMessage,
    MessageType,
    QueueEvents,
    QueueMetrics,
    JobHeartbeat,
)
from .recovery import QueueRecoveryService
from .worker import WorkerManager


__all__ = [
    # Configuration
    "QueueConfig",
    
    # Core components
    "QueueManager",
    "WorkerManager",
    "QueueRecoveryService",
    "EventEmitter",
    
    # Models
    "JobData",
    "JobOptions",
    "JobStatus",
    "JobMessage",
    "JobProgressMessage",
    "RespondToWebhookMessage",
    "JobFinishedMessage",
    "JobFailedMessage",
    "AbortJobMessage",
    "SendChunkMessage",
    "MessageType",
    "QueueEvents",
    "QueueMetrics",
    "JobHeartbeat",
    
    # Metrics
    "MetricsCollector",
    "MetricsExporter",
    "create_metrics_server",
    
    # Exceptions
    "QueueException",
    "QueueConnectionError",
    "QueueInitializationError",
    "JobProcessingError",
    "StalledJobError",
    "MaxStalledCountError",
    "QueueRecoveryError",
    "WorkerShutdownError",
]


# Version info
__version__ = "1.0.0"
__author__ = "BudFlow Team"