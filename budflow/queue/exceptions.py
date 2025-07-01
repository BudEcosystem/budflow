"""Queue system exceptions."""

from typing import Optional, Dict, Any


class QueueException(Exception):
    """Base exception for queue-related errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


class QueueConnectionError(QueueException):
    """Raised when unable to connect to queue backend."""
    pass


class QueueInitializationError(QueueException):
    """Raised when queue initialization fails."""
    pass


class JobProcessingError(QueueException):
    """Raised when job processing fails."""
    
    def __init__(self, message: str, job_id: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)
        self.job_id = job_id


class StalledJobError(QueueException):
    """Raised when a job is detected as stalled."""
    
    def __init__(self, message: str, job_id: str, stalled_count: int):
        super().__init__(message)
        self.job_id = job_id
        self.stalled_count = stalled_count


class MaxStalledCountError(StalledJobError):
    """Raised when a job exceeds maximum stalled count."""
    
    def __init__(self, job_id: str, max_count: int):
        message = f"Job {job_id} exceeded maximum stalled count of {max_count}"
        super().__init__(message, job_id, max_count)
        self.max_count = max_count


class QueueRecoveryError(QueueException):
    """Raised when queue recovery fails."""
    pass


class WorkerShutdownError(QueueException):
    """Raised when worker shutdown fails."""
    pass