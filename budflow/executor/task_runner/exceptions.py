"""Task runner exceptions."""

from typing import Optional, Dict, Any


class TaskRunnerError(Exception):
    """Base exception for task runner errors."""
    
    def __init__(
        self,
        message: str,
        task_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message)
        self.task_id = task_id
        self.details = details or {}


class TaskTimeoutError(TaskRunnerError):
    """Task execution timeout error."""
    
    def __init__(
        self,
        message: str = "Task execution timed out",
        task_id: Optional[str] = None,
        timeout_seconds: Optional[int] = None
    ):
        super().__init__(message, task_id)
        self.timeout_seconds = timeout_seconds
        self.details["timeout_seconds"] = timeout_seconds


class ResourceLimitError(TaskRunnerError):
    """Resource limit exceeded error."""
    
    def __init__(
        self,
        message: str,
        task_id: Optional[str] = None,
        resource_type: Optional[str] = None,
        limit: Optional[Any] = None,
        usage: Optional[Any] = None
    ):
        super().__init__(message, task_id)
        self.resource_type = resource_type
        self.limit = limit
        self.usage = usage
        self.details.update({
            "resource_type": resource_type,
            "limit": limit,
            "usage": usage,
        })


class SecurityViolationError(TaskRunnerError):
    """Security violation error."""
    
    def __init__(
        self,
        message: str,
        task_id: Optional[str] = None,
        violation_type: Optional[str] = None,
        code_snippet: Optional[str] = None
    ):
        super().__init__(message, task_id)
        self.violation_type = violation_type
        self.code_snippet = code_snippet
        self.details.update({
            "violation_type": violation_type,
            "code_snippet": code_snippet,
        })


class ProcessCommunicationError(TaskRunnerError):
    """Inter-process communication error."""
    
    def __init__(
        self,
        message: str,
        task_id: Optional[str] = None,
        operation: Optional[str] = None
    ):
        super().__init__(message, task_id)
        self.operation = operation
        self.details["operation"] = operation


class RunnerNotAvailableError(TaskRunnerError):
    """No runner available to execute task."""
    
    def __init__(
        self,
        message: str = "No runner available to execute task",
        task_id: Optional[str] = None,
        wait_time: Optional[float] = None
    ):
        super().__init__(message, task_id)
        self.wait_time = wait_time
        self.details["wait_time"] = wait_time


class RunnerCrashError(TaskRunnerError):
    """Runner process crashed."""
    
    def __init__(
        self,
        message: str,
        runner_id: str,
        exit_code: Optional[int] = None,
        signal: Optional[int] = None
    ):
        super().__init__(message)
        self.runner_id = runner_id
        self.exit_code = exit_code
        self.signal = signal
        self.details.update({
            "runner_id": runner_id,
            "exit_code": exit_code,
            "signal": signal,
        })