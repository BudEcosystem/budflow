"""Execution-related exceptions."""


class ExecutionError(Exception):
    """Base exception for execution errors."""
    pass


class ExecutionNotFoundError(ExecutionError):
    """Raised when execution is not found."""
    pass


class ExecutionPermissionError(ExecutionError):
    """Raised when user lacks permission for execution operation."""
    pass


class ExecutionStateError(ExecutionError):
    """Raised when execution is in invalid state for operation."""
    pass


class ExecutionTimeoutError(ExecutionError):
    """Raised when execution times out."""
    pass