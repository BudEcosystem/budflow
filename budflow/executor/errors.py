"""Execution engine error classes."""

from typing import Any, Dict, Optional


class ExecutionError(Exception):
    """Base class for all execution errors."""
    
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "details": self.details,
        }


class WorkflowExecutionError(ExecutionError):
    """Raised when workflow execution fails."""
    
    def __init__(
        self,
        message: str,
        workflow_id: int,
        execution_id: Optional[int] = None,
        **kwargs
    ):
        super().__init__(message, **kwargs)
        self.workflow_id = workflow_id
        self.execution_id = execution_id
        self.details.update({
            "workflow_id": workflow_id,
            "execution_id": execution_id,
        })


class NodeExecutionError(ExecutionError):
    """Raised when node execution fails."""
    
    def __init__(
        self,
        message: str,
        node_id: int,
        node_name: str,
        node_type: str,
        execution_id: Optional[int] = None,
        **kwargs
    ):
        super().__init__(message, **kwargs)
        self.node_id = node_id
        self.node_name = node_name
        self.node_type = node_type
        self.execution_id = execution_id
        self.details.update({
            "node_id": node_id,
            "node_name": node_name,
            "node_type": node_type,
            "execution_id": execution_id,
        })


class ExecutionTimeoutError(ExecutionError):
    """Raised when execution times out."""
    
    def __init__(
        self,
        message: str,
        timeout_seconds: int,
        **kwargs
    ):
        super().__init__(message, error_code="TIMEOUT", **kwargs)
        self.timeout_seconds = timeout_seconds
        self.details["timeout_seconds"] = timeout_seconds


class ExecutionCancelledError(ExecutionError):
    """Raised when execution is cancelled."""
    
    def __init__(self, message: str = "Execution was cancelled", **kwargs):
        super().__init__(message, error_code="CANCELLED", **kwargs)


class DataValidationError(ExecutionError):
    """Raised when data validation fails."""
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        expected_type: Optional[str] = None,
        actual_value: Any = None,
        **kwargs
    ):
        super().__init__(message, error_code="VALIDATION_ERROR", **kwargs)
        self.field = field
        self.expected_type = expected_type
        self.actual_value = actual_value
        self.details.update({
            "field": field,
            "expected_type": expected_type,
            "actual_value": str(actual_value),
        })


class MissingCredentialsError(ExecutionError):
    """Raised when required credentials are missing."""
    
    def __init__(
        self,
        message: str,
        credential_type: str,
        node_name: str,
        **kwargs
    ):
        super().__init__(message, error_code="MISSING_CREDENTIALS", **kwargs)
        self.credential_type = credential_type
        self.node_name = node_name
        self.details.update({
            "credential_type": credential_type,
            "node_name": node_name,
        })


class CircularDependencyError(ExecutionError):
    """Raised when circular dependency is detected in workflow."""
    
    def __init__(
        self,
        message: str,
        cycle_path: list,
        **kwargs
    ):
        super().__init__(message, error_code="CIRCULAR_DEPENDENCY", **kwargs)
        self.cycle_path = cycle_path
        self.details["cycle_path"] = cycle_path


class CircularWorkflowError(ExecutionError):
    """Raised when circular workflow dependency is detected."""
    
    def __init__(
        self,
        message: str,
        workflow_path: Optional[list] = None,
        **kwargs
    ):
        super().__init__(message, error_code="CIRCULAR_WORKFLOW", **kwargs)
        self.workflow_path = workflow_path or []
        self.details["workflow_path"] = self.workflow_path


class WorkflowValidationError(ExecutionError):
    """Raised when workflow validation fails."""
    
    def __init__(
        self,
        message: str,
        validation_errors: Optional[list] = None,
        **kwargs
    ):
        super().__init__(message, error_code="WORKFLOW_VALIDATION", **kwargs)
        self.validation_errors = validation_errors or []
        self.details["validation_errors"] = self.validation_errors