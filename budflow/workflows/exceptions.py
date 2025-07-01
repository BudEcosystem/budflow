"""Workflow-related exceptions."""


class WorkflowError(Exception):
    """Base exception for workflow errors."""
    pass


class WorkflowNotFoundError(WorkflowError):
    """Raised when workflow is not found."""
    pass


class WorkflowValidationError(WorkflowError):
    """Raised when workflow validation fails."""
    pass


class WorkflowPermissionError(WorkflowError):
    """Raised when user lacks permission for workflow operation."""
    pass


class WorkflowVersionConflictError(WorkflowError):
    """Raised when workflow version conflict occurs."""
    pass


class WorkflowExecutionError(WorkflowError):
    """Raised when workflow execution fails."""
    pass


class WorkflowActivationError(WorkflowError):
    """Raised when workflow activation/deactivation fails."""
    pass