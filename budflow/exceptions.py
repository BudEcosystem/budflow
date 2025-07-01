"""Base exceptions for BudFlow."""


class BudFlowException(Exception):
    """Base exception for all BudFlow errors."""
    pass


class ConfigurationError(BudFlowException):
    """Raised when there's a configuration error."""
    pass


class ValidationError(BudFlowException):
    """Raised when validation fails."""
    pass


class NotFoundError(BudFlowException):
    """Raised when a resource is not found."""
    pass


class PermissionError(BudFlowException):
    """Raised when permission is denied."""
    pass


class ConflictError(BudFlowException):
    """Raised when there's a conflict."""
    pass


class ServiceUnavailableError(BudFlowException):
    """Raised when a service is unavailable."""
    pass