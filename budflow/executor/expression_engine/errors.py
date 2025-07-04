"""Expression engine error classes."""

from typing import Any, Dict, Optional


class ExpressionError(Exception):
    """Base class for all expression evaluation errors."""
    
    def __init__(
        self,
        message: str,
        expression: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        error_code: Optional[str] = None,
        **kwargs
    ):
        super().__init__(message)
        self.message = message
        self.expression = expression
        self.context = context
        self.error_code = error_code
        self.details = kwargs
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "expression": self.expression,
            "error_code": self.error_code,
            "details": self.details,
        }


class ExpressionSyntaxError(ExpressionError):
    """Raised when expression has syntax errors."""
    
    def __init__(self, message: str, expression: str, line: int = None, column: int = None, **kwargs):
        super().__init__(message, expression, error_code="SYNTAX_ERROR", **kwargs)
        self.line = line
        self.column = column


class ExpressionRuntimeError(ExpressionError):
    """Raised when expression evaluation fails at runtime."""
    
    def __init__(self, message: str, expression: str = None, **kwargs):
        super().__init__(message, expression, error_code="RUNTIME_ERROR", **kwargs)


class ExpressionSecurityError(ExpressionError):
    """Raised when expression violates security policies."""
    
    def __init__(self, message: str, expression: str = None, **kwargs):
        super().__init__(message, expression, error_code="SECURITY_ERROR", **kwargs)


class ExpressionTimeoutError(ExpressionError):
    """Raised when expression evaluation times out."""
    
    def __init__(self, message: str, expression: str = None, timeout_seconds: float = None, **kwargs):
        super().__init__(message, expression, error_code="TIMEOUT_ERROR", **kwargs)
        self.timeout_seconds = timeout_seconds


class ExpressionTypeError(ExpressionError):
    """Raised when expression has type-related errors."""
    
    def __init__(
        self,
        message: str,
        expression: str = None,
        expected_type: str = None,
        actual_type: str = None,
        **kwargs
    ):
        super().__init__(message, expression, error_code="TYPE_ERROR", **kwargs)
        self.expected_type = expected_type
        self.actual_type = actual_type


class UndefinedVariableError(ExpressionError):
    """Raised when expression references undefined variables."""
    
    def __init__(self, message: str, expression: str = None, variable_name: str = None, **kwargs):
        super().__init__(message, expression, error_code="UNDEFINED_VARIABLE", **kwargs)
        self.variable_name = variable_name


class InvalidFilterError(ExpressionError):
    """Raised when expression uses invalid filters."""
    
    def __init__(self, message: str, expression: str = None, filter_name: str = None, **kwargs):
        super().__init__(message, expression, error_code="INVALID_FILTER", **kwargs)
        self.filter_name = filter_name


class InvalidFunctionError(ExpressionError):
    """Raised when expression uses invalid functions."""
    
    def __init__(self, message: str, expression: str = None, function_name: str = None, **kwargs):
        super().__init__(message, expression, error_code="INVALID_FUNCTION", **kwargs)
        self.function_name = function_name