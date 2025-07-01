"""Credential-specific exceptions."""

from budflow.exceptions import BudFlowException


class CredentialException(BudFlowException):
    """Base exception for credential-related errors."""
    pass


class CredentialNotFoundError(CredentialException):
    """Raised when a credential is not found."""
    pass


class CredentialAlreadyExistsError(CredentialException):
    """Raised when trying to create a credential that already exists."""
    pass


class CredentialAccessDeniedError(CredentialException):
    """Raised when user doesn't have permission to access a credential."""
    pass


class CredentialEncryptionError(CredentialException):
    """Raised when credential encryption/decryption fails."""
    pass


class InvalidCredentialTypeError(CredentialException):
    """Raised when an invalid credential type is specified."""
    pass


class CredentialTestFailedError(CredentialException):
    """Raised when credential test fails."""
    pass


class OAuthError(CredentialException):
    """Raised when OAuth flow fails."""
    pass