"""
Custom exception hierarchy for semantic-sync.

All exceptions inherit from SemanticSyncError to allow catching
all application-specific errors with a single except clause.
"""

from typing import Any


class SemanticSyncError(Exception):
    """Base exception for all semantic-sync errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            detail_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({detail_str})"
        return self.message


class ConfigurationError(SemanticSyncError):
    """Raised when configuration is invalid or missing."""

    pass


class AuthenticationError(SemanticSyncError):
    """Raised when authentication fails."""

    def __init__(
        self,
        message: str,
        provider: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if provider:
            details["provider"] = provider
        super().__init__(message, details)
        self.provider = provider


class ConnectionError(SemanticSyncError):
    """Raised when connection to a service fails."""

    def __init__(
        self,
        message: str,
        service: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if service:
            details["service"] = service
        super().__init__(message, details)
        self.service = service


class SyncError(SemanticSyncError):
    """Raised when synchronization fails."""

    def __init__(
        self,
        message: str,
        direction: str | None = None,
        source: str | None = None,
        target: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if direction:
            details["direction"] = direction
        if source:
            details["source"] = source
        if target:
            details["target"] = target
        super().__init__(message, details)
        self.direction = direction
        self.source = source
        self.target = target


class ValidationError(SemanticSyncError):
    """Raised when validation fails."""

    def __init__(
        self,
        message: str,
        field: str | None = None,
        value: Any = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = str(value)
        super().__init__(message, details)
        self.field = field
        self.value = value


class ResourceNotFoundError(SemanticSyncError):
    """Raised when a required resource is not found."""

    def __init__(
        self,
        message: str,
        resource_type: str | None = None,
        resource_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if resource_type:
            details["resource_type"] = resource_type
        if resource_id:
            details["resource_id"] = resource_id
        super().__init__(message, details)
        self.resource_type = resource_type
        self.resource_id = resource_id


class RateLimitError(SemanticSyncError):
    """Raised when API rate limit is exceeded."""

    def __init__(
        self,
        message: str,
        retry_after: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if retry_after:
            details["retry_after_seconds"] = retry_after
        super().__init__(message, details)
        self.retry_after = retry_after


class TransactionError(SemanticSyncError):
    """Raised when a database transaction fails."""

    def __init__(
        self,
        message: str,
        operation: str | None = None,
        rollback_performed: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if operation:
            details["operation"] = operation
        details["rollback_performed"] = rollback_performed
        super().__init__(message, details)
        self.operation = operation
        self.rollback_performed = rollback_performed
