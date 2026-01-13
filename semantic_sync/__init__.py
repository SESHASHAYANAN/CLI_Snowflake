"""
Semantic Sync - Enterprise CLI for Snowflake ↔ Fabric semantic model synchronization.

This package provides production-ready tools for synchronizing semantic models
between Snowflake Semantic Views and Microsoft Fabric Semantic Models.

Key Features:
- Unidirectional sync (Snowflake → Fabric OR Fabric → Snowflake)
- OAuth 2.0 authentication for Microsoft Fabric
- Dry-run mode for safe previews
- Change detection and diff visualization
- Metadata storage for audit trails
"""

from __future__ import annotations


__version__ = "1.0.0"
__author__ = "Platform Engineering Team"

from semantic_sync.utils.exceptions import (
    SemanticSyncError,
    ConfigurationError,
    AuthenticationError,
    ConnectionError,
    SyncError,
    ValidationError,
)

__all__ = [
    "__version__",
    "SemanticSyncError",
    "ConfigurationError",
    "AuthenticationError",
    "ConnectionError",
    "SyncError",
    "ValidationError",
]
