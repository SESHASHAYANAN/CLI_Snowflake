"""Core modules for semantic-sync."""

from __future__ import annotations


from semantic_sync.core.snowflake_reader import SnowflakeReader
from semantic_sync.core.snowflake_writer import SnowflakeWriter
from semantic_sync.core.snowflake_semantic_writer import (
    SnowflakeSemanticWriter,
    sync_fabric_to_snowflake,
)
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.core.fabric_xmla_client import FabricXmlaClient
from semantic_sync.core.fabric_model_parser import FabricModelParser
from semantic_sync.core.semantic_formatter import SemanticFormatter
from semantic_sync.core.change_detector import ChangeDetector, ChangeType, Change
from semantic_sync.core.semantic_updater import SemanticUpdater, SyncDirection, SyncMode
from semantic_sync.core.duckdb_repository import (
    Repository,
    ProjectInfo,
    RunInfo,
    ArtifactInfo,
    VersionMetadata,
    SnapshotInfo,
    get_repository,
)
from semantic_sync.core.fabric_snowflake_semantic_pipeline import (
    FabricToSnowflakePipeline,
    SemanticSyncConfig,
    SyncResult as PipelineSyncResult,
    sync_fabric_to_snowflake as sync_pipeline,
)

__all__ = [
    "SnowflakeReader",
    "SnowflakeWriter",
    "SnowflakeSemanticWriter",
    "sync_fabric_to_snowflake",
    "FabricClient",
    "FabricXmlaClient",
    "FabricModelParser",
    "SemanticFormatter",
    "ChangeDetector",
    "ChangeType",
    "Change",
    "SemanticUpdater",
    "SyncDirection",
    "SyncMode",
    # DuckDB Repository (replaces SQLite rollback)
    "Repository",
    "ProjectInfo",
    "RunInfo",
    "ArtifactInfo",
    "VersionMetadata",
    "SnapshotInfo",
    "get_repository",
    # Semantic Pipeline
    "FabricToSnowflakePipeline",
    "SemanticSyncConfig",
    "PipelineSyncResult",
    "sync_pipeline",
]

