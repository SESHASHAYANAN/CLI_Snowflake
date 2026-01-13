"""
Configuration settings for semantic-sync.

Uses Pydantic for validation and environment variable loading.
Supports YAML configuration files with environment variable overrides.
"""

from __future__ import annotations


import os
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from semantic_sync.utils.exceptions import ConfigurationError


class SyncDirection(str, Enum):
    """Valid sync directions."""

    SF_TO_FABRIC = "sf-to-fabric"
    FABRIC_TO_SF = "fabric-to-sf"

    @classmethod
    def from_string(cls, value: str) -> "SyncDirection":
        """Parse direction from string."""
        normalized = value.lower().strip()
        for direction in cls:
            if direction.value == normalized:
                return direction
        valid = ", ".join(d.value for d in cls)
        raise ValueError(f"Invalid direction '{value}'. Valid options: {valid}")

    @property
    def source_system(self) -> str:
        """Get source system name."""
        return "Snowflake" if self == SyncDirection.SF_TO_FABRIC else "Fabric"

    @property
    def target_system(self) -> str:
        """Get target system name."""
        return "Fabric" if self == SyncDirection.SF_TO_FABRIC else "Snowflake"


class SnowflakeConfig(BaseModel):
    """Snowflake connection configuration."""

    account: str = Field(..., description="Snowflake account identifier")
    user: str = Field(..., description="Snowflake username")
    password: SecretStr = Field(..., description="Snowflake password")
    warehouse: str = Field(..., description="Snowflake warehouse")
    database: str = Field(..., description="Snowflake database")
    schema_name: str = Field(default="PUBLIC", alias="schema", description="Snowflake schema")
    role: str | None = Field(default=None, description="Snowflake role")
    semantic_view_name: str = Field(..., description="Name of the semantic view")

    model_config = {"populate_by_name": True}

    @field_validator("account")
    @classmethod
    def validate_account(cls, v: str) -> str:
        """Validate Snowflake account format."""
        if not v or not v.strip():
            raise ValueError("Snowflake account cannot be empty")
        return v.strip()

    def get_connection_params(self) -> dict[str, Any]:
        """Get connection parameters for Snowflake connector."""
        params = {
            "account": self.account,
            "user": self.user,
            "password": self.password.get_secret_value(),
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema_name,
        }
        if self.role:
            params["role"] = self.role
        return params


class FabricConfig(BaseModel):
    """Microsoft Fabric configuration."""

    tenant_id: str = Field(..., description="Azure AD tenant ID")
    client_id: str = Field(..., description="Azure AD application client ID")
    client_secret: SecretStr = Field(..., description="Azure AD application client secret")
    workspace_id: str = Field(..., description="Fabric workspace ID")
    dataset_id: str = Field(..., description="Fabric semantic model (dataset) ID")
    api_base_url: str = Field(
        default="https://api.powerbi.com/v1.0/myorg",
        description="Power BI REST API base URL",
    )

    @field_validator("tenant_id", "client_id", "workspace_id", "dataset_id")
    @classmethod
    def validate_guid(cls, v: str, info: Any) -> str:
        """Validate GUID format."""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty")
        return v.strip()

    @property
    def token_scopes(self) -> list[str]:
        """Get OAuth token scopes for Fabric API."""
        return ["https://analysis.windows.net/powerbi/api/.default"]


class SyncConfig(BaseModel):
    """Sync behavior configuration."""

    batch_size: int = Field(default=100, ge=1, le=10000, description="Batch size for sync")
    timeout_seconds: int = Field(default=300, ge=30, le=3600, description="Operation timeout")
    max_retries: int = Field(default=3, ge=0, le=10, description="Max retry attempts")
    retry_delay_seconds: int = Field(default=5, ge=1, le=60, description="Delay between retries")
    store_metadata: bool = Field(default=True, description="Store sync metadata")
    metadata_path: str = Field(
        default=".semantic-sync/metadata",
        description="Path for metadata storage",
    )


class Settings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    # Snowflake settings (can also be from env vars)
    snowflake_account: str = Field(default="", alias="SNOWFLAKE_ACCOUNT")
    snowflake_user: str = Field(default="", alias="SNOWFLAKE_USER")
    snowflake_password: SecretStr = Field(default=SecretStr(""), alias="SNOWFLAKE_PASSWORD")
    snowflake_warehouse: str = Field(default="", alias="SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = Field(default="", alias="SNOWFLAKE_DATABASE")
    snowflake_schema: str = Field(default="PUBLIC", alias="SNOWFLAKE_SCHEMA")
    snowflake_role: str | None = Field(default=None, alias="SNOWFLAKE_ROLE")
    snowflake_semantic_view: str = Field(default="", alias="SNOWFLAKE_SEMANTIC_VIEW")

    # Fabric settings (from env vars)
    fabric_tenant_id: str = Field(default="", alias="FABRIC_TENANT_ID")
    fabric_client_id: str = Field(default="", alias="FABRIC_CLIENT_ID")
    fabric_client_secret: SecretStr = Field(default=SecretStr(""), alias="FABRIC_CLIENT_SECRET")
    fabric_workspace_id: str = Field(default="", alias="FABRIC_WORKSPACE_ID")
    fabric_dataset_id: str = Field(default="", alias="FABRIC_DATASET_ID")

    # Sync settings
    sync_batch_size: int = Field(default=100)
    sync_timeout: int = Field(default=300)
    sync_max_retries: int = Field(default=3)

    # Logging
    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=False)

    def get_snowflake_config(self) -> SnowflakeConfig:
        """Build SnowflakeConfig from settings."""
        return SnowflakeConfig(
            account=self.snowflake_account,
            user=self.snowflake_user,
            password=self.snowflake_password,
            warehouse=self.snowflake_warehouse,
            database=self.snowflake_database,
            schema=self.snowflake_schema,
            role=self.snowflake_role,
            semantic_view_name=self.snowflake_semantic_view,
        )

    def get_fabric_config(self) -> FabricConfig:
        """Build FabricConfig from settings."""
        return FabricConfig(
            tenant_id=self.fabric_tenant_id,
            client_id=self.fabric_client_id,
            client_secret=self.fabric_client_secret,
            workspace_id=self.fabric_workspace_id,
            dataset_id=self.fabric_dataset_id,
        )

    def get_sync_config(self) -> SyncConfig:
        """Build SyncConfig from settings."""
        return SyncConfig(
            batch_size=self.sync_batch_size,
            timeout_seconds=self.sync_timeout,
            max_retries=self.sync_max_retries,
        )


# Global settings instance
_settings: Settings | None = None


def load_settings(config_path: str | Path | None = None) -> Settings:
    """
    Load settings from YAML file and environment variables.

    Args:
        config_path: Optional path to YAML configuration file.
                    Environment variables always take precedence.

    Returns:
        Settings instance with merged configuration.
    """
    global _settings

    # Start with defaults
    config_data: dict[str, Any] = {}

    # Load from YAML if provided
    if config_path:
        path = Path(config_path)
        if path.exists():
            with open(path) as f:
                yaml_config = yaml.safe_load(f)
                if yaml_config:
                    config_data = _flatten_config(yaml_config)

    # Environment variables override YAML
    # Pydantic handles this automatically through Settings

    try:
        if config_data:
            _settings = Settings(**config_data)
        else:
            _settings = Settings()
        return _settings
    except Exception as e:
        raise ConfigurationError(f"Failed to load settings: {e}") from e


def get_settings() -> Settings:
    """Get the current settings instance, loading if necessary."""
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


def _flatten_config(config: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten nested YAML config to match Settings field names."""
    result: dict[str, Any] = {}

    for key, value in config.items():
        if isinstance(value, dict):
            # Handle nested sections like 'snowflake', 'fabric', 'sync'
            nested_prefix = f"{key}_" if not prefix else f"{prefix}{key}_"
            result.update(_flatten_config(value, nested_prefix))
        else:
            full_key = f"{prefix}{key}" if prefix else key
            result[full_key] = value

    return result
