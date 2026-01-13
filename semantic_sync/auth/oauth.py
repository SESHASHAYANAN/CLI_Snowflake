"""
OAuth 2.0 authentication for Microsoft Fabric.

Implements Client Credentials flow with token caching and automatic refresh.
Uses MSAL (Microsoft Authentication Library) for secure token management.
"""

from __future__ import annotations


import json
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import msal

from semantic_sync.config.settings import FabricConfig
from semantic_sync.utils.exceptions import AuthenticationError
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)


class TokenCache:
    """
    Thread-safe token cache with filesystem persistence.

    Stores tokens securely and handles expiration checks.
    """

    def __init__(self, cache_path: str | Path | None = None) -> None:
        """
        Initialize token cache.

        Args:
            cache_path: Optional path for persistent cache storage.
                       Defaults to ~/.semantic-sync/.token_cache
        """
        self._lock = threading.RLock()
        self._cache: dict[str, Any] = {}

        if cache_path:
            self._cache_path = Path(cache_path)
        else:
            self._cache_path = Path.home() / ".semantic-sync" / ".token_cache"

        self._load_cache()

    def _load_cache(self) -> None:
        """Load cache from filesystem."""
        try:
            if self._cache_path.exists():
                with open(self._cache_path) as f:
                    self._cache = json.load(f)
                    logger.debug("Token cache loaded from disk")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load token cache: {e}")
            self._cache = {}

    def _save_cache(self) -> None:
        """Persist cache to filesystem."""
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._cache_path, "w") as f:
                json.dump(self._cache, f)
            # Secure file permissions (owner only)
            os.chmod(self._cache_path, 0o600)
        except OSError as e:
            logger.warning(f"Failed to save token cache: {e}")

    def get(self, key: str) -> dict[str, Any] | None:
        """
        Get cached token if valid.

        Args:
            key: Cache key (typically client_id or similar identifier)

        Returns:
            Token data dict if valid, None if expired or missing
        """
        with self._lock:
            if key not in self._cache:
                return None

            cached = self._cache[key]
            expires_at = cached.get("expires_at", 0)

            # Check expiration with 5-minute buffer
            if time.time() + 300 > expires_at:
                logger.debug(f"Token for {key} is expired or expiring soon")
                del self._cache[key]
                self._save_cache()
                return None

            return cached

    def set(
        self,
        key: str,
        access_token: str,
        expires_in: int,
        token_type: str = "Bearer",
    ) -> None:
        """
        Store token in cache.

        Args:
            key: Cache key
            access_token: The OAuth access token
            expires_in: Token lifetime in seconds
            token_type: Token type (typically "Bearer")
        """
        with self._lock:
            self._cache[key] = {
                "access_token": access_token,
                "token_type": token_type,
                "expires_at": time.time() + expires_in,
                "cached_at": datetime.utcnow().isoformat(),
            }
            self._save_cache()
            logger.debug(f"Token cached for {key}, expires in {expires_in}s")

    def clear(self, key: str | None = None) -> None:
        """
        Clear cached tokens.

        Args:
            key: Specific key to clear, or None to clear all
        """
        with self._lock:
            if key:
                self._cache.pop(key, None)
            else:
                self._cache = {}
            self._save_cache()


class FabricOAuthClient:
    """
    OAuth 2.0 client for Microsoft Fabric API access.

    Implements Client Credentials flow with automatic token refresh.
    """

    # Power BI API scope
    DEFAULT_SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]

    # OneLake/Storage scope
    STORAGE_SCOPES = ["https://storage.azure.com/.default"]

    def __init__(
        self,
        config: FabricConfig,
        cache: TokenCache | None = None,
        scopes: list[str] | None = None,
    ) -> None:
        """
        Initialize OAuth client.

        Args:
            config: Fabric configuration with credentials
            cache: Optional token cache instance
            scopes: Optional custom scopes (defaults to Power BI API)
        """
        self._config = config
        self._cache = cache or TokenCache()
        self._lock = threading.RLock()
        self._custom_scopes = scopes  # Store custom scopes if provided

        # Initialize MSAL confidential client
        authority = f"https://login.microsoftonline.com/{config.tenant_id}"
        self._msal_app = msal.ConfidentialClientApplication(
            client_id=config.client_id,
            client_credential=config.client_secret.get_secret_value(),
            authority=authority,
        )

        # Use different cache keys for different scopes
        scope_suffix = "_storage" if scopes and "storage" in str(scopes) else ""
        self._cache_key = f"fabric_{config.client_id}{scope_suffix}"

    def get_access_token(self, force_refresh: bool = False) -> str:
        """
        Get a valid access token, refreshing if necessary.

        Args:
            force_refresh: If True, bypass cache and get new token

        Returns:
            Valid access token string

        Raises:
            AuthenticationError: If token acquisition fails
        """
        with self._lock:
            # Check cache first (unless force refresh)
            if not force_refresh:
                cached = self._cache.get(self._cache_key)
                if cached:
                    logger.debug("Using cached access token")
                    return cached["access_token"]

            # Acquire new token
            logger.info("Acquiring new access token from Azure AD")
            try:
                # Use custom scopes if provided, otherwise default
                scopes = self._custom_scopes if self._custom_scopes else self.DEFAULT_SCOPES
                result = self._msal_app.acquire_token_for_client(
                    scopes=scopes
                )
            except Exception as e:
                raise AuthenticationError(
                    f"Token acquisition failed: {e}",
                    provider="Microsoft Entra ID",
                    details={"tenant_id": self._config.tenant_id},
                ) from e

            if "access_token" not in result:
                error = result.get("error", "unknown")
                error_desc = result.get("error_description", "No description")
                raise AuthenticationError(
                    f"Token acquisition failed: {error}",
                    provider="Microsoft Entra ID",
                    details={
                        "error": error,
                        "error_description": error_desc,
                        "tenant_id": self._config.tenant_id,
                    },
                )

            # Cache the token
            token = result["access_token"]
            expires_in = result.get("expires_in", 3600)
            self._cache.set(self._cache_key, token, expires_in)

            logger.info("Successfully acquired access token")
            return token

    def get_authorization_header(self, force_refresh: bool = False) -> dict[str, str]:
        """
        Get HTTP Authorization header with valid Bearer token.

        Args:
            force_refresh: If True, bypass cache and get new token

        Returns:
            Dict with Authorization header
        """
        token = self.get_access_token(force_refresh=force_refresh)
        return {"Authorization": f"Bearer {token}"}

    def refresh_if_needed(self) -> str:
        """
        Refresh token if it's expired or expiring soon.

        Returns:
            Valid access token
        """
        cached = self._cache.get(self._cache_key)
        if cached:
            return cached["access_token"]
        return self.get_access_token(force_refresh=True)

    def clear_cache(self) -> None:
        """Clear cached tokens for this client."""
        self._cache.clear(self._cache_key)
        logger.info("Token cache cleared")

    def validate_credentials(self) -> bool:
        """
        Validate OAuth credentials by attempting token acquisition.

        Returns:
            True if credentials are valid

        Raises:
            AuthenticationError: If credentials are invalid
        """
        try:
            self.get_access_token(force_refresh=True)
            return True
        except AuthenticationError:
            raise
        except Exception as e:
            raise AuthenticationError(
                f"Credential validation failed: {e}",
                provider="Microsoft Entra ID",
                details={"client_id": self._config.client_id},
            ) from e


# Global OAuth client instance
_oauth_client: FabricOAuthClient | None = None


def get_oauth_client(config: FabricConfig | None = None) -> FabricOAuthClient:
    """
    Get or create the global OAuth client instance.

    Args:
        config: Fabric configuration. Required on first call.

    Returns:
        FabricOAuthClient instance

    Raises:
        ConfigurationError: If config is needed but not provided
    """
    global _oauth_client

    if _oauth_client is None:
        if config is None:
            from semantic_sync.config import get_settings
            config = get_settings().get_fabric_config()
        _oauth_client = FabricOAuthClient(config)

    return _oauth_client
