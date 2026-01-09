"""
Logging configuration for semantic-sync.

Provides structured logging with rich console output for CLI interactions
and JSON formatting for production log aggregation.
"""

import logging
import sys
from datetime import datetime
from enum import Enum
from typing import Any

from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme


class LogLevel(str, Enum):
    """Supported log levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# Custom theme for semantic-sync
SEMANTIC_SYNC_THEME = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "red bold",
        "critical": "red bold reverse",
        "success": "green bold",
        "source": "blue",
        "target": "magenta",
        "direction": "cyan bold",
    }
)

# Global console instance
console = Console(theme=SEMANTIC_SYNC_THEME)


class SemanticSyncLogger:
    """Custom logger wrapper with semantic-sync specific formatting."""

    def __init__(self, name: str, level: LogLevel = LogLevel.INFO) -> None:
        self.logger = logging.getLogger(name)
        self._setup_handler(level)

    def _setup_handler(self, level: LogLevel) -> None:
        """Configure the logger with RichHandler."""
        self.logger.setLevel(level.value)

        # Remove existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Add RichHandler for beautiful console output
        rich_handler = RichHandler(
            console=console,
            show_time=True,
            show_path=False,
            markup=True,
            rich_tracebacks=True,
            tracebacks_show_locals=False,
        )
        rich_handler.setLevel(level.value)

        formatter = logging.Formatter("%(message)s", datefmt="[%X]")
        rich_handler.setFormatter(formatter)

        self.logger.addHandler(rich_handler)

    def set_level(self, level: LogLevel) -> None:
        """Update log level."""
        self.logger.setLevel(level.value)
        for handler in self.logger.handlers:
            handler.setLevel(level.value)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self.logger.debug(self._format_message(message, kwargs))

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self.logger.info(self._format_message(message, kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self.logger.warning(self._format_message(message, kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self.logger.error(self._format_message(message, kwargs))

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message."""
        self.logger.critical(self._format_message(message, kwargs))

    def success(self, message: str, **kwargs: Any) -> None:
        """Log success message with special formatting."""
        formatted = self._format_message(message, kwargs)
        console.print(f"[success]✓ {formatted}[/success]")

    def sync_operation(
        self,
        operation: str,
        direction: str,
        source: str,
        target: str,
    ) -> None:
        """Log a sync operation with directional formatting."""
        console.print(
            f"\n[direction]▶ {operation}[/direction]\n"
            f"  [source]Source:[/source] {source}\n"
            f"  [target]Target:[/target] {target}\n"
            f"  [direction]Direction:[/direction] {direction}\n"
        )

    def _format_message(self, message: str, extra: dict[str, Any]) -> str:
        """Format message with extra context."""
        if extra:
            context = " | ".join(f"{k}={v}" for k, v in extra.items())
            return f"{message} [{context}]"
        return message


# Logger cache
_loggers: dict[str, SemanticSyncLogger] = {}


def get_logger(name: str = "semantic-sync") -> SemanticSyncLogger:
    """Get or create a logger instance."""
    if name not in _loggers:
        _loggers[name] = SemanticSyncLogger(name)
    return _loggers[name]


def setup_logging(
    level: LogLevel | str = LogLevel.INFO,
    json_output: bool = False,
) -> None:
    """
    Configure global logging settings.

    Args:
        level: Log level to use (can be LogLevel enum or string like "DEBUG", "INFO")
        json_output: If True, output logs in JSON format (for production)
    """
    # Convert string to LogLevel if needed
    if isinstance(level, str):
        level = LogLevel(level.upper())
    
    root_logger = logging.getLogger()
    root_logger.setLevel(level.value)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    if json_output:
        # JSON handler for production log aggregation
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level.value)

        class JsonFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                import json

                log_entry = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
                if record.exc_info:
                    log_entry["exception"] = self.formatException(record.exc_info)
                return json.dumps(log_entry)

        handler.setFormatter(JsonFormatter())
        root_logger.addHandler(handler)
    else:
        # Rich console handler for development
        rich_handler = RichHandler(
            console=console,
            show_time=True,
            show_path=False,
            markup=True,
            rich_tracebacks=True,
        )
        rich_handler.setLevel(level.value)
        root_logger.addHandler(rich_handler)

    # Update all cached loggers
    for logger in _loggers.values():
        logger.set_level(level)


def print_banner() -> None:
    """Print the semantic-sync banner."""
    console.print(
        """
[cyan bold]╔═══════════════════════════════════════════════════════════╗
║                    SEMANTIC-SYNC                          ║
║         Snowflake ↔ Fabric Semantic Model Sync            ║
╚═══════════════════════════════════════════════════════════╝[/cyan bold]
""",
        highlight=False,
    )
