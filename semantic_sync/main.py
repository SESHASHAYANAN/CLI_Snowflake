"""
Semantic Sync CLI - Command-line interface for semantic model synchronization.

Provides commands for syncing semantic models between Snowflake and Fabric.
"""

import sys
from pathlib import Path
from typing import Optional

import click

# Attempt to import core modules, handle gracefully if dependencies missing
try:
    from semantic_sync.core import (
        ChangeDetector,
        SemanticFormatter,
        SemanticUpdater,
    )
    from semantic_sync.core.semantic_formatter import OutputFormat
    from semantic_sync.core.semantic_updater import SyncDirection, SyncMode
    from semantic_sync.config import get_settings
    from semantic_sync.utils.logger import get_logger, setup_logging
    IMPORTS_AVAILABLE = True
except ImportError as e:
    IMPORTS_AVAILABLE = False
    IMPORT_ERROR = str(e)


@click.group()
@click.version_option(version="1.0.0", prog_name="semantic-sync")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug logging",
)
@click.pass_context
def cli(ctx: click.Context, config: Optional[Path], verbose: bool, debug: bool) -> None:
    """
    Semantic Sync - Enterprise CLI for Snowflake <-> Fabric semantic model synchronization.

    Synchronize semantic models between Microsoft Fabric and Snowflake with
    change detection, dry-run mode, and audit trails.

    Examples:

        # Preview changes from Fabric to Snowflake
        semantic-sync preview --direction fabric-to-snowflake

        # Sync from Fabric to Snowflake (dry run)
        semantic-sync sync --direction fabric-to-snowflake --dry-run

        # Sync with specific mode
        semantic-sync sync --direction snowflake-to-fabric --mode incremental

        # Validate connections
        semantic-sync validate
    """
    if not IMPORTS_AVAILABLE:
        click.echo(f"Error: Missing dependencies. {IMPORT_ERROR}", err=True)
        click.echo("Please install required packages: pip install -e .", err=True)
        sys.exit(1)

    ctx.ensure_object(dict)

    # Setup logging
    log_level = "DEBUG" if debug else ("INFO" if verbose else "WARNING")
    setup_logging(level=log_level)

    # Load configuration
    if config:
        ctx.obj["config_path"] = config

    ctx.obj["verbose"] = verbose
    ctx.obj["debug"] = debug


@cli.command()
@click.option(
    "--direction",
    "-d",
    type=click.Choice(["fabric-to-snowflake", "snowflake-to-fabric"]),
    required=True,
    help="Direction of synchronization",
)
@click.option(
    "--format",
    "-f",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format",
)
@click.pass_context
def preview(ctx: click.Context, direction: str, output_format: str) -> None:
    """
    Preview changes without applying them.

    Compares source and target semantic models and displays the differences
    that would be synchronized.
    """
    logger = get_logger(__name__)
    verbose = ctx.obj.get("verbose", False)

    try:
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        snowflake_config = settings.get_snowflake_config()

        updater = SemanticUpdater(
            fabric_config=fabric_config,
            snowflake_config=snowflake_config,
        )

        sync_direction = SyncDirection(direction)
        click.echo(f"Previewing changes for {direction}...")

        change_report = updater.preview_changes(sync_direction)

        # Format and display
        formatter = SemanticFormatter(
            output_format=OutputFormat(output_format),
            colorize=True,
            verbose=verbose,
        )

        click.echo()
        formatter.print_changes(change_report)

        # Exit with status based on whether changes exist
        if change_report.has_changes:
            summary = change_report.summary()
            click.echo(f"\nTotal changes: {summary['total']}")
            sys.exit(0)
        else:
            click.echo("\n[OK] Models are in sync. No changes needed.")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Preview failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--direction",
    "-d",
    type=click.Choice(["fabric-to-snowflake", "snowflake-to-fabric"]),
    required=True,
    help="Direction of synchronization",
)
@click.option(
    "--mode",
    "-m",
    type=click.Choice(["full", "incremental", "metadata-only"]),
    default="incremental",
    help="Sync mode",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Simulate sync without applying changes",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Skip confirmation prompt",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Save sync report to file",
)
@click.pass_context
def sync(
    ctx: click.Context,
    direction: str,
    mode: str,
    dry_run: bool,
    force: bool,
    output: Optional[Path],
) -> None:
    """
    Synchronize semantic models between platforms.

    Detects changes between source and target models, then applies
    the necessary updates to bring them in sync.
    """
    logger = get_logger(__name__)

    try:
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        snowflake_config = settings.get_snowflake_config()

        updater = SemanticUpdater(
            fabric_config=fabric_config,
            snowflake_config=snowflake_config,
        )

        sync_direction = SyncDirection(direction)
        sync_mode = SyncMode(mode)

        # Preview changes first
        click.echo(f"Analyzing changes for {direction}...")
        change_report = updater.preview_changes(sync_direction)

        if not change_report.has_changes:
            click.echo("\n[OK] Models are already in sync. No changes to apply.")
            sys.exit(0)

        # Show summary
        summary = change_report.summary()
        click.echo(f"\nChanges detected:")
        click.echo(f"  [+] Additions:     {summary['added']}")
        click.echo(f"  [~] Modifications: {summary['modified']}")
        click.echo(f"  [-] Removals:      {summary['removed']}")
        click.echo(f"  Total:            {summary['total']}")

        # Confirm unless dry-run or force
        if not dry_run and not force:
            click.echo()
            if not click.confirm("Do you want to apply these changes?"):
                click.echo("Sync cancelled.")
                sys.exit(0)

        # Perform sync
        action = "Simulating" if dry_run else "Applying"
        click.echo(f"\n{action} changes...")

        result = updater.sync(
            direction=sync_direction,
            mode=sync_mode,
            dry_run=dry_run,
        )

        # Display results
        if result.success:
            status = "[OK] Sync completed successfully" if not dry_run else "[OK] Dry run completed"
            click.echo(f"\n{status}")
        else:
            click.echo(f"\n[FAIL] Sync failed: {result.error_message}", err=True)

        click.echo(f"  Changes applied: {result.changes_applied}")
        click.echo(f"  Changes skipped: {result.changes_skipped}")
        click.echo(f"  Errors:          {result.errors}")
        click.echo(f"  Duration:        {result.duration_seconds:.2f}s")

        # Save report if requested
        if output:
            import json
            with open(output, "w") as f:
                json.dump(result.to_dict(), f, indent=2)
            click.echo(f"\nReport saved to: {output}")

        sys.exit(0 if result.success else 1)

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def validate(ctx: click.Context) -> None:
    """
    Validate connections to Fabric and Snowflake.

    Tests authentication and connectivity for both platforms.
    """
    logger = get_logger(__name__)

    try:
        settings = get_settings()

        click.echo("Validating connections...\n")

        # Validate Fabric
        click.echo("Testing Fabric connection...")
        try:
            fabric_config = settings.get_fabric_config()
            from semantic_sync.core import FabricClient
            client = FabricClient(fabric_config)
            client.validate_connection()
            click.echo("  [OK] Fabric connection successful")
            fabric_ok = True
        except Exception as e:
            click.echo(f"  [FAIL] Fabric connection failed: {e}", err=True)
            fabric_ok = False

        # Validate Snowflake
        click.echo("\nTesting Snowflake connection...")
        try:
            snowflake_config = settings.get_snowflake_config()
            from semantic_sync.core import SnowflakeReader
            reader = SnowflakeReader(snowflake_config)
            reader.test_connection()
            click.echo("  [OK] Snowflake connection successful")
            snowflake_ok = True
        except Exception as e:
            click.echo(f"  [FAIL] Snowflake connection failed: {e}", err=True)
            snowflake_ok = False

        # Summary
        click.echo("\n" + "=" * 40)
        if fabric_ok and snowflake_ok:
            click.echo("[OK] All connections validated successfully!")
            sys.exit(0)
        else:
            click.echo("[FAIL] Some connections failed. Please check your configuration.")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--source",
    "-s",
    type=click.Choice(["fabric", "snowflake"]),
    required=True,
    help="Source platform to read from",
)
@click.option(
    "--format",
    "-f",
    "output_format",
    type=click.Choice(["table", "json", "markdown"]),
    default="table",
    help="Output format",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Save model to file",
)
@click.pass_context
def describe(
    ctx: click.Context,
    source: str,
    output_format: str,
    output: Optional[Path],
) -> None:
    """
    Describe a semantic model from either platform.

    Reads and displays the complete semantic model definition.
    """
    logger = get_logger(__name__)
    verbose = ctx.obj.get("verbose", False)

    try:
        settings = get_settings()

        click.echo(f"Reading semantic model from {source}...")

        if source == "fabric":
            fabric_config = settings.get_fabric_config()
            from semantic_sync.core import FabricClient, FabricModelParser
            client = FabricClient(fabric_config)
            parser = FabricModelParser(client, fabric_config)
            model = parser.read_semantic_model()
        else:
            snowflake_config = settings.get_snowflake_config()
            from semantic_sync.core import SnowflakeReader
            reader = SnowflakeReader(snowflake_config)
            model = reader.read_semantic_view()

        # Format and display
        formatter = SemanticFormatter(
            output_format=OutputFormat(output_format),
            colorize=True,
            verbose=verbose,
        )

        click.echo()
        formatter.print_model(model)

        # Save if requested
        if output:
            formatter.save_model(model, str(output))
            click.echo(f"\nModel saved to: {output}")

    except Exception as e:
        logger.error(f"Describe failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
def config() -> None:
    """
    Show current configuration.

    Displays the active configuration settings (with secrets masked).
    """
    try:
        settings = get_settings()

        click.echo("Current Configuration")
        click.echo("=" * 40)

        # Fabric config
        click.echo("\n[Fabric]")
        click.echo(f"  Tenant ID:     {settings.fabric_tenant_id or '(not set)'}")
        click.echo(f"  Client ID:     {settings.fabric_client_id or '(not set)'}")
        secret = settings.fabric_client_secret.get_secret_value() if settings.fabric_client_secret else ""
        click.echo(f"  Client Secret: {'********' if secret else '(not set)'}")
        click.echo(f"  Workspace ID:  {settings.fabric_workspace_id or '(not set)'}")
        click.echo(f"  Dataset ID:    {settings.fabric_dataset_id or '(not set)'}")

        # Snowflake config
        click.echo("\n[Snowflake]")
        click.echo(f"  Account:       {settings.snowflake_account or '(not set)'}")
        click.echo(f"  User:          {settings.snowflake_user or '(not set)'}")
        sf_password = settings.snowflake_password.get_secret_value() if settings.snowflake_password else ""
        click.echo(f"  Password:      {'********' if sf_password else '(not set)'}")
        click.echo(f"  Database:      {settings.snowflake_database or '(not set)'}")
        click.echo(f"  Schema:        {settings.snowflake_schema or '(not set)'}")
        click.echo(f"  Warehouse:     {settings.snowflake_warehouse or '(not set)'}")
        click.echo(f"  Semantic View: {settings.snowflake_semantic_view or '(not set)'}")

    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        sys.exit(1)


def main() -> None:
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
