"""
Semantic Sync CLI - Command-line interface for semantic model synchronization.

Provides commands for syncing semantic models between Snowflake and Fabric.
"""

from __future__ import annotations


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


# ============================================================================
# SemaBridge Fabric->Snowflake Metadata Sync Command
# ============================================================================

SEMABRIDGE_BANNER = r"""
+===========================================================================+
|   SemaBridge: Fabric -> Snowflake                                         |
|   ----------------------------------------------------------------------- |
|                                                                           |
|   Full-lifecycle semantic model synchronization                           |
|                                                                           |
|   REST API approach: Metadata-only sync, no XMLA required                 |
+===========================================================================+
"""


@cli.command("fabric-to-sf")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Simulate sync without applying changes",
)
@click.option(
    "--all",
    "sync_all",
    is_flag=True,
    help="Sync ALL datasets in the workspace",
)
@click.option(
    "--mode",
    "-m",
    type=click.Choice(["full", "metadata-only"]),
    default="metadata-only",
    help="Sync mode (default: metadata-only)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Save sync report to file",
)
@click.pass_context
def fabric_to_snowflake(
    ctx: click.Context,
    dry_run: bool,
    sync_all: bool,
    mode: str,
    output: Optional[Path],
) -> None:
    """
    SemaBridge: Sync Fabric semantic model to Snowflake.
    
    This command uses REST API only - no XMLA endpoint required.
    Metadata (tables, columns, measures, relationships) is synced 
    to Snowflake metadata tables.
    
    Examples:
    
        # Preview what would be synced (dry run)
        semantic-sync fabric-to-sf --dry-run
        
        # Sync specific dataset (configured in .env)
        semantic-sync fabric-to-sf
        
        # Sync ALL datasets in workspace
        semantic-sync fabric-to-sf --all
        
        # Full metadata sync with report
        semantic-sync fabric-to-sf --mode full -o sync_report.json
    """
    logger = get_logger(__name__)
    
    click.echo(SEMABRIDGE_BANNER)
    
    try:
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        snowflake_config = settings.get_snowflake_config()
        
        # Show configuration summary
        click.echo("[Configuration]")
        click.echo("-" * 50)
        click.echo(f"  Source (Fabric):")
        click.echo(f"    Workspace:  {fabric_config.workspace_id}")
        if not sync_all:
             click.echo(f"    Dataset:    {fabric_config.dataset_id or '(none)'}")
        else:
             click.echo(f"    Dataset:    [ALL DATASETS] (Ignoring configured FABRIC_DATASET_ID)")
             
        click.echo(f"  Target (Snowflake):")
        click.echo(f"    Account:    {snowflake_config.account}")
        click.echo(f"    Database:   {snowflake_config.database}")
        click.echo(f"    Schema:     {snowflake_config.schema_name}")
        click.echo()
        
        # Create updater
        updater = SemanticUpdater(
            fabric_config=fabric_config,
            snowflake_config=snowflake_config,
        )
        
        sync_mode = SyncMode.METADATA_ONLY if mode == "metadata-only" else SyncMode.FULL
        
        action = "[DRY RUN]" if dry_run else "[SYNCING]"
        click.echo(f"{action} Fabric -> Snowflake (mode: {mode})")
        click.echo("-" * 50)
        
        if sync_all:
            # Sync ALL datasets
            results = updater.sync_all_workspace_datasets(
                mode=sync_mode,
                dry_run=dry_run,
            )
            
            # Summary for multiple datasets
            click.echo()
            click.echo("[Workspace Sync Summary]")
            click.echo("-" * 50)
            success_count = sum(1 for r in results if r.success)
            fail_count = len(results) - success_count
            
            click.echo(f"Total Datasets: {len(results)}")
            click.echo(f"Successful:     {success_count}")
            click.echo(f"Failed:         {fail_count}")
            
            if success_count > 0 and not dry_run:
                click.echo("\nZero-gravity transmission complete for multiple models!")
            
            # Save consolidated report
            if output:
                 import json
                 report = {
                     "summary": {
                         "total": len(results),
                         "success": success_count,
                         "failed": fail_count
                     },
                     "details": [r.to_dict() for r in results]
                 }
                 with open(output, "w") as f:
                     json.dump(report, f, indent=2)
                 click.echo(f"\nReport saved to: {output}")
            
            sys.exit(0 if fail_count == 0 else 1)
            
        else:
            # Single dataset sync (original behavior)
            if not fabric_config.dataset_id:
                click.echo("[ERROR] No Dataset ID configured. Use --all to sync workspace or set FABRIC_DATASET_ID.", err=True)
                sys.exit(1)
                
            # Perform sync
            result = updater.sync(
                direction=SyncDirection.FABRIC_TO_SNOWFLAKE,
                mode=sync_mode,
                dry_run=dry_run,
            )
            
            # Display results
            click.echo()
            click.echo("[Results]")
            click.echo("-" * 50)
            
            if result.success:
                status_symbol = "[OK]" if not dry_run else "[PREVIEW]"
                status_text = "Sync Successful!" if not dry_run else "Dry Run Complete"
                click.echo(f"{status_symbol} {status_text}")
            else:
                click.echo(f"[FAIL] Sync Failed: {result.error_message}", err=True)
                
            click.echo(f"  Source Model:    {result.source_model}")
            click.echo(f"  Target:          {result.target_model}")
            click.echo(f"  Changes Applied: {result.changes_applied}")
            click.echo(f"  Changes Skipped: {result.changes_skipped}")
            click.echo(f"  Errors:          {result.errors}")
            click.echo(f"  Duration:        {result.duration_seconds:.2f}s")
            
            # Save report if requested
            if output:
                import json
                with open(output, "w") as f:
                    json.dump(result.to_dict(), f, indent=2)
                click.echo(f"\nReport saved to: {output}")
                
            # Summary
            if result.success and not dry_run:
                click.echo()
                click.echo("=" * 50)
                click.echo("Zero-gravity transmission complete!")
                click.echo("Semantic metadata has landed in Snowflake.")
                click.echo("=" * 50)
                
            sys.exit(0 if result.success else 1)
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        click.echo(f"\n[ERROR] {e}", err=True)
        sys.exit(1)


@cli.command("sf-to-fabric")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Simulate sync without applying changes",
)
@click.option(
    "--mode",
    "-m",
    type=click.Choice(["full", "incremental"]),
    default="incremental",
    help="Sync mode (default: incremental)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Save sync report to file",
)
@click.pass_context  
def snowflake_to_fabric(
    ctx: click.Context,
    dry_run: bool,
    mode: str,
    output: Optional[Path],
) -> None:
    """
    SemaBridge: Sync Snowflake schema to Fabric Push API dataset.
    
    This command reads schema from Snowflake and updates/creates 
    tables in a Fabric Push API dataset.
    
    Examples:
    
        # Preview what would be synced
        semantic-sync sf-to-fabric --dry-run
        
        # Perform incremental sync
        semantic-sync sf-to-fabric
        
        # Full sync with report
        semantic-sync sf-to-fabric --mode full -o sync_report.json
    """
    logger = get_logger(__name__)
    
    click.echo(SEMABRIDGE_BANNER.replace("Fabric -> Snowflake", "Snowflake -> Fabric"))
    
    try:
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        snowflake_config = settings.get_snowflake_config()
        
        click.echo("[Configuration]")
        click.echo("-" * 50)
        click.echo(f"  Source (Snowflake):")
        click.echo(f"    Account:    {snowflake_config.account}")
        click.echo(f"    Database:   {snowflake_config.database}")
        click.echo(f"    Schema:     {snowflake_config.schema_name}")
        click.echo(f"  Target (Fabric):")
        click.echo(f"    Workspace:  {fabric_config.workspace_id}")
        click.echo(f"    Dataset:    {fabric_config.dataset_id}")
        click.echo()
        
        updater = SemanticUpdater(
            fabric_config=fabric_config,
            snowflake_config=snowflake_config,
        )
        
        sync_mode = SyncMode.INCREMENTAL if mode == "incremental" else SyncMode.FULL
        
        action = "[DRY RUN]" if dry_run else "[SYNCING]"
        click.echo(f"{action} Snowflake -> Fabric (mode: {mode})")
        click.echo("-" * 50)
        
        result = updater.sync(
            direction=SyncDirection.SNOWFLAKE_TO_FABRIC,
            mode=sync_mode,
            dry_run=dry_run,
        )
        
        click.echo()
        click.echo("[Results]")
        click.echo("-" * 50)
        
        if result.success:
            status_symbol = "[OK]" if not dry_run else "[PREVIEW]"
            status_text = "Sync Successful!" if not dry_run else "Dry Run Complete"
            click.echo(f"{status_symbol} {status_text}")
        else:
            click.echo(f"[FAIL] Sync Failed: {result.error_message}", err=True)
            
        click.echo(f"  Source Model:    {result.source_model}")
        click.echo(f"  Target:          {result.target_model}")
        click.echo(f"  Changes Applied: {result.changes_applied}")
        click.echo(f"  Changes Skipped: {result.changes_skipped}")
        click.echo(f"  Errors:          {result.errors}")
        click.echo(f"  Duration:        {result.duration_seconds:.2f}s")
        
        if output:
            import json
            with open(output, "w") as f:
                json.dump(result.to_dict(), f, indent=2)
            click.echo(f"\nReport saved to: {output}")
            
        if result.success and not dry_run:
            click.echo()
            click.echo("=" * 50)
            click.echo("Zero-gravity transmission complete!")
            click.echo("Snowflake schema has landed in Fabric.")
            click.echo("=" * 50)
            
        sys.exit(0 if result.success else 1)
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        click.echo(f"\n[ERROR] {e}", err=True)
        sys.exit(1)


# ============================================================================
# Snapshot/Rollback Commands
# ============================================================================

@cli.group()
def snapshot() -> None:
    """
    Manage semantic model snapshots for rollback capability.
    
    Snapshots allow you to save the current state of a semantic model
    before sync operations, enabling rollback if needed.
    
    Examples:
    
        # Create a snapshot before sync
        semantic-sync snapshot create
        
        # List all snapshots
        semantic-sync snapshot list
        
        # Restore from the latest snapshot
        semantic-sync snapshot restore --latest
        
        # Restore from a specific snapshot
        semantic-sync snapshot restore --id <snapshot-id>
    """
    pass


@snapshot.command("create")
@click.option(
    "--source",
    "-s",
    type=click.Choice(["fabric", "snowflake"]),
    default="fabric",
    help="Source to snapshot (default: fabric)",
)
@click.option(
    "--description",
    "-d",
    type=str,
    help="Description for this snapshot",
)
@click.pass_context
def snapshot_create(ctx: click.Context, source: str, description: str | None) -> None:
    """
    Create a snapshot of the current semantic model.
    
    Saves the current state to the repository for later restoration.
    """
    from semantic_sync.core.duckdb_repository import get_repository
    
    logger = get_logger(__name__)
    
    try:
        settings = get_settings()
        repo = get_repository()
        
        click.echo(f"Creating snapshot from {source}...")
        
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
        
        # Serialize model to dict
        model_data = {
            "name": model.name,
            "source": model.source,
            "tables": [t.__dict__ for t in model.tables] if hasattr(model.tables[0], '__dict__') else model.tables,
            "measures": [m.__dict__ for m in model.measures] if model.measures and hasattr(model.measures[0], '__dict__') else model.measures,
            "relationships": [r.__dict__ for r in model.relationships] if model.relationships and hasattr(model.relationships[0], '__dict__') else model.relationships,
        }
        
        # Create snapshot
        snapshot_id = repo.create_snapshot(
            model_name=model.name,
            source=source,
            model_data=model_data,
            description=description,
            tables_count=len(model.tables),
            columns_count=sum(len(t.columns) for t in model.tables),
            measures_count=len(model.measures),
        )
        
        click.echo()
        click.echo("=" * 50)
        click.echo("[OK] Snapshot created successfully!")
        click.echo("=" * 50)
        click.echo(f"  Snapshot ID:  {snapshot_id}")
        click.echo(f"  Model:        {model.name}")
        click.echo(f"  Source:       {source}")
        click.echo(f"  Tables:       {len(model.tables)}")
        click.echo(f"  Measures:     {len(model.measures)}")
        if description:
            click.echo(f"  Description:  {description}")
        click.echo("=" * 50)
        
    except Exception as e:
        logger.error(f"Snapshot creation failed: {e}")
        click.echo(f"\n[ERROR] {e}", err=True)
        sys.exit(1)


@snapshot.command("list")
@click.option(
    "--limit",
    "-n",
    type=int,
    default=10,
    help="Maximum number of snapshots to show",
)
@click.option(
    "--model",
    "-m",
    type=str,
    help="Filter by model name",
)
def snapshot_list(limit: int, model: str | None) -> None:
    """
    List available snapshots.
    
    Shows recent snapshots that can be restored.
    """
    from semantic_sync.core.duckdb_repository import get_repository
    
    repo = get_repository()
    snapshots = repo.list_snapshots(limit=limit, model_name=model)
    
    if not snapshots:
        click.echo("No snapshots found.")
        return
    
    click.echo()
    click.echo("=" * 80)
    click.echo(" AVAILABLE SNAPSHOTS")
    click.echo("=" * 80)
    click.echo(f"{'ID':<38} {'Model':<20} {'Tables':<8} {'Created':<20}")
    click.echo("-" * 80)
    
    for snap in snapshots:
        created = snap.created_at.strftime("%Y-%m-%d %H:%M")
        click.echo(f"{snap.snapshot_id[:36]:<38} {snap.model_name[:18]:<20} {snap.tables_count:<8} {created:<20}")
        if snap.description:
            click.echo(f"    └─ {snap.description}")
    
    click.echo("=" * 80)
    click.echo(f"Total: {len(snapshots)} snapshot(s)")


@snapshot.command("restore")
@click.option(
    "--id",
    "snapshot_id",
    type=str,
    help="Snapshot ID to restore",
)
@click.option(
    "--latest",
    is_flag=True,
    help="Restore the most recent snapshot",
)
@click.option(
    "--apply",
    is_flag=True,
    help="Apply the snapshot to the target system (Snowflake)",
)
@click.option(
    "--target",
    type=click.Choice(["snowflake"]),
    default="snowflake",
    help="Target to apply the snapshot to (default: snowflake)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be restored without applying",
)
@click.pass_context
def snapshot_restore(
    ctx: click.Context,
    snapshot_id: str | None,
    latest: bool,
    apply: bool,
    target: str,
    dry_run: bool,
) -> None:
    """
    Restore a semantic model from a snapshot.
    
    Use --latest for the most recent snapshot, or --id for a specific one.
    Add --apply to actually push the snapshot back to the target system.
    
    Examples:
        # Just view snapshot data
        semantic-sync snapshot restore --latest
        
        # Apply snapshot to Snowflake
        semantic-sync snapshot restore --id <snapshot-id> --apply
        
        # Preview what would be applied
        semantic-sync snapshot restore --latest --apply --dry-run
    """
    from semantic_sync.core.duckdb_repository import get_repository
    
    logger = get_logger(__name__)
    
    if not snapshot_id and not latest:
        click.echo("Error: Please specify --id or --latest", err=True)
        sys.exit(1)
    
    try:
        repo = get_repository()
        
        # Get snapshot
        if latest:
            result = repo.get_latest_snapshot()
            if not result:
                click.echo("Error: No snapshots available", err=True)
                sys.exit(1)
            snap_info, model_data = result
            snapshot_id = snap_info.snapshot_id
            click.echo(f"Using latest snapshot: {snapshot_id}")
        else:
            result = repo.get_snapshot(snapshot_id)
            if not result:
                click.echo(f"Error: Snapshot not found: {snapshot_id}", err=True)
                sys.exit(1)
            snap_info, model_data = result
        
        click.echo(f"\nRestoring from snapshot {snapshot_id}...")
        
        click.echo()
        click.echo("=" * 50)
        click.echo("[OK] Snapshot data retrieved!")
        click.echo("=" * 50)
        click.echo(f"  Model:         {snap_info.model_name}")
        click.echo(f"  Source:        {snap_info.source}")
        click.echo(f"  Tables:        {snap_info.tables_count}")
        click.echo(f"  Columns:       {snap_info.columns_count}")
        click.echo(f"  Measures:      {snap_info.measures_count}")
        click.echo("=" * 50)
        
        if apply:
            if dry_run:
                click.echo("\n[DRY RUN] Would apply the following changes:")
                _show_apply_preview(model_data)
                click.echo("\nNo changes applied (dry run mode)")
            else:
                click.echo(f"\nApplying snapshot to {target}...")
                _apply_snapshot_to_snowflake(model_data, snap_info)
                click.echo("\n[OK] Snapshot applied successfully!")
        else:
            click.echo("\nModel data retrieved. Use --apply to push to target.")
        
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        click.echo(f"\n[ERROR] {e}", err=True)
        sys.exit(1)


def _show_apply_preview(model_data: dict) -> None:
    """Show preview of what would be applied."""
    tables = model_data.get("tables", [])
    click.echo(f"  Tables to restore: {len(tables)}")
    for table in tables[:5]:
        name = table.get("name", table.get("table_name", "Unknown"))
        click.echo(f"    - {name}")
    if len(tables) > 5:
        click.echo(f"    ... and {len(tables) - 5} more")


def _apply_snapshot_to_snowflake(model_data: dict, snap_info) -> None:
    """Apply snapshot data back to Snowflake."""
    import snowflake.connector
    
    settings = get_settings()
    snowflake_config = settings.get_snowflake_config()
    
    # Extract password from SecretStr if needed
    password = snowflake_config.password
    if hasattr(password, 'get_secret_value'):
        password = password.get_secret_value()
    
    click.echo("  Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=snowflake_config.account,
        user=snowflake_config.user,
        password=password,
        warehouse=snowflake_config.warehouse,
        database=snowflake_config.database,
        role=snowflake_config.role or "SYSADMIN",  # Use SYSADMIN for DDL operations
    )
    cursor = conn.cursor()
    
    try:
        # Set the database and schema context
        cursor.execute(f"USE DATABASE {snowflake_config.database}")
        cursor.execute(f"USE SCHEMA {snowflake_config.schema_name}")
        
        tables = model_data.get("tables", [])
        restored_count = 0
        
        for table in tables:
            table_name = table.get("name", table.get("table_name"))
            if not table_name:
                continue
            
            # Get columns from snapshot
            columns = table.get("columns", [])
            if not columns:
                click.echo(f"  Skipping {table_name} (no column data)")
                continue
            
            # Build column definitions
            col_defs = []
            for col in columns:
                # Handle both dict and string representations
                if isinstance(col, dict):
                    col_name = col.get("name", col.get("column_name"))
                    col_type = col.get("data_type", col.get("dataType", "VARCHAR"))
                elif isinstance(col, str):
                    # Parse string format: name='ORDER_ID' data_type='NUMBER' ...
                    col_name = _extract_attr_from_repr(col, "name")
                    col_type = _extract_attr_from_repr(col, "data_type")
                else:
                    continue
                
                if col_name:
                    # Map common types
                    snowflake_type = _map_to_snowflake_type(col_type)
                    # Quote column name to handle reserved words/spaces
                    col_defs.append(f'"{col_name}" {snowflake_type}')
            
            if col_defs:
                # Create or replace table
                # Quote table name
                create_sql = f'CREATE OR REPLACE TABLE "{table_name}" (\n  {",\n  ".join(col_defs)}\n)'
                click.echo(f"  Restoring table: {table_name} ({len(col_defs)} columns)")
                cursor.execute(create_sql)
                restored_count += 1
        
        click.echo(f"\n  Restored {restored_count} table(s) to Snowflake")
        
    finally:
        cursor.close()
        conn.close()


def _extract_attr_from_repr(repr_str: str, attr_name: str) -> str | None:
    """Extract attribute value from a repr string like: name='value' data_type='NUMBER'."""
    import re
    pattern = rf"{attr_name}='([^']*)'"
    match = re.search(pattern, repr_str)
    return match.group(1) if match else None


def _map_to_snowflake_type(data_type: str) -> str:
    """Map data types to Snowflake types."""
    if not data_type:
        return "VARCHAR"
    
    data_type = str(data_type).upper()
    
    # Common mappings
    type_map = {
        "STRING": "VARCHAR",
        "INT64": "INTEGER",
        "INT32": "INTEGER",
        "DOUBLE": "FLOAT",
        "DECIMAL": "NUMBER",
        "BOOLEAN": "BOOLEAN",
        "DATETIME": "TIMESTAMP",
        "DATE": "DATE",
        "TIME": "TIME",
    }
    
    for key, value in type_map.items():
        if key in data_type:
            return value
    
    # Already a Snowflake type or close enough
    if any(t in data_type for t in ["VARCHAR", "INTEGER", "FLOAT", "NUMBER", "TIMESTAMP"]):
        return data_type
    
    return "VARCHAR"


@snapshot.command("cleanup")
@click.option(
    "--keep",
    "-k",
    type=int,
    default=10,
    help="Number of recent snapshots to keep",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Skip confirmation",
)
def snapshot_cleanup(keep: int, force: bool) -> None:
    """
    Remove old snapshots, keeping only the most recent ones.
    """
    from semantic_sync.core.duckdb_repository import get_repository
    
    repo = get_repository()
    snapshots = repo.list_snapshots(limit=100)
    
    if len(snapshots) <= keep:
        click.echo(f"Only {len(snapshots)} snapshot(s) exist. Nothing to clean up.")
        return
    
    to_delete = len(snapshots) - keep
    
    if not force:
        click.echo(f"This will delete {to_delete} old snapshot(s), keeping the {keep} most recent.")
        if not click.confirm("Continue?"):
            click.echo("Cancelled.")
            return
    
    deleted = repo.cleanup_old_snapshots(keep_last=keep)
    click.echo(f"[OK] Deleted {deleted} old snapshot(s).")

# ============================================================================
# Repository Commands (DuckDB-based)
# ============================================================================

@cli.group()
def repo() -> None:
    """
    Manage the embedded repository (projects, runs, artifacts).
    
    The repository stores project/run metadata, artifacts, and version
    information using DuckDB for queryability and persistence.
    
    Examples:
    
        # List all projects
        semantic-sync repo list-projects
        
        # List runs with filters
        semantic-sync repo list-runs --status success
        semantic-sync repo list-runs --project MyProject
        
        # Show run details
        semantic-sync repo show-run <run-id>
        
        # Get artifacts for a run
        semantic-sync repo get-artifacts <run-id>
        
        # View repository statistics
        semantic-sync repo stats
    """
    pass


@repo.command("list-projects")
@click.option("--limit", default=50, help="Maximum number of projects to show")
def repo_list_projects(limit: int) -> None:
    """List all projects in the repository."""
    from semantic_sync.core.duckdb_repository import get_repository
    
    repository = get_repository()
    projects = repository.list_projects(limit=limit)
    
    if not projects:
        click.echo("No projects found.")
        click.echo("\nTip: Projects are created automatically when you run syncs with --project flag.")
        return
    
    click.echo()
    click.echo(" PROJECTS")
    click.echo("=" * 70)
    click.echo(f"{'Name':<25} {'ID':<38} {'Updated':<20}")
    click.echo("-" * 70)
    
    for proj in projects:
        updated = proj.updated_at.strftime("%Y-%m-%d %H:%M")
        click.echo(f"{proj.name[:24]:<25} {proj.project_id[:36]:<38} {updated:<20}")
    
    click.echo("-" * 70)
    click.echo(f"Total: {len(projects)} project(s)")


@repo.command("list-runs")
@click.option("--project", "-p", help="Filter by project name")
@click.option("--status", "-s", type=click.Choice(["running", "success", "failed", "partial"]), help="Filter by status")
@click.option("--limit", default=20, help="Maximum number of runs to show")
def repo_list_runs(project: str | None, status: str | None, limit: int) -> None:
    """List runs with optional filters."""
    from semantic_sync.core.duckdb_repository import get_repository
    
    repository = get_repository()
    runs = repository.list_runs(project_name=project, status=status, limit=limit)
    
    if not runs:
        click.echo("No runs found.")
        if project:
            click.echo(f"\nNo runs matching project '{project}'")
        if status:
            click.echo(f"\nNo runs with status '{status}'")
        return
    
    click.echo()
    click.echo(" RUNS")
    click.echo("=" * 90)
    click.echo(f"{'Run ID':<38} {'Status':<10} {'Direction':<22} {'Started':<20}")
    click.echo("-" * 90)
    
    for run in runs:
        started = run.started_at.strftime("%Y-%m-%d %H:%M")
        status_display = run.status.upper()
        if run.status == "success":
            status_display = "[OK]"
        elif run.status == "failed":
            status_display = "[FAIL]"
        elif run.status == "running":
            status_display = "[...]"
        
        direction = run.direction or "N/A"
        click.echo(f"{run.run_id[:36]:<38} {status_display:<10} {direction[:20]:<22} {started:<20}")
    
    click.echo("-" * 90)
    click.echo(f"Total: {len(runs)} run(s)")


@repo.command("show-run")
@click.argument("run_id")
def repo_show_run(run_id: str) -> None:
    """Show details of a specific run."""
    from semantic_sync.core.duckdb_repository import get_repository
    
    repository = get_repository()
    run = repository.get_run(run_id)
    
    if not run:
        click.echo(f"Run not found: {run_id}", err=True)
        return
    
    click.echo()
    click.echo(" RUN DETAILS")
    click.echo("=" * 50)
    click.echo(f"  Run ID:          {run.run_id}")
    click.echo(f"  Project ID:      {run.project_id}")
    click.echo(f"  Status:          {run.status}")
    click.echo(f"  Direction:       {run.direction or 'N/A'}")
    click.echo(f"  Source:          {run.source_connector or 'N/A'}")
    click.echo(f"  Target:          {run.target_connector or 'N/A'}")
    click.echo(f"  Started:         {run.started_at}")
    click.echo(f"  Completed:       {run.completed_at or '(still running)'}")
    click.echo(f"  Changes Applied: {run.changes_applied}")
    click.echo(f"  Errors:          {run.errors}")
    if run.error_message:
        click.echo(f"  Error Message:   {run.error_message}")
    
    # Show artifacts
    artifacts = repository.get_artifacts(run_id)
    if artifacts:
        click.echo()
        click.echo(" ARTIFACTS")
        click.echo("-" * 50)
        for art in artifacts:
            click.echo(f"  - {art.artifact_type}: {art.artifact_id[:16]}... ({art.content_hash})")
    
    # Show version metadata
    version = repository.get_version_metadata(run_id)
    if version:
        click.echo()
        click.echo(" VERSION METADATA")
        click.echo("-" * 50)
        click.echo(f"  SemaBridge:      {version.semabridge_version}")
        if version.connector_versions:
            for name, ver in version.connector_versions.items():
                click.echo(f"  {name}:          {ver}")


@repo.command("get-artifacts")
@click.argument("run_id")
@click.option("--type", "-t", "artifact_type", 
              type=click.Choice(["source_format", "sml", "target_format", "log", "report"]),
              help="Filter by artifact type")
@click.option("--output", "-o", type=click.Path(), help="Save artifacts to file")
def repo_get_artifacts(run_id: str, artifact_type: str | None, output: str | None) -> None:
    """Get artifacts for a run."""
    from semantic_sync.core.duckdb_repository import get_repository
    import json
    
    repository = get_repository()
    artifacts = repository.get_artifacts(run_id, artifact_type=artifact_type)
    
    if not artifacts:
        click.echo(f"No artifacts found for run: {run_id}")
        return
    
    if output:
        # Save to file
        data = [art.to_dict() for art in artifacts]
        with open(output, "w") as f:
            json.dump(data, f, indent=2)
        click.echo(f"Saved {len(artifacts)} artifact(s) to {output}")
    else:
        # Display summary
        click.echo()
        click.echo(f" ARTIFACTS FOR RUN {run_id[:16]}...")
        click.echo("=" * 70)
        
        for art in artifacts:
            click.echo()
            click.echo(f"[{art.artifact_type.upper()}]")
            click.echo(f"  ID:      {art.artifact_id}")
            click.echo(f"  Hash:    {art.content_hash}")
            click.echo(f"  Created: {art.created_at}")
            
            # Show content preview
            content_str = json.dumps(art.content, indent=2)
            if len(content_str) > 200:
                click.echo(f"  Content: {content_str[:200]}...")
            else:
                click.echo(f"  Content: {content_str}")


@repo.command("stats")
def repo_stats() -> None:
    """Show repository statistics."""
    from semantic_sync.core.duckdb_repository import get_repository
    
    repository = get_repository()
    stats = repository.get_stats()
    
    click.echo()
    click.echo(" REPOSITORY STATISTICS")
    click.echo("=" * 50)
    click.echo(f"  Database Path:    {stats['database_path']}")
    click.echo(f"  Projects:         {stats['projects']}")
    click.echo(f"  Total Runs:       {stats['total_runs']}")
    click.echo(f"  Successful Runs:  {stats['successful_runs']}")
    click.echo(f"  Failed Runs:      {stats['failed_runs']}")
    click.echo(f"  Artifacts:        {stats['artifacts']}")


@repo.command("cleanup")
@click.option("--older-than", default=30, help="Delete runs older than N days")
@click.option("--keep-successful", is_flag=True, default=True, help="Keep successful runs (default: True)")
@click.option("--force", "-f", is_flag=True, help="Skip confirmation")
def repo_cleanup(older_than: int, keep_successful: bool, force: bool) -> None:
    """Clean up old runs and artifacts."""
    from semantic_sync.core.duckdb_repository import get_repository
    
    repository = get_repository()
    
    if not force:
        msg = f"This will delete runs older than {older_than} days"
        if keep_successful:
            msg += " (keeping successful runs)"
        click.echo(msg)
        if not click.confirm("Continue?"):
            click.echo("Cancelled.")
            return
    
    deleted = repository.cleanup_old_runs(older_than_days=older_than, keep_successful=keep_successful)
    click.echo(f"[OK] Deleted {deleted} old run(s) and their artifacts.")


@repo.command("create-project")
@click.argument("name")
@click.option("--description", "-d", help="Project description")
def repo_create_project(name: str, description: str | None) -> None:
    """Create a new project."""
    from semantic_sync.core.duckdb_repository import get_repository
    
    repository = get_repository()
    
    try:
        project_id = repository.create_project(name, description)
        click.echo(f"[OK] Created project '{name}'")
        click.echo(f"  Project ID: {project_id}")
    except ValueError as e:
        click.echo(f"[ERROR] {e}", err=True)

def main() -> None:
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()

