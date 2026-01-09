"""
Semantic model formatter for display and export.

Formats semantic models and changes for human-readable output,
including console display, JSON export, and diff views.
"""

import json
from datetime import datetime
from enum import Enum
from typing import Any

from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
)
from semantic_sync.core.change_detector import Change, ChangeReport, ChangeType
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)


class OutputFormat(str, Enum):
    """Output format options."""

    TABLE = "table"
    JSON = "json"
    YAML = "yaml"
    MARKDOWN = "markdown"


class SemanticFormatter:
    """
    Formats semantic models and change reports for display and export.

    Provides multiple output formats for human consumption and
    machine processing.
    """

    def __init__(
        self,
        output_format: OutputFormat = OutputFormat.TABLE,
        colorize: bool = True,
        verbose: bool = False,
    ) -> None:
        """
        Initialize the formatter.

        Args:
            output_format: The format for output (table, json, yaml, markdown)
            colorize: Whether to use ANSI colors in output
            verbose: Whether to include detailed information
        """
        self._format = output_format
        self._colorize = colorize
        self._verbose = verbose

        # ANSI color codes
        self._colors = {
            "reset": "\033[0m" if colorize else "",
            "bold": "\033[1m" if colorize else "",
            "green": "\033[92m" if colorize else "",
            "red": "\033[91m" if colorize else "",
            "yellow": "\033[93m" if colorize else "",
            "blue": "\033[94m" if colorize else "",
            "cyan": "\033[96m" if colorize else "",
            "dim": "\033[2m" if colorize else "",
        }

    def format_model(self, model: SemanticModel) -> str:
        """
        Format a semantic model for display.

        Args:
            model: The semantic model to format

        Returns:
            Formatted string representation
        """
        if self._format == OutputFormat.JSON:
            return self._format_model_json(model)
        elif self._format == OutputFormat.MARKDOWN:
            return self._format_model_markdown(model)
        else:
            return self._format_model_table(model)

    def format_changes(self, report: ChangeReport) -> str:
        """
        Format a change report for display.

        Args:
            report: The change report to format

        Returns:
            Formatted string representation
        """
        if self._format == OutputFormat.JSON:
            return self._format_changes_json(report)
        elif self._format == OutputFormat.MARKDOWN:
            return self._format_changes_markdown(report)
        else:
            return self._format_changes_table(report)

    def format_diff(self, change: Change) -> str:
        """
        Format a single change as a diff view.

        Args:
            change: The change to format

        Returns:
            Diff-formatted string
        """
        c = self._colors
        lines = []

        header = f"{c['bold']}{change.entity_type.upper()}: {change.entity_name}{c['reset']}"
        if change.parent_entity:
            header = f"{c['bold']}{change.entity_type.upper()}: {change.parent_entity}.{change.entity_name}{c['reset']}"

        lines.append(header)

        if change.change_type == ChangeType.ADDED:
            lines.append(f"  {c['green']}+ Added{c['reset']}")
            if change.new_value and self._verbose:
                for key, value in change.new_value.items():
                    lines.append(f"    {c['green']}+ {key}: {value}{c['reset']}")

        elif change.change_type == ChangeType.REMOVED:
            lines.append(f"  {c['red']}- Removed{c['reset']}")
            if change.old_value and self._verbose:
                for key, value in change.old_value.items():
                    lines.append(f"    {c['red']}- {key}: {value}{c['reset']}")

        elif change.change_type == ChangeType.MODIFIED:
            lines.append(f"  {c['yellow']}~ Modified{c['reset']}")
            if change.details:
                for key, diff in change.details.items():
                    if isinstance(diff, dict) and "old" in diff and "new" in diff:
                        lines.append(f"    {c['red']}- {key}: {diff['old']}{c['reset']}")
                        lines.append(f"    {c['green']}+ {key}: {diff['new']}{c['reset']}")
                    else:
                        lines.append(f"    ~ {key}: {diff}")

        return "\n".join(lines)

    def _format_model_table(self, model: SemanticModel) -> str:
        """Format model as ASCII table."""
        c = self._colors
        lines = []

        # Header
        lines.append(f"{c['bold']}Semantic Model: {model.name}{c['reset']}")
        lines.append(f"Source: {model.source}")
        if model.description:
            lines.append(f"Description: {model.description}")
        lines.append(f"Extracted: {model.extracted_at.isoformat()}")
        lines.append("")

        # Tables
        lines.append(f"{c['bold']}Tables ({model.table_count()}){c['reset']}")
        lines.append("-" * 60)

        for table in model.tables:
            hidden = f" {c['dim']}[hidden]{c['reset']}" if table.is_hidden else ""
            lines.append(f"  {c['cyan']}{table.name}{c['reset']}{hidden}")
            if table.description:
                lines.append(f"    {c['dim']}{table.description[:60]}{c['reset']}")

            # Columns
            for col in table.columns:
                col_hidden = f" {c['dim']}[hidden]{c['reset']}" if col.is_hidden else ""
                nullable = "NULL" if col.is_nullable else "NOT NULL"
                lines.append(
                    f"      {col.name}: {c['blue']}{col.data_type}{c['reset']} "
                    f"{c['dim']}{nullable}{c['reset']}{col_hidden}"
                )

            lines.append("")

        # Measures
        if model.measures:
            lines.append(f"{c['bold']}Measures ({model.measure_count()}){c['reset']}")
            lines.append("-" * 60)
            for measure in model.measures:
                hidden = f" {c['dim']}[hidden]{c['reset']}" if measure.is_hidden else ""
                lines.append(f"  {c['yellow']}{measure.name}{c['reset']}{hidden}")
                if self._verbose and measure.expression:
                    expr = measure.expression[:80]
                    lines.append(f"    {c['dim']}{expr}...{c['reset']}")
            lines.append("")

        # Relationships
        if model.relationships:
            lines.append(f"{c['bold']}Relationships ({model.relationship_count()}){c['reset']}")
            lines.append("-" * 60)
            for rel in model.relationships:
                active = "" if rel.is_active else f" {c['dim']}[inactive]{c['reset']}"
                lines.append(
                    f"  {rel.from_table}.{rel.from_column} "
                    f"{c['bold']}->{c['reset']} "
                    f"{rel.to_table}.{rel.to_column}"
                    f" {c['dim']}({rel.cardinality}){c['reset']}{active}"
                )
            lines.append("")

        return "\n".join(lines)

    def _format_model_json(self, model: SemanticModel) -> str:
        """Format model as JSON."""
        return json.dumps(model.model_dump(), indent=2, default=str)

    def _format_model_markdown(self, model: SemanticModel) -> str:
        """Format model as Markdown."""
        lines = []

        lines.append(f"# Semantic Model: {model.name}")
        lines.append("")
        lines.append(f"- **Source**: {model.source}")
        if model.description:
            lines.append(f"- **Description**: {model.description}")
        lines.append(f"- **Extracted**: {model.extracted_at.isoformat()}")
        lines.append("")

        # Tables
        lines.append(f"## Tables ({model.table_count()})")
        lines.append("")

        for table in model.tables:
            hidden = " `[hidden]`" if table.is_hidden else ""
            lines.append(f"### {table.name}{hidden}")
            if table.description:
                lines.append(f"_{table.description}_")
            lines.append("")

            # Columns table
            lines.append("| Column | Type | Nullable | Description |")
            lines.append("|--------|------|----------|-------------|")
            for col in table.columns:
                nullable = "Yes" if col.is_nullable else "No"
                desc = col.description[:30] if col.description else "-"
                lines.append(f"| {col.name} | {col.data_type} | {nullable} | {desc} |")
            lines.append("")

        # Measures
        if model.measures:
            lines.append(f"## Measures ({model.measure_count()})")
            lines.append("")
            for measure in model.measures:
                lines.append(f"### {measure.name}")
                if measure.description:
                    lines.append(f"_{measure.description}_")
                lines.append("")
                lines.append(f"```dax\n{measure.expression}\n```")
                lines.append("")

        # Relationships
        if model.relationships:
            lines.append(f"## Relationships ({model.relationship_count()})")
            lines.append("")
            lines.append("| From | To | Cardinality | Active |")
            lines.append("|------|-----|-------------|--------|")
            for rel in model.relationships:
                active = "Yes" if rel.is_active else "No"
                lines.append(
                    f"| {rel.from_table}.{rel.from_column} | "
                    f"{rel.to_table}.{rel.to_column} | "
                    f"{rel.cardinality} | {active} |"
                )
            lines.append("")

        return "\n".join(lines)

    def _format_changes_table(self, report: ChangeReport) -> str:
        """Format change report as ASCII table."""
        c = self._colors
        lines = []

        # Header
        lines.append(f"{c['bold']}Change Report{c['reset']}")
        lines.append(f"Source: {report.source} -> Target: {report.target}")
        lines.append(f"Generated: {report.generated_at.isoformat()}")
        lines.append("")

        # Summary
        summary = report.summary()
        lines.append(f"{c['bold']}Summary{c['reset']}")
        lines.append("-" * 40)
        lines.append(f"  {c['green']}Additions:{c['reset']}    {summary['added']}")
        lines.append(f"  {c['yellow']}Modifications:{c['reset']} {summary['modified']}")
        lines.append(f"  {c['red']}Removals:{c['reset']}      {summary['removed']}")
        lines.append(f"  {c['bold']}Total:{c['reset']}         {summary['total']}")
        lines.append("")

        if not report.has_changes:
            lines.append(f"{c['green']}No changes detected. Models are in sync.{c['reset']}")
            return "\n".join(lines)

        # Detailed changes
        lines.append(f"{c['bold']}Changes{c['reset']}")
        lines.append("-" * 60)

        # Group by entity type
        entity_types = ["table", "column", "measure", "relationship"]
        for entity_type in entity_types:
            entity_changes = [
                change for change in report.changes
                if change.entity_type == entity_type and change.change_type != ChangeType.UNCHANGED
            ]

            if entity_changes:
                lines.append(f"\n{c['cyan']}{entity_type.upper()}S{c['reset']}")
                for change in entity_changes:
                    lines.append(self.format_diff(change))
                    lines.append("")

        return "\n".join(lines)

    def _format_changes_json(self, report: ChangeReport) -> str:
        """Format change report as JSON."""
        return json.dumps(report.to_dict(), indent=2, default=str)

    def _format_changes_markdown(self, report: ChangeReport) -> str:
        """Format change report as Markdown."""
        lines = []

        lines.append("# Change Report")
        lines.append("")
        lines.append(f"- **Source**: {report.source}")
        lines.append(f"- **Target**: {report.target}")
        lines.append(f"- **Generated**: {report.generated_at.isoformat()}")
        lines.append("")

        # Summary
        summary = report.summary()
        lines.append("## Summary")
        lines.append("")
        lines.append(f"- [+] Additions: **{summary['added']}**")
        lines.append(f"- [~] Modifications: **{summary['modified']}**")
        lines.append(f"- [-] Removals: **{summary['removed']}**")
        lines.append(f"- **Total Changes: {summary['total']}**")
        lines.append("")

        if not report.has_changes:
            lines.append("> [OK] No changes detected. Models are in sync.")
            return "\n".join(lines)

        # Detailed changes
        lines.append("## Detailed Changes")
        lines.append("")

        # Additions
        if report.additions:
            lines.append("### Additions [+]")
            lines.append("")
            for change in report.additions:
                prefix = f"{change.parent_entity}." if change.parent_entity else ""
                lines.append(f"- **{change.entity_type}**: `{prefix}{change.entity_name}`")
            lines.append("")

        # Modifications
        if report.modifications:
            lines.append("### Modifications [~]")
            lines.append("")
            for change in report.modifications:
                prefix = f"{change.parent_entity}." if change.parent_entity else ""
                lines.append(f"- **{change.entity_type}**: `{prefix}{change.entity_name}`")
                if change.details:
                    for key, diff in change.details.items():
                        if isinstance(diff, dict):
                            lines.append(f"  - {key}: `{diff.get('old')}` → `{diff.get('new')}`")
            lines.append("")

        # Removals
        if report.removals:
            lines.append("### Removals [-]")
            lines.append("")
            for change in report.removals:
                prefix = f"{change.parent_entity}." if change.parent_entity else ""
                lines.append(f"- **{change.entity_type}**: `{prefix}{change.entity_name}`")
            lines.append("")

        return "\n".join(lines)

    def print_model(self, model: SemanticModel) -> None:
        """Print formatted model to console."""
        print(self.format_model(model))

    def print_changes(self, report: ChangeReport) -> None:
        """Print formatted change report to console."""
        print(self.format_changes(report))

    def save_model(self, model: SemanticModel, filepath: str) -> None:
        """Save formatted model to file."""
        content = self.format_model(model)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Model saved to {filepath}")

    def save_changes(self, report: ChangeReport, filepath: str) -> None:
        """Save formatted change report to file."""
        content = self.format_changes(report)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Change report saved to {filepath}")
