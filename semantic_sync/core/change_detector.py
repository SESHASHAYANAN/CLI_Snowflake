"""
Change detector for semantic model synchronization.

Compares semantic models to detect additions, modifications, and deletions
between source and target systems.
"""

from __future__ import annotations


from enum import Enum
from typing import Any
from dataclasses import dataclass, field
from datetime import datetime

from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
)
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)


class ChangeType(str, Enum):
    """Types of changes detected between models."""

    ADDED = "added"
    MODIFIED = "modified"
    REMOVED = "removed"
    UNCHANGED = "unchanged"


@dataclass
class Change:
    """Represents a single change between source and target models."""

    change_type: ChangeType
    entity_type: str  # "table", "column", "measure", "relationship"
    entity_name: str
    old_value: dict[str, Any] | None = None
    new_value: dict[str, Any] | None = None
    parent_entity: str | None = None  # For columns, the parent table name
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Human-readable string representation."""
        if self.parent_entity:
            return f"{self.change_type.value.upper()}: {self.entity_type} '{self.parent_entity}.{self.entity_name}'"
        return f"{self.change_type.value.upper()}: {self.entity_type} '{self.entity_name}'"

    def to_dict(self) -> dict[str, Any]:
        """Convert change to dictionary for serialization."""
        return {
            "change_type": self.change_type.value,
            "entity_type": self.entity_type,
            "entity_name": self.entity_name,
            "parent_entity": self.parent_entity,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "details": self.details,
        }


@dataclass
class ChangeReport:
    """Summary report of all detected changes."""

    source: str
    target: str
    changes: list[Change]
    generated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def additions(self) -> list[Change]:
        """Get all additions."""
        return [c for c in self.changes if c.change_type == ChangeType.ADDED]

    @property
    def modifications(self) -> list[Change]:
        """Get all modifications."""
        return [c for c in self.changes if c.change_type == ChangeType.MODIFIED]

    @property
    def removals(self) -> list[Change]:
        """Get all removals."""
        return [c for c in self.changes if c.change_type == ChangeType.REMOVED]

    @property
    def has_changes(self) -> bool:
        """Check if there are any changes."""
        return any(c.change_type != ChangeType.UNCHANGED for c in self.changes)

    def summary(self) -> dict[str, int]:
        """Get summary counts of changes."""
        return {
            "added": len(self.additions),
            "modified": len(self.modifications),
            "removed": len(self.removals),
            "total": len(self.additions) + len(self.modifications) + len(self.removals),
        }

    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary for serialization."""
        return {
            "source": self.source,
            "target": self.target,
            "generated_at": self.generated_at.isoformat(),
            "summary": self.summary(),
            "changes": [c.to_dict() for c in self.changes],
        }


class ChangeDetector:
    """
    Detects changes between source and target semantic models.

    Compares tables, columns, measures, and relationships to identify
    what needs to be synchronized.
    """

    def __init__(
        self,
        ignore_hidden: bool = False,
        case_sensitive: bool = False,
    ) -> None:
        """
        Initialize the change detector.

        Args:
            ignore_hidden: If True, skip hidden entities from comparison
            case_sensitive: If True, entity name comparisons are case-sensitive
        """
        self._ignore_hidden = ignore_hidden
        self._case_sensitive = case_sensitive

    def detect_changes(
        self,
        source: SemanticModel,
        target: SemanticModel,
    ) -> ChangeReport:
        """
        Detect all changes between source and target models.

        Args:
            source: The source semantic model (authoritative)
            target: The target semantic model (to be updated)

        Returns:
            ChangeReport containing all detected changes
        """
        logger.info(
            f"Detecting changes between '{source.name}' (source) "
            f"and '{target.name}' (target)"
        )

        changes: list[Change] = []

        # Detect table changes
        table_changes = self._detect_table_changes(source, target)
        changes.extend(table_changes)

        # Detect measure changes
        measure_changes = self._detect_measure_changes(source, target)
        changes.extend(measure_changes)

        # Detect relationship changes
        relationship_changes = self._detect_relationship_changes(source, target)
        changes.extend(relationship_changes)

        report = ChangeReport(
            source=source.name,
            target=target.name,
            changes=changes,
        )

        summary = report.summary()
        logger.info(
            f"Change detection complete: {summary['added']} additions, "
            f"{summary['modified']} modifications, {summary['removed']} removals"
        )

        return report

    def _normalize_name(self, name: str) -> str:
        """Normalize entity name for comparison."""
        if self._case_sensitive:
            return name
        return name.lower()

    def _detect_table_changes(
        self,
        source: SemanticModel,
        target: SemanticModel,
    ) -> list[Change]:
        """Detect changes in tables and their columns."""
        changes: list[Change] = []

        source_tables = {
            self._normalize_name(t.name): t
            for t in source.tables
            if not (self._ignore_hidden and t.is_hidden)
        }
        target_tables = {
            self._normalize_name(t.name): t
            for t in target.tables
            if not (self._ignore_hidden and t.is_hidden)
        }

        # Tables added in source
        for name, table in source_tables.items():
            if name not in target_tables:
                changes.append(
                    Change(
                        change_type=ChangeType.ADDED,
                        entity_type="table",
                        entity_name=table.name,
                        new_value=table.model_dump(),
                    )
                )
                # All columns in new table are also additions
                for col in table.columns:
                    changes.append(
                        Change(
                            change_type=ChangeType.ADDED,
                            entity_type="column",
                            entity_name=col.name,
                            parent_entity=table.name,
                            new_value=col.model_dump(),
                        )
                    )
            else:
                # Table exists in both - check for modifications
                target_table = target_tables[name]
                table_changes = self._compare_tables(table, target_table)
                changes.extend(table_changes)

        # Tables removed from source (exist in target but not source)
        for name, table in target_tables.items():
            if name not in source_tables:
                changes.append(
                    Change(
                        change_type=ChangeType.REMOVED,
                        entity_type="table",
                        entity_name=table.name,
                        old_value=table.model_dump(),
                    )
                )

        return changes

    def _compare_tables(
        self,
        source_table: SemanticTable,
        target_table: SemanticTable,
    ) -> list[Change]:
        """Compare two tables for modifications."""
        changes: list[Change] = []

        # Check table-level property changes
        table_modified = False
        table_changes = {}

        if source_table.description != target_table.description:
            table_modified = True
            table_changes["description"] = {
                "old": target_table.description,
                "new": source_table.description,
            }

        if source_table.is_hidden != target_table.is_hidden:
            table_modified = True
            table_changes["is_hidden"] = {
                "old": target_table.is_hidden,
                "new": source_table.is_hidden,
            }

        if table_modified:
            changes.append(
                Change(
                    change_type=ChangeType.MODIFIED,
                    entity_type="table",
                    entity_name=source_table.name,
                    old_value=target_table.model_dump(),
                    new_value=source_table.model_dump(),
                    details=table_changes,
                )
            )

        # Compare columns
        column_changes = self._detect_column_changes(
            source_table.columns,
            target_table.columns,
            source_table.name,
        )
        changes.extend(column_changes)

        return changes

    def _detect_column_changes(
        self,
        source_columns: list[SemanticColumn],
        target_columns: list[SemanticColumn],
        table_name: str,
    ) -> list[Change]:
        """Detect changes in columns within a table."""
        changes: list[Change] = []

        source_cols = {
            self._normalize_name(c.name): c
            for c in source_columns
            if not (self._ignore_hidden and c.is_hidden)
        }
        target_cols = {
            self._normalize_name(c.name): c
            for c in target_columns
            if not (self._ignore_hidden and c.is_hidden)
        }

        # Columns added
        for name, col in source_cols.items():
            if name not in target_cols:
                changes.append(
                    Change(
                        change_type=ChangeType.ADDED,
                        entity_type="column",
                        entity_name=col.name,
                        parent_entity=table_name,
                        new_value=col.model_dump(),
                    )
                )
            else:
                # Column exists in both - check for modifications
                target_col = target_cols[name]
                col_change = self._compare_columns(col, target_col, table_name)
                if col_change:
                    changes.append(col_change)

        # Columns removed
        for name, col in target_cols.items():
            if name not in source_cols:
                changes.append(
                    Change(
                        change_type=ChangeType.REMOVED,
                        entity_type="column",
                        entity_name=col.name,
                        parent_entity=table_name,
                        old_value=col.model_dump(),
                    )
                )

        return changes

    def _compare_columns(
        self,
        source_col: SemanticColumn,
        target_col: SemanticColumn,
        table_name: str,
    ) -> Change | None:
        """Compare two columns for modifications."""
        modified = False
        details = {}

        # Compare key properties
        if source_col.data_type != target_col.data_type:
            modified = True
            details["data_type"] = {
                "old": target_col.data_type,
                "new": source_col.data_type,
            }

        if source_col.description != target_col.description:
            modified = True
            details["description"] = {
                "old": target_col.description,
                "new": source_col.description,
            }

        if source_col.is_nullable != target_col.is_nullable:
            modified = True
            details["is_nullable"] = {
                "old": target_col.is_nullable,
                "new": source_col.is_nullable,
            }

        if source_col.is_hidden != target_col.is_hidden:
            modified = True
            details["is_hidden"] = {
                "old": target_col.is_hidden,
                "new": source_col.is_hidden,
            }

        if source_col.format_string != target_col.format_string:
            modified = True
            details["format_string"] = {
                "old": target_col.format_string,
                "new": source_col.format_string,
            }

        if modified:
            return Change(
                change_type=ChangeType.MODIFIED,
                entity_type="column",
                entity_name=source_col.name,
                parent_entity=table_name,
                old_value=target_col.model_dump(),
                new_value=source_col.model_dump(),
                details=details,
            )

        return None

    def _detect_measure_changes(
        self,
        source: SemanticModel,
        target: SemanticModel,
    ) -> list[Change]:
        """Detect changes in measures."""
        changes: list[Change] = []

        source_measures = {
            self._normalize_name(m.name): m
            for m in source.measures
            if not (self._ignore_hidden and m.is_hidden)
        }
        target_measures = {
            self._normalize_name(m.name): m
            for m in target.measures
            if not (self._ignore_hidden and m.is_hidden)
        }

        # Measures added
        for name, measure in source_measures.items():
            if name not in target_measures:
                changes.append(
                    Change(
                        change_type=ChangeType.ADDED,
                        entity_type="measure",
                        entity_name=measure.name,
                        new_value=measure.model_dump(),
                    )
                )
            else:
                # Measure exists in both - check for modifications
                target_measure = target_measures[name]
                measure_change = self._compare_measures(measure, target_measure)
                if measure_change:
                    changes.append(measure_change)

        # Measures removed
        for name, measure in target_measures.items():
            if name not in source_measures:
                changes.append(
                    Change(
                        change_type=ChangeType.REMOVED,
                        entity_type="measure",
                        entity_name=measure.name,
                        old_value=measure.model_dump(),
                    )
                )

        return changes

    def _compare_measures(
        self,
        source_measure: SemanticMeasure,
        target_measure: SemanticMeasure,
    ) -> Change | None:
        """Compare two measures for modifications."""
        modified = False
        details = {}

        if source_measure.expression != target_measure.expression:
            modified = True
            details["expression"] = {
                "old": target_measure.expression,
                "new": source_measure.expression,
            }

        if source_measure.description != target_measure.description:
            modified = True
            details["description"] = {
                "old": target_measure.description,
                "new": source_measure.description,
            }

        if source_measure.format_string != target_measure.format_string:
            modified = True
            details["format_string"] = {
                "old": target_measure.format_string,
                "new": source_measure.format_string,
            }

        if source_measure.is_hidden != target_measure.is_hidden:
            modified = True
            details["is_hidden"] = {
                "old": target_measure.is_hidden,
                "new": source_measure.is_hidden,
            }

        if modified:
            return Change(
                change_type=ChangeType.MODIFIED,
                entity_type="measure",
                entity_name=source_measure.name,
                old_value=target_measure.model_dump(),
                new_value=source_measure.model_dump(),
                details=details,
            )

        return None

    def _detect_relationship_changes(
        self,
        source: SemanticModel,
        target: SemanticModel,
    ) -> list[Change]:
        """Detect changes in relationships."""
        changes: list[Change] = []

        # Build lookup by relationship key (from_table.from_column -> to_table.to_column)
        def relationship_key(rel: SemanticRelationship) -> str:
            return f"{rel.from_table}.{rel.from_column}->{rel.to_table}.{rel.to_column}"

        source_rels = {relationship_key(r): r for r in source.relationships}
        target_rels = {relationship_key(r): r for r in target.relationships}

        # Relationships added
        for key, rel in source_rels.items():
            if key not in target_rels:
                changes.append(
                    Change(
                        change_type=ChangeType.ADDED,
                        entity_type="relationship",
                        entity_name=rel.name,
                        new_value=rel.model_dump(),
                        details={"relationship_key": key},
                    )
                )
            else:
                # Relationship exists - check for modifications
                target_rel = target_rels[key]
                rel_change = self._compare_relationships(rel, target_rel)
                if rel_change:
                    changes.append(rel_change)

        # Relationships removed
        for key, rel in target_rels.items():
            if key not in source_rels:
                changes.append(
                    Change(
                        change_type=ChangeType.REMOVED,
                        entity_type="relationship",
                        entity_name=rel.name,
                        old_value=rel.model_dump(),
                        details={"relationship_key": key},
                    )
                )

        return changes

    def _compare_relationships(
        self,
        source_rel: SemanticRelationship,
        target_rel: SemanticRelationship,
    ) -> Change | None:
        """Compare two relationships for modifications."""
        modified = False
        details = {}

        if source_rel.cardinality != target_rel.cardinality:
            modified = True
            details["cardinality"] = {
                "old": target_rel.cardinality,
                "new": source_rel.cardinality,
            }

        if source_rel.cross_filter_direction != target_rel.cross_filter_direction:
            modified = True
            details["cross_filter_direction"] = {
                "old": target_rel.cross_filter_direction,
                "new": source_rel.cross_filter_direction,
            }

        if source_rel.is_active != target_rel.is_active:
            modified = True
            details["is_active"] = {
                "old": target_rel.is_active,
                "new": source_rel.is_active,
            }

        if modified:
            return Change(
                change_type=ChangeType.MODIFIED,
                entity_type="relationship",
                entity_name=source_rel.name,
                old_value=target_rel.model_dump(),
                new_value=source_rel.model_dump(),
                details=details,
            )

        return None
