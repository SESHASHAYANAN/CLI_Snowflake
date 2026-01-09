"""
Shared data models for semantic metadata.

Pydantic models representing semantic entities that can be
synchronized between Snowflake and Fabric.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class DataType(str, Enum):
    """Normalized data types across platforms."""

    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIME = "time"
    BINARY = "binary"
    VARIANT = "variant"
    ARRAY = "array"
    OBJECT = "object"
    UNKNOWN = "unknown"

    @classmethod
    def from_snowflake(cls, sf_type: str) -> "DataType":
        """Map Snowflake data type to normalized type."""
        sf_type = sf_type.upper()
        mapping = {
            "VARCHAR": cls.STRING,
            "CHAR": cls.STRING,
            "STRING": cls.STRING,
            "TEXT": cls.STRING,
            "NUMBER": cls.DECIMAL,
            "DECIMAL": cls.DECIMAL,
            "NUMERIC": cls.DECIMAL,
            "INT": cls.INTEGER,
            "INTEGER": cls.INTEGER,
            "BIGINT": cls.INTEGER,
            "SMALLINT": cls.INTEGER,
            "TINYINT": cls.INTEGER,
            "BYTEINT": cls.INTEGER,
            "FLOAT": cls.FLOAT,
            "FLOAT4": cls.FLOAT,
            "FLOAT8": cls.FLOAT,
            "DOUBLE": cls.FLOAT,
            "DOUBLE PRECISION": cls.FLOAT,
            "REAL": cls.FLOAT,
            "BOOLEAN": cls.BOOLEAN,
            "DATE": cls.DATE,
            "DATETIME": cls.DATETIME,
            "TIMESTAMP": cls.DATETIME,
            "TIMESTAMP_LTZ": cls.DATETIME,
            "TIMESTAMP_NTZ": cls.DATETIME,
            "TIMESTAMP_TZ": cls.DATETIME,
            "TIME": cls.TIME,
            "BINARY": cls.BINARY,
            "VARBINARY": cls.BINARY,
            "VARIANT": cls.VARIANT,
            "ARRAY": cls.ARRAY,
            "OBJECT": cls.OBJECT,
        }
        # Handle parameterized types like VARCHAR(255)
        base_type = sf_type.split("(")[0].strip()
        return mapping.get(base_type, cls.UNKNOWN)

    @classmethod
    def from_fabric(cls, fabric_type: str) -> "DataType":
        """Map Fabric/Power BI data type to normalized type."""
        fabric_type = fabric_type.lower()
        mapping = {
            "string": cls.STRING,
            "text": cls.STRING,
            "int64": cls.INTEGER,
            "int32": cls.INTEGER,
            "integer": cls.INTEGER,
            "decimal": cls.DECIMAL,
            "double": cls.FLOAT,
            "float": cls.FLOAT,
            "boolean": cls.BOOLEAN,
            "bool": cls.BOOLEAN,
            "date": cls.DATE,
            "datetime": cls.DATETIME,
            "datetimeoffset": cls.DATETIME,
            "time": cls.TIME,
            "binary": cls.BINARY,
        }
        return mapping.get(fabric_type, cls.UNKNOWN)

    def to_snowflake(self) -> str:
        """Convert normalized type to Snowflake type."""
        mapping = {
            DataType.STRING: "VARCHAR",
            DataType.INTEGER: "INTEGER",
            DataType.DECIMAL: "DECIMAL",
            DataType.FLOAT: "FLOAT",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.DATE: "DATE",
            DataType.DATETIME: "TIMESTAMP",
            DataType.TIME: "TIME",
            DataType.BINARY: "BINARY",
            DataType.VARIANT: "VARIANT",
            DataType.ARRAY: "ARRAY",
            DataType.OBJECT: "OBJECT",
            DataType.UNKNOWN: "VARIANT",
        }
        return mapping.get(self, "VARIANT")

    def to_fabric(self) -> str:
        """Convert normalized type to Fabric type."""
        mapping = {
            DataType.STRING: "String",
            DataType.INTEGER: "Int64",
            DataType.DECIMAL: "Decimal",
            DataType.FLOAT: "Double",
            DataType.BOOLEAN: "Boolean",
            DataType.DATE: "Date",
            DataType.DATETIME: "DateTime",
            DataType.TIME: "Time",
            DataType.BINARY: "Binary",
            DataType.VARIANT: "String",
            DataType.ARRAY: "String",
            DataType.OBJECT: "String",
            DataType.UNKNOWN: "String",
        }
        return mapping.get(self, "String")


class SemanticColumn(BaseModel):
    """Represents a column in a semantic table."""

    name: str = Field(..., description="Column name")
    data_type: str = Field(..., description="Data type (platform-specific)")
    normalized_type: DataType = Field(
        default=DataType.UNKNOWN,
        description="Normalized data type",
    )
    is_nullable: bool = Field(default=True, description="Whether column allows nulls")
    description: str = Field(default="", description="Column description")
    is_hidden: bool = Field(default=False, description="Whether column is hidden")
    format_string: str | None = Field(default=None, description="Display format")
    source_column: str | None = Field(default=None, description="Source column reference")

    def model_post_init(self, __context: Any) -> None:
        """Normalize data type after initialization."""
        if self.normalized_type == DataType.UNKNOWN and self.data_type:
            # Try to infer normalized type
            self.normalized_type = DataType.from_snowflake(self.data_type)


class SemanticMeasure(BaseModel):
    """Represents a measure in a semantic model."""

    name: str = Field(..., description="Measure name")
    expression: str = Field(..., description="DAX/calculation expression")
    description: str = Field(default="", description="Measure description")
    format_string: str | None = Field(default=None, description="Display format")
    is_hidden: bool = Field(default=False, description="Whether measure is hidden")
    folder: str | None = Field(default=None, description="Display folder")
    data_type: str = Field(default="decimal", description="Result data type")
    table_name: str | None = Field(default=None, description="Parent table name")


class SemanticRelationship(BaseModel):
    """Represents a relationship between tables."""

    name: str = Field(..., description="Relationship name")
    from_table: str = Field(..., description="Source table name")
    from_column: str = Field(..., description="Source column name")
    to_table: str = Field(..., description="Target table name")
    to_column: str = Field(..., description="Target column name")
    cardinality: str = Field(
        default="many-to-one",
        description="Relationship cardinality",
    )
    cross_filter_direction: str = Field(
        default="single",
        description="Cross-filter direction (single/both)",
    )
    is_active: bool = Field(default=True, description="Whether relationship is active")


class SemanticTable(BaseModel):
    """Represents a table in a semantic model."""

    name: str = Field(..., description="Table name")
    description: str = Field(default="", description="Table description")
    columns: list[SemanticColumn] = Field(default_factory=list)
    source_table: str | None = Field(default=None, description="Source table reference")
    is_hidden: bool = Field(default=False, description="Whether table is hidden")
    partition_source: str | None = Field(default=None, description="Partition definition")


class SemanticModel(BaseModel):
    """Complete semantic model representation."""

    name: str = Field(..., description="Model name")
    source: str = Field(..., description="Source system (snowflake/fabric)")
    description: str = Field(default="", description="Model description")
    tables: list[SemanticTable] = Field(default_factory=list)
    measures: list[SemanticMeasure] = Field(default_factory=list)
    relationships: list[SemanticRelationship] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    version: str = Field(default="1.0", description="Model version")

    def get_table(self, name: str) -> SemanticTable | None:
        """Get a table by name."""
        for table in self.tables:
            if table.name.lower() == name.lower():
                return table
        return None

    def get_measure(self, name: str) -> SemanticMeasure | None:
        """Get a measure by name."""
        for measure in self.measures:
            if measure.name.lower() == name.lower():
                return measure
        return None

    def get_relationship(self, name: str) -> SemanticRelationship | None:
        """Get a relationship by name."""
        for rel in self.relationships:
            if rel.name.lower() == name.lower():
                return rel
        return None

    def table_count(self) -> int:
        """Get number of tables."""
        return len(self.tables)

    def column_count(self) -> int:
        """Get total number of columns across all tables."""
        return sum(len(t.columns) for t in self.tables)

    def measure_count(self) -> int:
        """Get number of measures."""
        return len(self.measures)

    def relationship_count(self) -> int:
        """Get number of relationships."""
        return len(self.relationships)
