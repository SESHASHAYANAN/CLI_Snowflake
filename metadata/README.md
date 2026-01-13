# Metadata Registry

This directory contains manual metadata definitions for semantic models that cannot be read via standard APIs.

## File Format

Each model has its own YAML file named `{model_name}.yaml` with the following structure:

```yaml
description: Model description
tables:
  - name: TableName
    description: Table description
    columns:
      - name: column_name
        dataType: String|Int64|Double|DateTime|Boolean
        description: Column description
        isNullable: true|false
```

## Supported Data Types

- `String` - Text data
- `Int64` - Integer numbers
- `Double` - Decimal numbers
- `DateTime` - Date and time values
- `Boolean` - True/false values

## How It Works

1. When the semantic sync tool cannot read a model via REST API or DMV queries
2. It falls back to checking this metadata directory
3. If a matching `{model_name}.yaml` file exists, it uses that metadata
4. The metadata is then synced to Snowflake as if it came from Fabric

## Current Definitions

- `new_rep.yaml` - Sales Representatives dataset

## Adding New Definitions

Create a new YAML file in this directory following the format above, then run the sync command.
