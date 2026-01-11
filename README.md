# 🌌 SemaBridge / Semantic-Sync

> *"import antigravity"* — Zero-gravity semantic model transmission between Fabric and Snowflake

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## ✨ Overview

**SemaBridge** (aka `semantic-sync`) is a CLI-based semantic model converter that extracts metadata from sources (e.g., Microsoft Fabric/Power BI), normalizes to a canonical SML format, and emits to targets (e.g., Snowflake semantic views).

### Key Features

- 🔄 **Bi-directional sync**: Fabric ↔ Snowflake metadata synchronization
- 🔌 **REST API approach**: No XMLA endpoint required for basic operations
- 📊 **Metadata-only**: Transmits schema, measures, and relationships (not data)
- 🔍 **Dry-run mode**: Preview changes before applying
- 📝 **Audit trail**: Sync history stored in Snowflake metadata tables
- 🎨 **Beautiful CLI**: Rich output with progress and status indicators

---

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
cd semantic-sync

# Install in development mode
pip install -e .

# Or install with dev dependencies
pip install -e ".[dev]"
```

### Configuration

Create a `.env` file in your project directory:

```env
# Fabric Configuration
FABRIC_TENANT_ID=your-tenant-id
FABRIC_CLIENT_ID=your-app-client-id
FABRIC_CLIENT_SECRET=your-client-secret
FABRIC_WORKSPACE_ID=your-workspace-id
FABRIC_DATASET_ID=your-dataset-id

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account.region
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_SEMANTIC_VIEW=semantic_model
```

### Basic Usage

```bash
# 🌌 Sync Fabric semantic model to Snowflake (REST API, metadata-only)
semantic-sync fabric-to-sf

# Preview changes first (dry run)
semantic-sync fabric-to-sf --dry-run

# Sync with full metadata and save report
semantic-sync fabric-to-sf --mode full -o sync_report.json

# 🔄 Sync Snowflake schema to Fabric Push API dataset
semantic-sync sf-to-fabric

# 📋 Show current configuration
semantic-sync config

# ✅ Validate connections
semantic-sync validate

# 📖 Describe a model from either platform
semantic-sync describe --source fabric
semantic-sync describe --source snowflake
```

---

## 📖 Commands Reference

### `fabric-to-sf` — Fabric → Snowflake Sync

Syncs semantic model metadata from Fabric to Snowflake using REST API.

```bash
semantic-sync fabric-to-sf [OPTIONS]

Options:
  --dry-run        Simulate sync without applying changes
  -m, --mode       Sync mode: "full" or "metadata-only" (default)
  -o, --output     Save sync report to file
```

**What gets synced:**
- Table metadata (names, descriptions)
- Column metadata (names, types, descriptions)
- Measures (DAX expressions, descriptions) -> **Converted to SQL Views**
- Relationships (foreign keys, cardinality)

**Where it lands in Snowflake:**
- `_SEMANTIC_METADATA` — Full model JSON
- `_SEMANTIC_MEASURES` — Measure definitions
- `_SEMANTIC_RELATIONSHIPS` — Relationship definitions
- `_SEMANTIC_SYNC_HISTORY` — Audit trail
- Table/Column COMMENTs — Inline documentation
- **SQL Views** — Measures are transpiled to SQL Views (e.g. `Total_Sales`)

### `sf-to-fabric` — Snowflake → Fabric Sync

Syncs Snowflake schema to a Fabric Push API dataset.

```bash
semantic-sync sf-to-fabric [OPTIONS]

Options:
  --dry-run        Simulate sync without applying changes
  -m, --mode       Sync mode: "full" or "incremental" (default)
  -o, --output     Save sync report to file
```

### `sync` — Generic Sync Command

Full-featured sync with all options.

```bash
semantic-sync sync --direction fabric-to-snowflake --mode metadata-only
semantic-sync sync --direction snowflake-to-fabric --mode incremental
```

### `preview` — Preview Changes

Preview what changes would be applied without making any modifications.

```bash
semantic-sync preview --direction fabric-to-snowflake
```

### `describe` — Describe a Model

Read and display the complete semantic model from either platform.

```bash
semantic-sync describe --source fabric --format json
semantic-sync describe --source snowflake --format table
```

### `validate` — Validate Connections

Test connectivity to both Fabric and Snowflake.

```bash
semantic-sync validate
```

### `config` — Show Configuration

Display current configuration (secrets masked).

```bash
semantic-sync config
```

---

## 🏗️ Architecture

```
semantic-sync/
├── semantic_sync/
│   ├── __init__.py
│   ├── main.py                    # CLI entry point
│   ├── auth/                      # OAuth/authentication
│   │   └── oauth.py
│   ├── config/                    # Configuration management
│   │   ├── settings.py
│   │   └── default.yaml
│   ├── core/                      # Core sync logic
│   │   ├── models.py              # Pydantic data models
│   │   ├── fabric_client.py       # Fabric REST API client
│   │   ├── fabric_model_parser.py # Parse Fabric datasets
│   │   ├── snowflake_reader.py    # Read Snowflake schemas
│   │   ├── snowflake_writer.py    # Write to Snowflake
│   │   ├── snowflake_semantic_writer.py  # NEW: Metadata sync
│   │   ├── change_detector.py     # Diff detection
│   │   └── semantic_updater.py    # Orchestration
│   └── utils/                     # Utilities
│       ├── logger.py
│       └── exceptions.py
├── tests/                         # Unit tests
├── pyproject.toml                 # Project configuration
├── .env                           # Environment variables (git-ignored)
└── README.md                      # This file
```

### Sync Flow (Fabric → Snowflake)

```
1. 📥 EXTRACT: Read Fabric dataset via REST API
   └── FabricClient.get_dataset_tables()

2. 🔄 CONVERT: Parse to canonical SemanticModel
   └── FabricModelParser.read_semantic_model()

3. 📤 EMIT: Write metadata to Snowflake
   └── SnowflakeSemanticWriter.sync_semantic_model()
      ├── Create/update metadata tables
      ├── Store measures and relationships
      ├── Apply COMMENTs to tables/columns
      └── Record sync history
```

---

## 📊 Snowflake Metadata Tables

The Fabric→Snowflake sync creates these tables in your target schema:

### `_SEMANTIC_METADATA`

Complete model stored as JSON:

| Column | Type | Description |
|--------|------|-------------|
| MODEL_ID | VARCHAR | Unique model identifier |
| MODEL_NAME | VARCHAR | Model display name |
| SOURCE_SYSTEM | VARCHAR | Origin system (fabric/snowflake) |
| TABLE_COUNT | INTEGER | Number of tables |
| MODEL_JSON | VARIANT | Full model as JSON |
| SYNC_VERSION | INTEGER | Version counter |

### `_SEMANTIC_MEASURES`

DAX measure definitions:

| Column | Type | Description |
|--------|------|-------------|
| MEASURE_ID | VARCHAR | Unique measure identifier |
| MEASURE_NAME | VARCHAR | Display name |
| TABLE_NAME | VARCHAR | Parent table |
| EXPRESSION | TEXT | DAX expression |
| DESCRIPTION | TEXT | Documentation |

### `_SEMANTIC_RELATIONSHIPS`

Table relationships:

| Column | Type | Description |
|--------|------|-------------|
| RELATIONSHIP_ID | VARCHAR | Unique identifier |
| FROM_TABLE | VARCHAR | Source table |
| FROM_COLUMN | VARCHAR | Source column |
| TO_TABLE | VARCHAR | Target table |
| TO_COLUMN | VARCHAR | Target column |
| CARDINALITY | VARCHAR | Relationship type |

### `_SEMANTIC_SYNC_HISTORY`

Audit trail:

| Column | Type | Description |
|--------|------|-------------|
| SYNC_ID | VARCHAR | Unique sync operation ID |
| RUN_ID | VARCHAR | Run identifier |
| STARTED_AT | TIMESTAMP | Sync start time |
| STATUS | VARCHAR | success/partial/failed |
| CHANGES_APPLIED | INTEGER | Number of changes |

---

## 🔧 Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=semantic_sync --cov-report=html

# Run specific test file
pytest tests/test_models.py
```

### Code Quality

```bash
# Format code
black semantic_sync/

# Lint
ruff check semantic_sync/

# Type checking
mypy semantic_sync/
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

---

## 📜 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 🎨 Fun Facts

> *"In the spirit of [XKCD 353](https://xkcd.com/353/), we believe semantic model sync should feel like `import antigravity` — effortless, magical, and slightly amusing."*

The ASCII art banners in this tool are inspired by the "zero-gravity" feeling of watching metadata flow seamlessly between platforms. 🌌

---

Made with ❤️ by the Platform Engineering Team
