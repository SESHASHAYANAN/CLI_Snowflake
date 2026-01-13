# Automated Fabric to Snowflake Sync

## 🎯 Overview

This system automatically detects new semantic models created in Microsoft Fabric and syncs them to Snowflake **without any manual intervention**.

### Key Features

✅ **Automatic Detection**: Monitors Fabric workspace for new or updated models  
✅ **Smart Conversion**: Auto-converts Import/DirectQuery models to Push API format  
✅ **CLI Integration**: Uses existing `semantic-sync` CLI commands  
✅ **State Tracking**: Remembers what's been synced to avoid duplicates  
✅ **Background Monitoring**: Runs continuously or as scheduled task  

---

## 🔄 How It Works

```
┌─────────────────────────────────────────────────────────────┐
│  1. Create New Model in Fabric                              │
│     (Can be Import, DirectQuery, or Push API)               │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  2. Automated Monitor Detects New Model                     │
│     - Runs every 5 minutes (configurable)                   │
│     - Checks for new/updated models                         │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  3. Auto-Convert to Push API (if needed)                    │
│     - Reads model schema via REST API/XMLA/BIM              │
│     - Converts to Push API JSON format                      │
│     - Creates new "{ModelName}_PushSync" dataset            │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  4. Sync to Snowflake Using CLI                             │
│     - Runs: semantic-sync sync --direction fabric-to-snowflake│
│     - Updates `_SEMANTIC_METADATA` tables in Snowflake      │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  5. Model Available in Snowflake ✅                          │
│     - Tables synced to Snowflake                            │
│     - Metadata available in semantic layer                  │
│     - Ready for querying!                                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Files Overview

### Core Components

| File | Purpose |
|------|---------|
| `model_converter.py` | Converts REST API models to Push API format |
| `automated_sync_monitor.py` | Monitors for new models and triggers sync |
| `test_auto_sync.py` | Test script to verify the workflow |

### Supporting Files

| File | Purpose |
|------|---------|
| `sync_state.json` | Tracks which models have been synced (auto-created) |
| `{ModelName}_push_api.json` | Push API definition for each converted model |

---

## 🚀 Quick Start

### Option 1: One-Time Sync

Convert all existing models and sync to Snowflake:

```bash
# Auto-convert all models without Push API versions
python model_converter.py --auto

# Or convert a specific model
python model_converter.py --dataset-id <ID> --dataset-name "MyModel"
```

### Option 2: Continuous Monitoring

Run the automated monitor in the background:

```bash
# Start the monitor (checks every 5 minutes)
python automated_sync_monitor.py

# Custom interval (e.g., every 2 minutes = 120 seconds)
python automated_sync_monitor.py --interval 120

# Run once and exit (for scheduled tasks)
python automated_sync_monitor.py --once
```

### Option 3: Test the Workflow

Run the test suite to verify everything works:

```bash
python test_auto_sync.py
```

---

## 🔧 Configuration

The system uses your existing `.env` configuration:

```env
# Fabric Configuration
FABRIC_TENANT_ID=your-tenant-id
FABRIC_CLIENT_ID=your-client-id
FABRIC_CLIENT_SECRET=your-client-secret
FABRIC_WORKSPACE_ID=your-workspace-id

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
```

---

## 📊 Example Workflow

### Scenario: You create a new "Sales Analytics" model in Fabric

1. **You create model**: Import mode dataset with tables: `Sales`, `Products`, `Customers`

2. **Monitor detects** (within 5 minutes):
   ```
   🆕 New models detected: 1
      - Sales Analytics (created: 2026-01-13T10:30:00Z)
   ```

3. **Auto-conversion happens**:
   ```
   📋 Converting non-Push API model: Sales Analytics
   ✅ Created Push API dataset: Sales Analytics_PushSync
   ```

4. **Sync to Snowflake**:
   ```
   🚀 Running sync using semantic-sync CLI...
   ✅ Synced to ANALYTICS_DB.SEMANTIC_LAYER
   ```

5. **Verify in Snowflake**:
   ```sql
   SELECT * FROM ANALYTICS_DB.SEMANTIC_LAYER._SEMANTIC_METADATA 
   WHERE MODEL_NAME = 'Sales Analytics_PushSync';
   ```

---

## 🎮 Usage Examples

### Check Current State

```python
from automated_sync_monitor import SyncMonitor

monitor = SyncMonitor()

# List all models
models = monitor.get_fabric_models()
for model_id, model_data in models.items():
    print(f"{model_data['name']}: {model_id}")

# Check sync state
print(monitor.state)
```

### Manual Conversion

```python
from model_converter import ModelConverter

converter = ModelConverter()

# Convert specific model
converter.convert_and_sync(
    dataset_id="abc-123-def-456",
    dataset_name="My Model"
)

# Auto-convert all
converter.auto_convert_new_models()
```

### Verify Sync in Snowflake

```bash
# Use existing verification scripts
python check_demo_table.py

# Or query directly
python -c "
from semantic_sync.config.settings import load_settings
import snowflake.connector

settings = load_settings()
sf = settings.get_snowflake_config()

conn = snowflake.connector.connect(
    account=sf.account,
    user=sf.user,
    password=sf.password.get_secret_value(),
    warehouse=sf.warehouse,
    database=sf.database,
    schema=sf.schema_name
)

cursor = conn.cursor()
cursor.execute('SELECT MODEL_NAME FROM _SEMANTIC_METADATA')
for row in cursor:
    print(f'  - {row[0]}')
cursor.close()
conn.close()
"
```

---

## 🛠️ Troubleshooting

### Model Not Syncing?

1. **Check if model is empty**:
   ```bash
   python check_demo_rest.py
   ```

2. **Verify Push API dataset was created**:
   ```bash
   python list_fabric_datasets.py | grep "_PushSync"
   ```

3. **Check sync state**:
   ```bash
   cat sync_state.json
   ```

4. **Run manual sync**:
   ```bash
   semantic-sync sync --direction fabric-to-snowflake --verbose
   ```

### Monitor Not Detecting Changes?

- Check the interval time (default: 5 minutes)
- Verify the model was modified/created after the last check
- Check logs in the terminal output

### Conversion Failing?

- Model must have at least one table with columns
- Verify model schema is readable (not hidden/protected)
- Check if you have proper permissions

---

## 📝 State File Format

The `sync_state.json` tracks synced models:

```json
{
  "synced_models": {
    "abc-123-def-456": {
      "name": "Sales Analytics_PushSync",
      "last_sync_time": "2026-01-13T10:35:00",
      "sync_count": 3
    }
  },
  "last_check": "2026-01-13T10:40:00"
}
```

---

## 🔄 Integration with Existing Scripts

The automated system works seamlessly with your existing scripts:

- ✅ `fabric_to_snowflake_sync.py` - Batch sync all models
- ✅ `auto_sync.py` - Snowflake to Fabric sync
- ✅ `check_*.py` scripts - Verification scripts
- ✅ `semantic-sync` CLI - Core sync commands

---

## 🎯 Next Steps

1. **Start the monitor**:
   ```bash
   python automated_sync_monitor.py
   ```

2. **Create a test model** in Fabric with some tables

3. **Wait 5 minutes** and watch the magic happen! 🪄

4. **Verify in Snowflake**:
   ```bash
   python check_demo_table.py
   ```

---

## 💡 Tips

- Use `--interval 60` for faster checking during testing
- Run `--once` mode from Windows Task Scheduler for production
- Monitor the `sync_state.json` file to see what's been synced
- Check Snowflake's `_SEMANTIC_METADATA` table to verify syncs
- Push API datasets support real-time data push via REST API

---

## 🤝 Support

If you encounter issues:

1. Check the logs in the terminal output
2. Verify your `.env` configuration
3. Run `semantic-sync validate` to test connections
4. Check the individual test scripts for debugging

---

**Happy Syncing! 🚀**
