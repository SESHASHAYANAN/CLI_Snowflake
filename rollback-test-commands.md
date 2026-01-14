# 🔄 Comprehensive Rollback Test Suite

This document provides step-by-step test cases to verify the robustness of the Semantic Sync rollback functionality. Each test validates a specific scenario.

---

## Prerequisites

```bash
cd /Users/sharada/Documents/projects/CLI_Snowflake
source venv/bin/activate
```

---

## Test 1: Column Addition & Removal

**Scenario:** Add a new column, then rollback to remove it.

### Step 1.1: Create Baseline
```bash
semantic-sync snapshot create --source snowflake -d "Test1: Baseline before column add"
# Note the Snapshot ID: _______________
```

### Step 1.2: Add Column
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
conn.cursor().execute('ALTER TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA ADD COLUMN TEST_COL_1 VARCHAR')
print('✅ Added TEST_COL_1')
conn.close()
"
```

### Step 1.3: Verify Column Exists
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
cols = [c[0] for c in conn.cursor().execute('DESC TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA').fetchall()]
print('Column TEST_COL_1 exists:', 'TEST_COL_1' in cols)
conn.close()
"
```

### Step 1.4: Rollback
```bash
semantic-sync snapshot restore --id <BASELINE_ID> --apply
```

### Step 1.5: Verify Rollback
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
cols = [c[0] for c in conn.cursor().execute('DESC TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA').fetchall()]
print('🎉 Rollback Success! Column gone:', 'TEST_COL_1' not in cols)
conn.close()
"
```

**Expected Result:** Column `TEST_COL_1` should NOT exist after rollback.

---

## Test 2: Column Data Type Change

**Scenario:** Change a column's data type, then rollback.

### Step 2.1: Create Baseline
```bash
semantic-sync snapshot create --source snowflake -d "Test2: Baseline before type change"
# Note the Snapshot ID: _______________
```

### Step 2.2: Change Column Type
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
# Add a column with specific type
conn.cursor().execute('ALTER TABLE ANALYTICS_DB.SEMANTIC_LAYER.PRODUCTS ADD COLUMN PRICE_TEST INTEGER')
print('✅ Added PRICE_TEST as INTEGER')
conn.close()
"
```

### Step 2.3: Create "Bad" Snapshot
```bash
semantic-sync snapshot create --source snowflake -d "Test2: After type change"
```

### Step 2.4: Rollback to Baseline
```bash
semantic-sync snapshot restore --id <BASELINE_ID> --apply
```

### Step 2.5: Verify
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
cols = [c[0] for c in conn.cursor().execute('DESC TABLE ANALYTICS_DB.SEMANTIC_LAYER.PRODUCTS').fetchall()]
print('🎉 Rollback Success! Column gone:', 'PRICE_TEST' not in cols)
conn.close()
"
```

**Expected Result:** Column `PRICE_TEST` should NOT exist after rollback.

---

## Test 3: New Table Addition & Removal

**Scenario:** Add a new table, then rollback (table should persist but not be in snapshot).

### Step 3.1: Create Baseline
```bash
semantic-sync snapshot create --source snowflake -d "Test3: Baseline before new table"
# Note the Snapshot ID: _______________
```

### Step 3.2: Add New Table
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
conn.cursor().execute('''
CREATE TABLE ANALYTICS_DB.SEMANTIC_LAYER.TEST_NEW_TABLE (
    id INTEGER,
    name VARCHAR(100),
    created_at TIMESTAMP
)
''')
print('✅ Created TEST_NEW_TABLE')
conn.close()
"
```

### Step 3.3: Create Snapshot with New Table
```bash
semantic-sync snapshot create --source snowflake -d "Test3: After adding new table"
# Note the Snapshot ID: _______________
```

### Step 3.4: Rollback
```bash
semantic-sync snapshot restore --id <BASELINE_ID> --apply
```

### Step 3.5: Verify
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
tables = [t[1] for t in conn.cursor().execute('SHOW TABLES IN SCHEMA ANALYTICS_DB.SEMANTIC_LAYER').fetchall()]
print('⚠️ Note: TEST_NEW_TABLE still exists:', 'TEST_NEW_TABLE' in tables)
print('(Rollback restores captured tables, does NOT drop extra tables)')
conn.close()
"
```

**Expected Result:** `TEST_NEW_TABLE` STILL exists (rollback doesn't drop tables not in snapshot).

> **Note:** This is a known limitation. Rollback restores the *state* of captured tables but does not delete tables that were added after the snapshot.

---

## Test 4: Multiple Column Changes

**Scenario:** Add multiple columns, then rollback all at once.

### Step 4.1: Create Baseline
```bash
semantic-sync snapshot create --source snowflake -d "Test4: Baseline before multi-column"
# Note the Snapshot ID: _______________
```

### Step 4.2: Add Multiple Columns
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
cur = conn.cursor()
cur.execute('ALTER TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA ADD COLUMN MULTI_COL_A VARCHAR')
cur.execute('ALTER TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA ADD COLUMN MULTI_COL_B INTEGER')
cur.execute('ALTER TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA ADD COLUMN MULTI_COL_C FLOAT')
print('✅ Added 3 columns: MULTI_COL_A, MULTI_COL_B, MULTI_COL_C')
conn.close()
"
```

### Step 4.3: Rollback
```bash
semantic-sync snapshot restore --id <BASELINE_ID> --apply
```

### Step 4.4: Verify All Removed
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
cols = [c[0] for c in conn.cursor().execute('DESC TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA').fetchall()]
a_gone = 'MULTI_COL_A' not in cols
b_gone = 'MULTI_COL_B' not in cols
c_gone = 'MULTI_COL_C' not in cols
print(f'🎉 All columns removed: A={a_gone}, B={b_gone}, C={c_gone}')
conn.close()
"
```

**Expected Result:** All three columns should be gone.

---

## Test 5: Measure Modification (Fabric/PowerBI)

**Scenario:** Modify a measure in Power BI, then rollback.

> **Note:** This test requires a working Fabric connection. If Fabric DAX queries fail, skip this test.

### Step 5.1: Create Baseline from Fabric
```bash
semantic-sync snapshot create --source fabric -d "Test5: Fabric baseline"
# Note the Snapshot ID: _______________
```

### Step 5.2: Modify Measure in Power BI
1. Open Power BI Desktop or the Fabric Web UI
2. Navigate to your semantic model
3. Edit an existing measure (e.g., change `SUM(Sales)` to `SUM(Sales) * 1.1`)
4. Save the model

### Step 5.3: Create Snapshot After Change
```bash
semantic-sync snapshot create --source fabric -d "Test5: After measure change"
```

### Step 5.4: Rollback
```bash
semantic-sync snapshot restore --id <BASELINE_ID> --apply
```

### Step 5.5: Verify
Check the measure in Power BI to confirm it reverted.

**Expected Result:** Measure should revert to original definition.

---

## Test 6: Dry Run Verification

**Scenario:** Use `--dry-run` to preview without applying.

### Step 6.1: Create Snapshot
```bash
semantic-sync snapshot create --source snowflake -d "Test6: Dry run test"
```

### Step 6.2: Make a Change
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
conn.cursor().execute('ALTER TABLE ANALYTICS_DB.SEMANTIC_LAYER.PRODUCTS ADD COLUMN DRY_RUN_TEST VARCHAR')
print('✅ Added DRY_RUN_TEST column')
conn.close()
"
```

### Step 6.3: Dry Run Restore
```bash
semantic-sync snapshot restore --id <SNAPSHOT_ID> --apply --dry-run
```

### Step 6.4: Verify Column Still Exists
```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
cols = [c[0] for c in conn.cursor().execute('DESC TABLE ANALYTICS_DB.SEMANTIC_LAYER.PRODUCTS').fetchall()]
print('Dry run preserved column:', 'DRY_RUN_TEST' in cols)
conn.close()
"
```

**Expected Result:** Column should STILL exist (dry run doesn't apply changes).

---

## Test 7: Restore Latest Snapshot

**Scenario:** Use `--latest` flag instead of specific ID.

### Step 7.1: Create Multiple Snapshots
```bash
semantic-sync snapshot create --source snowflake -d "Test7: Snapshot A"
sleep 2
semantic-sync snapshot create --source snowflake -d "Test7: Snapshot B (latest)"
```

### Step 7.2: Restore Latest
```bash
semantic-sync snapshot restore --latest --apply
```

**Expected Result:** Should restore "Snapshot B" (most recent).

---

## Test 8: Snapshot Cleanup

**Scenario:** Verify cleanup keeps only specified number of snapshots.

### Step 8.1: List Current Snapshots
```bash
semantic-sync snapshot list
```

### Step 8.2: Cleanup (Keep 3)
```bash
semantic-sync snapshot cleanup --keep 3 --force
```

### Step 8.3: Verify
```bash
semantic-sync snapshot list
```

**Expected Result:** Only 3 most recent snapshots remain.

---

## Quick Reference: All Commands

| Action | Command |
|--------|---------|
| Create snapshot | `semantic-sync snapshot create --source snowflake -d "description"` |
| List snapshots | `semantic-sync snapshot list` |
| Preview restore | `semantic-sync snapshot restore --id <ID> --apply --dry-run` |
| Apply restore | `semantic-sync snapshot restore --id <ID> --apply` |
| Restore latest | `semantic-sync snapshot restore --latest --apply` |
| Cleanup old | `semantic-sync snapshot cleanup --keep 3 --force` |

---

## Known Limitations

1. **New tables not dropped:** Rollback restores captured tables but does NOT delete tables added after the snapshot.
2. **Fabric DAX errors:** Some Fabric datasets may fail DAX queries; use Snowflake for reliable testing.
3. **Data not preserved:** Rollback restores *schema* (structure), not *data*. Any data in modified tables is lost.
4. **Permissions required:** `SYSADMIN` role with ownership on tables is required for rollback.
