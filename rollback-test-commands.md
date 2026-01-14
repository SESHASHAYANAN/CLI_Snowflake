# 🔄 Snowflake Rollback Test Workflow

Follow these steps to verify the Semantic Sync rollback functionality.

### 📝 Prerequisites
Ensure you are in the project root and your virtual environment is activated:
```bash
source venv/bin/activate
```

---

### Step 1: Create Baseline Snapshot
Capture the clean state before making changes.
```bash
semantic-sync snapshot create --source snowflake -d "Baseline - clean state"
```
*> Note the Snapshot ID returned in the output (e.g., `abc-123`)*

---

### Step 2: Make a Change (Add a Column)
Run this Python snippet to add a test column `ROLLBACK_TEST_COL` to `SALES_DATA`.

```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
conn.cursor().execute('ALTER TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA ADD COLUMN ROLLBACK_TEST_COL VARCHAR')
print('✅ Added column ROLLBACK_TEST_COL to SALES_DATA')
conn.close()
"
```

---

### Step 3: Create Snapshot of Changed State
Capture the "bad" version (optional, but good for history).
```bash
semantic-sync snapshot create --source snowflake -d "Bad version - added test column"
```

---

### Step 4: Verify the Change
Check that the column exists in Snowflake.

```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
cols = conn.cursor().execute('DESC TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA').fetchall()
found = any(c[0] == 'ROLLBACK_TEST_COL' for c in cols)
print(f'🧐 Column ROLLBACK_TEST_COL exists: {found}')
conn.close()
"
```

---

### Step 5: Rollback to Baseline
Restore the first snapshot using the ID from Step 1.

```bash
# Replace <ID_FROM_STEP_1> with your actual Snapshot ID
semantic-sync snapshot restore --id <ID_FROM_STEP_1> --apply
```
*> Expected Output: "Restored X table(s) to Snowflake"*

---

### Step 6: Verify Rollback
Run the verification check again. The column should be **GONE**.

```bash
python -c "
import snowflake.connector, os; from dotenv import load_dotenv; load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'), role='SYSADMIN'
)
cols = conn.cursor().execute('DESC TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA').fetchall()
found = any(c[0] == 'ROLLBACK_TEST_COL' for c in cols)
print(f'🎉 Rollback successful! Column ROLLBACK_TEST_COL exists: {found}')
# Expected: False
conn.close()
"
```
