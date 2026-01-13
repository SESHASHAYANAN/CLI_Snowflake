import json
import sys
import snowflake.connector
from semantic_sync.config.settings import load_settings

# Fix encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

try:
    settings = load_settings()
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**settings.get_snowflake_config().get_connection_params())
    cursor = conn.cursor()
    
    print("Fetching metadata for 'Employee'...")
    cursor.execute("""
        SELECT 
            MODEL_ID,
            MODEL_NAME, 
            TABLE_COUNT, 
            COLUMN_COUNT,
            MODEL_JSON
        FROM _SEMANTIC_METADATA
        WHERE MODEL_NAME = 'Employee'
    """)
    
    row = cursor.fetchone()
    
    if row:
        model_id, model_name, table_count, col_count, model_json_str = row
        print(f"\n[FOUND] Employee Model:")
        print(f"  ID:          {model_id}")
        print(f"  Name:        {model_name}")
        print(f"  Tables:      {table_count}")
        print(f"  Columns:     {col_count}")
        
        if model_json_str:
            model_json = json.loads(model_json_str)
            tables = model_json.get('tables', [])
            if tables:
                print("\n  Tables Detail:")
                for t in tables:
                    t_name = t.get('name', 'Unknown')
                    t_cols = t.get('columns', [])
                    print(f"    - {t_name} ({len(t_cols)} columns)")
                    for c in t_cols[:3]:
                        print(f"      * {c.get('name')} ({c.get('data_type')})")
                    if len(t_cols) > 3:
                        print(f"      ... and {len(t_cols)-3} more")
        
        if table_count == 0 or col_count == 0:
            print("\n  [WARNING] Model appears empty (0 tables or 0 columns).")
            print("  Required Action: Add metadata definition to metadata/Employee.yaml")
    else:
        print("\n[NOT FOUND] 'Employee' model not found in Snowflake metadata.")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"[ERROR] {e}")
