
import snowflake.connector
from semantic_sync.config.settings import load_settings

def check_demo_details():
    settings = load_settings()
    sf_config = settings.get_snowflake_config()
    
    conn = snowflake.connector.connect(
        user=sf_config.user,
        password=sf_config.password.get_secret_value(),
        account=sf_config.account,
        warehouse=sf_config.warehouse,
        database=sf_config.database,
        schema=sf_config.schema_name,
        role=sf_config.role
    )
    
    cursor = conn.cursor()
    
    print(f"Connected to {sf_config.database}.{sf_config.schema_name}")
    
    # Use fully qualified names if needed or ensure context
    table_prefix = f"{sf_config.database}.{sf_config.schema_name}"
    
    # Check Model in _SEMANTIC_METADATA
    query = f"SELECT MODEL_NAME, MODEL_JSON FROM {table_prefix}._SEMANTIC_METADATA WHERE MODEL_NAME = 'demo Table'"
    print(f"Executing: {query}")
    cursor.execute(query)
    row = cursor.fetchone()
    
    if not row:
        print("[MISSING] Model 'demo Table' NOT found in _SEMANTIC_METADATA table.")
        return
        
    import json
    model_name = row[0]
    model_json_str = row[1]
    
    print(f"[FOUND] Model: {model_name}")
    
    if model_json_str:
        try:
            model_data = json.loads(model_json_str)
            tables = model_data.get('tables', [])
            
            print(f"Found {len(tables)} tables in MODEL_JSON:")
            for t in tables:
                t_name = t.get('name')
                cols = t.get('columns', [])
                print(f"  - Table: {t_name}")
                print(f"    Columns ({len(cols)}):")
                for c in cols:
                     print(f"      * {c.get('name')} ({c.get('data_type')})")
        except json.JSONDecodeError:
            print("[ERROR] Failed to parse MODEL_JSON")
    else:
        print("[WARN] MODEL_JSON is empty")

    conn.close()

if __name__ == "__main__":
    check_demo_details()
