"""
Check sync status between Fabric and Snowflake.
Lists models present in Fabric but missing from Snowflake, and vice versa.
"""
import os
import sys
import snowflake.connector
from dotenv import load_dotenv

# Fix encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Add current directory to path so we can import local modules
current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Now import local modules
try:
    from semantic_sync.core.fabric_client import FabricClient
    from semantic_sync.config.settings import load_settings
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

# Load environment variables
load_dotenv()

def get_snowflake_models():
    """Get set of model names from Snowflake."""
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MODEL_NAME FROM _SEMANTIC_METADATA")
        return {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()
        conn.close()

def get_fabric_datasets():
    """Get set of dataset names from Fabric."""
    # Load configuration
    try:
        settings = load_settings()
        client = FabricClient(settings.get_fabric_config())
        
        # Use client to list datasets in the configured workspace
        # list_workspace_datasets returns list of dicts
        datasets = client.list_workspace_datasets()
        
        return {item['name'] for item in datasets}
        
    except Exception as e:
        print(f"Error accessing Fabric: {e}")
        return set()

def main():
    print("=" * 80)
    print("CHECKING SYNC STATUS: FABRIC vs SNOWFLAKE")
    print("=" * 80)
    
    print("Fetching Snowflake models...")
    try:
        snowflake_models = get_snowflake_models()
        print(f"Found {len(snowflake_models)} models in Snowflake.")
    except Exception as e:
        print(f"Failed to fetch Snowflake models: {e}")
        return
    
    print("Fetching Fabric models...")
    try:
        fabric_models = get_fabric_datasets()
        print(f"Found {len(fabric_models)} models in Fabric.")
    except Exception as e:
        print(f"Failed to fetch Fabric models: {e}")
        return

    print("-" * 80)
    
    # Comparisons
    # Case-insensitive comparison might be safer, but let's try exact first
    snowflake_models_lower = {m.lower(): m for m in snowflake_models}
    fabric_models_lower = {m.lower(): m for m in fabric_models}
    
    missing_in_sf = []
    for f_lower, f_name in fabric_models_lower.items():
        if f_lower not in snowflake_models_lower:
            missing_in_sf.append(f_name)
    
    extra_in_sf = []
    for s_lower, s_name in snowflake_models_lower.items():
        if s_lower not in fabric_models_lower:
            extra_in_sf.append(s_name)
            
    synced_count = len(fabric_models) - len(missing_in_sf)

    if missing_in_sf:
        print(f"\n[MISSING] {len(missing_in_sf)} models are in Fabric but NOT in Snowflake:")
        for name in sorted(missing_in_sf):
            print(f"  ❌ {name}")
            
        print("\nTo sync these, run: semantic-sync fabric-to-sf")
    else:
        print("\n[OK] All Fabric models are present in Snowflake.")

    if extra_in_sf:
        print(f"\n[EXTRA] {len(extra_in_sf)} models are in Snowflake but NOT in Fabric:")
        for name in sorted(extra_in_sf):
            print(f"  ⚠️  {name}")

    print(f"\n[SYNCED] {synced_count} match(es).")

if __name__ == "__main__":
    main()
