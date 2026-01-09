"""
Create the SEMANTIC_VIEW in Snowflake for semantic-sync to use.
"""

import sys
import os

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")

from semantic_sync.config import get_settings
import snowflake.connector

def main():
    """Create SEMANTIC_VIEW in Snowflake."""
    print("="*60)
    print("Snowflake SEMANTIC_VIEW Creator")
    print("="*60)
    print()
    
    try:
        # Load configuration
        settings = get_settings()
        snowflake_config = settings.get_snowflake_config()
        
        print(f"Database: {snowflake_config.database}")
        print(f"Schema: {snowflake_config.schema_name}")
        print(f"View Name: {snowflake_config.semantic_view_name}")
        print()
        
        # Connect to Snowflake
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=snowflake_config.account,
            user=snowflake_config.user,
            password=snowflake_config.password.get_secret_value(),
            warehouse=snowflake_config.warehouse,
            database=snowflake_config.database,
            schema=snowflake_config.schema_name
        )
        
        print("[OK] Connected successfully!")
        print()
        
        cursor = conn.cursor()
        
        # Create the semantic view
        print("Creating SEMANTIC_VIEW...")
        print()
        
        create_view_sql = f"""
CREATE OR REPLACE VIEW {snowflake_config.semantic_view_name} AS
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default,
    ordinal_position
FROM information_schema.columns
WHERE table_schema = '{snowflake_config.schema_name}'
  AND table_catalog = '{snowflake_config.database}'
  AND table_name NOT LIKE 'SV_%'  -- Exclude existing semantic views
ORDER BY table_name, ordinal_position;
"""
        
        print("Executing SQL:")
        print(create_view_sql)
        print()
        
        cursor.execute(create_view_sql)
        
        print("[OK] SEMANTIC_VIEW created successfully!")
        print()
        
        # Test the view
        print("Testing the view...")
        test_sql = f"SELECT COUNT(*) as column_count FROM {snowflake_config.semantic_view_name}"
        cursor.execute(test_sql)
        result = cursor.fetchone()
        column_count = result[0] if result else 0
        
        print(f"[OK] View contains {column_count} column definitions")
        print()
        
        # Show sample data
        print("Sample data from SEMANTIC_VIEW:")
        print("-" * 80)
        sample_sql = f"SELECT table_name, column_name, data_type FROM {snowflake_config.semantic_view_name} LIMIT 10"
        cursor.execute(sample_sql)
        results = cursor.fetchall()
        
        print(f"{'Table Name':<30} {'Column Name':<30} {'Data Type':<20}")
        print("-" * 80)
        for row in results:
            print(f"{row[0]:<30} {row[1]:<30} {row[2]:<20}")
        
        print()
        print("="*60)
        print("SUCCESS!")
        print("="*60)
        print()
        print("You can now run:")
        print("  python -m semantic_sync.main preview --direction snowflake-to-fabric")
        print()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
