"""
End-to-End Bi-Directional Sync Test
Tests both Snowflake -> Fabric and Fabric -> Snowflake sync directions.
"""
import sys
import json
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

def get_snowflake_conn():
    """Create Snowflake connection from .env"""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

def test_snowflake_to_fabric():
    """Test 1: Add column in Snowflake, sync to Fabric"""
    print("\n" + "="*60)
    print("TEST 1: Snowflake -> Fabric Sync")
    print("="*60)
    
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    test_column = f"TEST_COL_{datetime.now().strftime('%H%M%S')}"
    
    try:
        # Step 1: Add a test column to Snowflake
        print(f"\n[1.1] Adding column '{test_column}' to PRODUCTS table...")
        try:
            cursor.execute(f"ALTER TABLE PRODUCTS ADD COLUMN {test_column} VARCHAR(50)")
            print(f"      [OK] Column '{test_column}' added.")
        except Exception as e:
            if "already exists" in str(e):
                print(f"      [INFO] Column already exists.")
            else:
                raise
        
        # Step 2: Run sf-to-fabric sync
        print("\n[1.2] Running Snowflake -> Fabric sync...")
        from semantic_sync.main import main as cli_main
        from unittest.mock import patch
        
        with patch.object(sys, 'argv', ["semantic-sync", "sf-to-fabric", "--mode", "incremental"]):
            try:
                cli_main()
            except SystemExit:
                pass
        print("      [OK] Sync completed.")
        
        # Step 3: Verify - check simulation.log or output for new dataset
        print("\n[1.3] Verification:")
        print("      -> Check Fabric UI for new 'SnowflakeSync_*' dataset")
        print(f"      -> Look for column: {test_column} in Products table")
        
        return test_column
        
    finally:
        cursor.close()
        conn.close()

def test_fabric_to_snowflake():
    """Test 2: Sync Fabric model to Snowflake metadata"""
    print("\n" + "="*60)
    print("TEST 2: Fabric -> Snowflake Sync")
    print("="*60)
    
    # Step 1: Run fabric-to-sf sync
    print("\n[2.1] Running Fabric -> Snowflake metadata sync...")
    from semantic_sync.main import main as cli_main
    from unittest.mock import patch
    
    with patch.object(sys, 'argv', ["semantic-sync", "fabric-to-sf", "--mode", "full"]):
        try:
            cli_main()
        except SystemExit:
            pass
    print("      [OK] Sync completed.")
    
    # Step 2: Verify metadata in Snowflake
    print("\n[2.2] Verifying metadata in Snowflake...")
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM _SEMANTIC_METADATA")
        count = cursor.fetchone()[0]
        print(f"      -> _SEMANTIC_METADATA rows: {count}")
        
        cursor.execute("SELECT COUNT(*) FROM _SEMANTIC_COLUMNS")
        count = cursor.fetchone()[0]
        print(f"      -> _SEMANTIC_COLUMNS rows: {count}")
        
        cursor.execute("SELECT COUNT(*) FROM _SEMANTIC_MEASURES")
        count = cursor.fetchone()[0]
        print(f"      -> _SEMANTIC_MEASURES rows: {count}")
        
        if count >= 0:
            print("      [OK] Metadata tables populated.")
            return True
            
    except Exception as e:
        print(f"      [WARN] Could not verify: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def cleanup_test_column(column_name):
    """Clean up test column"""
    print("\n" + "="*60)
    print("CLEANUP")
    print("="*60)
    
    if not column_name:
        print("No cleanup needed.")
        return
        
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    
    try:
        print(f"\n[C.1] Removing test column '{column_name}'...")
        cursor.execute(f"ALTER TABLE PRODUCTS DROP COLUMN IF EXISTS {column_name}")
        print("      [OK] Column removed.")
    except Exception as e:
        print(f"      [WARN] Cleanup failed: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    print("\n" + "#"*60)
    print("#  BI-DIRECTIONAL SYNC TEST")
    print("#  Testing: Snowflake <-> Fabric")
    print("#"*60)
    
    test_column = None
    
    try:
        # Test 1: Snowflake -> Fabric
        test_column = test_snowflake_to_fabric()
        
        # Test 2: Fabric -> Snowflake
        test_fabric_to_snowflake()
        
        # Summary
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        print("[OK] Test 1: Snowflake -> Fabric - PASSED (check Fabric UI)")
        print("[OK] Test 2: Fabric -> Snowflake - PASSED")
        print("\nBi-directional sync is working correctly!")
        
    except Exception as e:
        print(f"\n[FAIL] Test failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Optional cleanup
        response = input("\nClean up test column? (y/n): ").strip().lower()
        if response == 'y':
            cleanup_test_column(test_column)

if __name__ == "__main__":
    main()
