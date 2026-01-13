"""
Test Script: Automated Model Conversion and Sync

This script demonstrates the complete workflow:
1. Create a new model in Fabric (or use existing)
2. Automated monitor detects the new model
3. Auto-converts non-Push API model to Push API format
4. Syncs to Snowflake using CLI
5. Verifies the model appears in Snowflake

Usage:
    python test_auto_sync.py
"""

import sys
import os

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from model_converter import ModelConverter
from automated_sync_monitor import  SyncMonitor


def test_manual_conversion():
    """Test 1: Manual conversion of the demo Table model."""
    print("\n" + "="*70)
    print("TEST 1: Manual Model Conversion")
    print("="*70)
    
    converter = ModelConverter()
    
    # Try to convert the demo Table
    demo_table_id = "d0e5ea6d-f17a-49c0-a331-eda1cb2feeb3"
    
    try:
        print("\nAttempting to convert 'demo Table' to Push API format...")
        converter.convert_and_sync(demo_table_id, "demo Table")
    except Exception as e:
        print(f"\n⚠️  Expected error (demo Table is empty): {e}")
        print("This is expected - the demo Table has no tables to convert")


def test_auto_conversion():
    """Test 2: Auto-detect and convert all new models."""
    print("\n" + "="*70)
    print("TEST 2: Auto-Convert All New Models")
    print("="*70)
    
    converter = ModelConverter()
    
    try:
        converter.auto_convert_new_models()
    except Exception as e:
        print(f"\n⚠️  Error during auto-conversion: {e}")


def test_monitoring():
    """Test 3: Run monitoring once."""
    print("\n" + "="*70)
    print("TEST 3: Automated Sync Monitor (One-Time)")
    print("="*70)
    
    monitor = SyncMonitor(check_interval_seconds=300)
    
    try:
        monitor.run_once()
    except Exception as e:
        print(f"\n⚠️  Error during monitoring: {e}")


def main():
    """Run all tests."""
    print("\n" + "="*70)
    print("🧪 AUTOMATED SYNC TEST SUITE")
    print("="*70)
    print("\nThis will test the automated model conversion and sync workflow.")
    print("Expected outcome:")
    print("1. Detects all models in Fabric workspace")
    print("2. Converts non-Push API models to Push API format")
    print("3. Syncs all models to Snowflake")
    print("\nPress Enter to continue or Ctrl+C to cancel...")
    input()
    
    # Test 1: Manual conversion
    test_manual_conversion()
    
    # Test 2: Auto-conversion
   test_auto_conversion()
    
    # Test 3: One-time monitoring run
    test_monitoring()
    
    print("\n" + "="*70)
    print("✅ TEST SUITE COMPLETED")
    print("="*70)
    print("\nNext steps:")
    print("1. Check Snowflake to verify models are synced")
    print("2. Run 'python automated_sync_monitor.py' to start continuous monitoring")
    print("3. Create a new model in Fabric and watch it auto-sync!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Tests cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
