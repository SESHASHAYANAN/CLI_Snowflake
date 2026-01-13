#!/usr/bin/env python3
"""Quick Snowflake Database Viewer"""

import sys
import os
sys.path.append(os.getcwd())
from semantic_sync.config import get_settings
import snowflake.connector

def view_database():
    settings = get_settings()
    conf = settings.get_snowflake_config()
    
    print(f"\nConnecting to {conf.account}...")
    conn = snowflake.connector.connect(
        account=conf.account,
        user=conf.user,
        password=conf.password.get_secret_value(),
        warehouse=conf.warehouse,
        database=conf.database,
        schema=conf.schema_name
    )
    
    cursor = conn.cursor()
    
    # 1. List all tables
    print("\n" + "="*70)
    print("ALL TABLES")
    print("="*70)
    cursor.execute("SHOW TABLES")
    for row in cursor.fetchall():
        print(f"  {row[1]}")  # Table name
    
    # 2. View products
    print("\n" + "="*70)
    print("DEMO_PRODUCTS DATA")
    print("="*70)
    cursor.execute("SELECT * FROM DEMO_PRODUCTS")
    print(f"{'ID':<5} {'Name':<20} {'Category':<15} {'Price':<10}")
    print("-"*70)
    for row in cursor.fetchall():
        print(f"{row[0]:<5} {row[1]:<20} {row[2]:<15} ${row[3]:<10}")
    
    # 3. View sales
    print("\n" + "="*70)
    print("SALES_DATA")
    print("="*70)
    cursor.execute("SELECT * FROM SALES_DATA")
    print(f"{'Order':<8} {'Customer':<20} {'Product':<15} {'Qty':<5} {'Price':<12}")
    print("-"*70)
    for row in cursor.fetchall():
        print(f"{row[0]:<8} {row[1]:<20} {row[2]:<15} {row[3]:<5} ${row[4]:<12}")
    
    # 4. View models
    print("\n" + "="*70)
    print("SYNCED MODELS")
    print("="*70)
    cursor.execute("SELECT MODEL_NAME, TABLE_COUNT FROM _SEMANTIC_METADATA ORDER BY UPDATED_AT DESC")
    for row in cursor.fetchall():
        print(f"  {row[0]:<40} Tables: {row[1]}")
    
    # 5. Row counts
    print("\n" + "="*70)
    print("ROW COUNTS")
    print("="*70)
    cursor.execute("SELECT COUNT(*) FROM DEMO_PRODUCTS")
    print(f"  DEMO_PRODUCTS: {cursor.fetchone()[0]} rows")
    
    cursor.execute("SELECT COUNT(*) FROM SALES_DATA")
    print(f"  SALES_DATA: {cursor.fetchone()[0]} rows")
    
    cursor.execute("SELECT COUNT(*) FROM MY_NEW_TABLE")
    print(f"  MY_NEW_TABLE: {cursor.fetchone()[0]} rows")
    
    conn.close()
    print("\n" + "="*70)
    print("DATABASE VIEW COMPLETE")
    print("="*70 + "\n")

if __name__ == "__main__":
    view_database()
