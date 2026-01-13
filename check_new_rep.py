"""
Check if new_rep exists in Snowflake after sync
"""
import os
import sys
import snowflake.connector
from dotenv import load_dotenv

# Fix encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Load environment variables
load_dotenv()

# Snowflake connection
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)

cursor = conn.cursor()

print("=" * 80)
print("CHECKING FOR 'new_rep' IN SNOWFLAKE")
print("=" * 80)

# First, check the structure of _SEMANTIC_METADATA
print("\n0. Checking structure of _SEMANTIC_METADATA table:")
print("-" * 80)
cursor.execute("DESCRIBE TABLE _SEMANTIC_METADATA")
columns = cursor.fetchall()
column_names = [col[0] for col in columns]
print(f"Columns: {', '.join(column_names)}")

# Check 1: Look in _SEMANTIC_METADATA table
print("\n1. Checking _SEMANTIC_METADATA table for 'new_rep' model:")
print("-" * 80)
cursor.execute("""
    SELECT MODEL_NAME, DESCRIPTION, TABLE_COUNT, COLUMN_COUNT, CREATED_AT
    FROM _SEMANTIC_METADATA
    WHERE LOWER(MODEL_NAME) LIKE '%new_rep%'
""")
results = cursor.fetchall()
if results:
    print(f"[YES] FOUND {len(results)} matching model(s) in _SEMANTIC_METADATA:")
    for row in results:
        print(f"   - Model Name: {row[0]}")
        print(f"     Description: {row[1]}")
        print(f"     Tables: {row[2]}, Columns: {row[3]}")
        print(f"     Created: {row[4]}")
else:
    print("[NO] 'new_rep' NOT found in _SEMANTIC_METADATA table")

# Check 2: Look for any tables with 'new_rep' in the name
print("\n2. Checking for 'new_rep' tables in SEMANTIC_LAYER schema:")
print("-" * 80)
cursor.execute("""
    SHOW TABLES IN SCHEMA SEMANTIC_LAYER
""")
all_tables = cursor.fetchall()
new_rep_tables = [t for t in all_tables if 'new_rep' in t[1].lower()]
if new_rep_tables:
    print(f"[YES] FOUND {len(new_rep_tables)} matching table(s):")
    for row in new_rep_tables:
        print(f"   - {row[1]}")
else:
    print("[NO] No 'new_rep' tables found")

# Check 3: List all models to see what's actually there
print("\n3. All semantic models currently in Snowflake:")
print("-" * 80)
cursor.execute("""
    SELECT MODEL_NAME, TABLE_COUNT, COLUMN_COUNT
    FROM _SEMANTIC_METADATA
    ORDER BY MODEL_NAME
""")
all_models = cursor.fetchall()
print(f"Total models: {len(all_models)}")
for row in all_models:
    marker = " <-- NEW_REP HERE!" if "new_rep" in row[0].lower() else ""
    print(f"   - {row[0]} (Tables: {row[1]}, Cols: {row[2]}){marker}")

print("\n" + "=" * 80)
print("SUMMARY:")
print("=" * 80)

# Final verdict
cursor.execute("""
    SELECT COUNT(*) 
    FROM _SEMANTIC_METADATA 
    WHERE LOWER(MODEL_NAME) LIKE '%new_rep%'
""")
count = cursor.fetchone()[0]

if count > 0:
    print(f"\n>>> YES - 'new_rep' IS in Snowflake ({count} model(s) found) <<<")
    print("\nDetails of new_rep model(s):")
    cursor.execute("""
        SELECT MODEL_NAME, DESCRIPTION, TABLE_COUNT, COLUMN_COUNT, MEASURE_COUNT, CREATED_AT
        FROM _SEMANTIC_METADATA 
        WHERE LOWER(MODEL_NAME) LIKE '%new_rep%'
    """)
    for row in cursor.fetchall():
        print(f"\n  Model: {row[0]}")
        print(f"  Description: {row[1]}")
        print(f"  Tables: {row[2]}")
        print(f"  Columns: {row[3]}")
        print(f"  Measures: {row[4]}")
        print(f"  Created: {row[5]}")
else:
    print("\n>>> NO - 'new_rep' is NOT in Snowflake <<<")
    print("\nPossible reasons:")
    print("  1. Sync hasn't been run yet")
    print("  2. 'new_rep' model doesn't exist in Fabric")
    print("  3. Sync encountered an error")
    print("  4. Model name is different than expected")

cursor.close()
conn.close()
