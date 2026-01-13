"""
Check for new tables created in Snowflake SEMANTIC_LAYER schema
"""
import os
import sys
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
print("CHECKING FOR NEW TABLES IN SNOWFLAKE")
print("=" * 80)

# Get all tables in SEMANTIC_LAYER schema
print("\n1. All Tables in SEMANTIC_LAYER Schema:")
print("-" * 80)
cursor.execute("""
    SELECT 
        TABLE_NAME,
        ROW_COUNT,
        BYTES / 1024 as SIZE_KB,
        CREATED as CREATED_DATE,
        LAST_ALTERED as LAST_UPDATED
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
    ORDER BY CREATED DESC
""")

all_tables = cursor.fetchall()
print(f"Total tables: {len(all_tables)}\n")

# Show all tables with creation time
for idx, row in enumerate(all_tables, 1):
    table_name = row[0]
    row_count = row[1] if row[1] else 0
    size_kb = row[2] if row[2] else 0
    created = row[3]
    updated = row[4]
    
    # Calculate age
    if created:
        # Make datetime timezone-aware for comparison
        now = datetime.now(created.tzinfo) if created.tzinfo else datetime.now()
        age = now - created
        if age < timedelta(hours=1):
            age_str = f"{int(age.total_seconds() / 60)} min ago"
            marker = " <-- NEW! 🆕"
        elif age < timedelta(days=1):
            age_str = f"{int(age.total_seconds() / 3600)} hours ago"
            marker = " <-- Recent"
        else:
            age_str = f"{age.days} days ago"
            marker = ""
    else:
        age_str = "Unknown"
        marker = ""
    
    print(f"{idx:2}. {table_name:40} | Rows: {row_count:6} | Created: {created} ({age_str}){marker}")

# Check for tables created in the last hour
print("\n" + "=" * 80)
print("2. Tables Created in Last Hour:")
print("-" * 80)
cursor.execute("""
    SELECT 
        TABLE_NAME,
        ROW_COUNT,
        CREATED
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
    AND CREATED >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    ORDER BY CREATED DESC
""")

recent_tables = cursor.fetchall()
if recent_tables:
    print(f"Found {len(recent_tables)} new table(s):\n")
    for row in recent_tables:
        print(f"  - {row[0]}")
        print(f"    Rows: {row[1]}")
        print(f"    Created: {row[2]}")
        print()
else:
    print("No tables created in the last hour.")

# Check for tables updated in the last hour
print("\n" + "=" * 80)
print("3. Tables Modified in Last Hour:")
print("-" * 80)
cursor.execute("""
    SELECT 
        TABLE_NAME,
        ROW_COUNT,
        CREATED,
        LAST_ALTERED
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
    AND LAST_ALTERED >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    ORDER BY LAST_ALTERED DESC
""")

modified_tables = cursor.fetchall()
if modified_tables:
    print(f"Found {len(modified_tables)} modified table(s):\n")
    for row in modified_tables:
        print(f"  - {row[0]}")
        print(f"    Rows: {row[1]}")
        print(f"    Created: {row[2]}")
        print(f"    Last Modified: {row[3]}")
        print()
else:
    print("No tables modified in the last hour.")

# Check metadata tables
print("\n" + "=" * 80)
print("4. Metadata Tables Status:")
print("-" * 80)

metadata_tables = [
    '_SEMANTIC_METADATA',
    '_SEMANTIC_COLUMNS', 
    '_SEMANTIC_MEASURES',
    '_SEMANTIC_RELATIONSHIPS',
    '_SEMANTIC_SYNC_HISTORY'
]

for table in metadata_tables:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:30} | {count:6} rows")
    except Exception as e:
        print(f"  {table:30} | ERROR: {e}")

# Check for any new data tables (non-metadata)
print("\n" + "=" * 80)
print("5. Data Tables (Non-Metadata):")
print("-" * 80)
cursor.execute("""
    SELECT 
        TABLE_NAME,
        ROW_COUNT,
        CREATED
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
    AND TABLE_NAME NOT LIKE '_SEMANTIC%'
    ORDER BY CREATED DESC
""")

data_tables = cursor.fetchall()
print(f"Total data tables: {len(data_tables)}\n")
for row in data_tables:
    table_name = row[0]
    row_count = row[1] if row[1] is not None else 0
    created = row[2]
    if created:
        now = datetime.now(created.tzinfo) if created.tzinfo else datetime.now()
        age = now - created
        age_str = f"{int(age.total_seconds() / 60)} min ago" if age < timedelta(hours=1) else ""
        marker = " 🆕" if age < timedelta(hours=1) else ""
    else:
        age_str = ""
        marker = ""
    print(f"  - {table_name:30} | Rows: {row_count:6} | Created: {created} {age_str}{marker}")

cursor.close()
conn.close()
