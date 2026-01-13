"""
List all semantic metadata stored in Snowflake
"""
import os
import sys
import json
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

print("=" * 100)
print("SEMANTIC METADATA IN SNOWFLAKE")
print("=" * 100)

# Get all metadata
cursor.execute("""
    SELECT 
        MODEL_ID,
        MODEL_NAME, 
        SOURCE_SYSTEM,
        DESCRIPTION,
        TABLE_COUNT, 
        COLUMN_COUNT, 
        MEASURE_COUNT,
        RELATIONSHIP_COUNT,
        MODEL_JSON,
        CREATED_AT,
        UPDATED_AT,
        SYNC_VERSION
    FROM _SEMANTIC_METADATA
    ORDER BY MODEL_NAME
""")

models = cursor.fetchall()

print(f"\nTotal Semantic Models: {len(models)}\n")
print("=" * 100)

for idx, row in enumerate(models, 1):
    model_id = row[0]
    model_name = row[1]
    source_system = row[2]
    description = row[3]
    table_count = row[4]
    column_count = row[5]
    measure_count = row[6]
    relationship_count = row[7]
    model_json_str = row[8]
    created_at = row[9]
    updated_at = row[10]
    sync_version = row[11]
    
    print(f"\n{idx}. {model_name}")
    print("-" * 100)
    print(f"   Model ID:        {model_id}")
    print(f"   Source System:   {source_system}")
    print(f"   Description:     {description if description else '(none)'}")
    print(f"   Tables:          {table_count}")
    print(f"   Columns:         {column_count}")
    print(f"   Measures:        {measure_count}")
    print(f"   Relationships:   {relationship_count}")
    print(f"   Sync Version:    {sync_version}")
    print(f"   Created:         {created_at}")
    print(f"   Updated:         {updated_at}")
    
    # Parse and display table details from MODEL_JSON
    if model_json_str:
        try:
            model_json = json.loads(model_json_str)
            tables = model_json.get('tables', [])
            
            if tables:
                print(f"\n   Tables Detail:")
                for table in tables:
                    table_name = table.get('name', 'unknown')
                    columns = table.get('columns', [])
                    table_desc = table.get('description', '')
                    
                    print(f"      • {table_name} ({len(columns)} columns)")
                    if table_desc:
                        print(f"        Description: {table_desc}")
                    
                    if columns:
                        print(f"        Columns:")
                        for col in columns:
                            col_name = col.get('name', 'unknown')
                            col_type = col.get('data_type', 'unknown')
                            col_desc = col.get('description', '')
                            nullable = col.get('is_nullable', True)
                            nullable_str = "nullable" if nullable else "not null"
                            
                            if col_desc:
                                print(f"          - {col_name} ({col_type}, {nullable_str}) - {col_desc}")
                            else:
                                print(f"          - {col_name} ({col_type}, {nullable_str})")
            
            # Show measures if any
            measures = model_json.get('measures', [])
            if measures:
                print(f"\n   Measures ({len(measures)}):")
                for measure in measures:
                    measure_name = measure.get('name', 'unknown')
                    expression = measure.get('expression', '')
                    measure_desc = measure.get('description', '')
                    print(f"      • {measure_name}")
                    if measure_desc:
                        print(f"        Description: {measure_desc}")
                    if expression:
                        print(f"        Expression: {expression[:80]}{'...' if len(expression) > 80 else ''}")
            
            # Show relationships if any
            relationships = model_json.get('relationships', [])
            if relationships:
                print(f"\n   Relationships ({len(relationships)}):")
                for rel in relationships:
                    from_table = rel.get('from_table', 'unknown')
                    from_column = rel.get('from_column', 'unknown')
                    to_table = rel.get('to_table', 'unknown')
                    to_column = rel.get('to_column', 'unknown')
                    print(f"      • {from_table}.{from_column} → {to_table}.{to_column}")
                    
        except Exception as e:
            print(f"\n   [Error parsing MODEL_JSON: {e}]")

print("\n" + "=" * 100)
print("SUMMARY BY CATEGORY")
print("=" * 100)

# Count by source
cursor.execute("""
    SELECT SOURCE_SYSTEM, COUNT(*) as count
    FROM _SEMANTIC_METADATA
    GROUP BY SOURCE_SYSTEM
    ORDER BY count DESC
""")
print("\nModels by Source:")
for row in cursor.fetchall():
    print(f"  - {row[0]}: {row[1]} model(s)")

# Models with data vs empty
cursor.execute("""
    SELECT 
        CASE 
            WHEN TABLE_COUNT > 0 THEN 'With Data'
            ELSE 'Empty'
        END as status,
        COUNT(*) as count
    FROM _SEMANTIC_METADATA
    GROUP BY status
""")
print("\nModels by Status:")
for row in cursor.fetchall():
    print(f"  - {row[0]}: {row[1]} model(s)")

# Total statistics
cursor.execute("""
    SELECT 
        SUM(TABLE_COUNT) as total_tables,
        SUM(COLUMN_COUNT) as total_columns,
        SUM(MEASURE_COUNT) as total_measures,
        SUM(RELATIONSHIP_COUNT) as total_relationships
    FROM _SEMANTIC_METADATA
""")
totals = cursor.fetchone()
print("\nTotal Statistics:")
print(f"  - Total Tables:        {totals[0]}")
print(f"  - Total Columns:       {totals[1]}")
print(f"  - Total Measures:      {totals[2]}")
print(f"  - Total Relationships: {totals[3]}")

cursor.close()
conn.close()
