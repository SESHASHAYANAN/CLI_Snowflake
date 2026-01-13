"""
Check the semantic metadata stored in Snowflake for all models
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

print("=" * 80)
print("ANALYZING _SEMANTIC_METADATA IN SNOWFLAKE")
print("=" * 80)

# Get all models with their metadata JSON
cursor.execute("""
    SELECT 
        MODEL_NAME, 
        TABLE_COUNT, 
        COLUMN_COUNT, 
        MEASURE_COUNT,
        MODEL_JSON,
        CREATED_AT,
        UPDATED_AT
    FROM _SEMANTIC_METADATA
    ORDER BY MODEL_NAME
""")

models = cursor.fetchall()

# Categorize models
working_models = ['annual', 'continent', 'industry', 'probablility']
target_model = 'new_rep'

for row in models:
    model_name = row[0]
    table_count = row[1]
    column_count = row[2]
    measure_count = row[3]
    model_json_str = row[4]
    created_at = row[5]
    updated_at = row[6]
    
    # Determine category
    if model_name.lower() in [m.lower() for m in working_models]:
        category = "WORKING"
    elif model_name.lower() == target_model.lower():
        category = "TARGET"
    else:
        category = "OTHER"
    
    print(f"\n{'='*80}")
    print(f"Model: {model_name} [{category}]")
    print(f"{'='*80}")
    print(f"  Tables: {table_count}, Columns: {column_count}, Measures: {measure_count}")
    print(f"  Created: {created_at}")
    print(f"  Updated: {updated_at}")
    
    # Parse and display MODEL_JSON
    if model_json_str:
        try:
            model_json = json.loads(model_json_str)
            print(f"\n  MODEL_JSON Structure:")
            print(f"    - name: {model_json.get('name', 'N/A')}")
            print(f"    - source: {model_json.get('source', 'N/A')}")
            print(f"    - description: {model_json.get('description', 'N/A')}")
            
            tables = model_json.get('tables', [])
            print(f"    - tables: {len(tables)} table(s)")
            
            if tables:
                for table in tables:
                    table_name = table.get('name', 'unknown')
                    columns = table.get('columns', [])
                    print(f"      * {table_name}: {len(columns)} columns")
                    for col in columns[:3]:  # Show first 3 columns
                        col_name = col.get('name', 'unknown')
                        col_type = col.get('data_type', 'unknown')
                        print(f"        - {col_name} ({col_type})")
                    if len(columns) > 3:
                        print(f"        ... and {len(columns) - 3} more column(s)")
            else:
                print(f"      (No tables defined)")
            
            measures = model_json.get('measures', [])
            print(f"    - measures: {len(measures)} measure(s)")
            
            relationships = model_json.get('relationships', [])
            print(f"    - relationships: {len(relationships)} relationship(s)")
            
            metadata = model_json.get('metadata', {})
            print(f"    - metadata:")
            for key, value in metadata.items():
                print(f"      * {key}: {value}")
                
        except Exception as e:
            print(f"\n  MODEL_JSON: Error parsing JSON - {e}")
            print(f"  Raw JSON (first 200 chars): {model_json_str[:200]}...")
    else:
        print(f"\n  MODEL_JSON: NULL or empty")

print("\n" + "=" * 80)
print("COMPARISON SUMMARY")
print("=" * 80)

# Compare working vs non-working
print("\nWORKING Models (annual, continent, industry, probablility):")
cursor.execute("""
    SELECT MODEL_NAME, TABLE_COUNT, COLUMN_COUNT, MEASURE_COUNT
    FROM _SEMANTIC_METADATA
    WHERE LOWER(MODEL_NAME) IN ('annual', 'continent', 'industry', 'probablility')
    ORDER BY MODEL_NAME
""")
for row in cursor.fetchall():
    print(f"  - {row[0]}: {row[1]} tables, {row[2]} columns, {row[3]} measures")

print("\nTARGET Model (new_rep):")
cursor.execute("""
    SELECT MODEL_NAME, TABLE_COUNT, COLUMN_COUNT, MEASURE_COUNT
    FROM _SEMANTIC_METADATA
    WHERE LOWER(MODEL_NAME) = 'new_rep'
""")
result = cursor.fetchone()
if result:
    print(f"  - {result[0]}: {result[1]} tables, {result[2]} columns, {result[3]} measures")
else:
    print("  - NOT FOUND")

print("\n" + "=" * 80)
print("KEY INSIGHT:")
print("=" * 80)
print("If working models have MODEL_JSON with actual table/column definitions,")
print("but new_rep doesn't, then the issue is that new_rep was synced before")
print("its metadata was properly defined in Fabric.")

cursor.close()
conn.close()
