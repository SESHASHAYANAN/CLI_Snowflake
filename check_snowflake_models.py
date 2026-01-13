"""List all models synced to Snowflake"""
import snowflake.connector
from semantic_sync.config.settings import load_settings

s = load_settings()
c = s.get_snowflake_config()

conn = snowflake.connector.connect(
    account=c.account,
    user=c.user,
    password=c.password.get_secret_value(),
    warehouse=c.warehouse,
    database=c.database,
    schema=c.schema_name
)

cursor = conn.cursor()
print(f"\nModels in Snowflake ({c.database}.{c.schema_name}):")
print("=" * 60)

cursor.execute("SELECT MODEL_NAME FROM _SEMANTIC_METADATA")
for row in cursor.fetchall():
    print(f"  [OK] {row[0]}")

cursor.close()
conn.close()
