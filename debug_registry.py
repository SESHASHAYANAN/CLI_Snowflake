"""Debug metadata registry lookup"""
from semantic_sync.core.metadata_registry import get_metadata_registry

registry = get_metadata_registry()
model_name = "demo Table"

print(f"Checking registry for '{model_name}'...")
print(f"Registry dir: {registry.registry_dir}")

has_def = registry.has_manual_definition(model_name)
print(f"Has definition? {has_def}")

normalized = registry._normalize_name(model_name)
print(f"Normalized name: '{normalized}'")

print("Keys in file cache:", list(registry._file_metadata_cache.keys()))

if has_def:
    tables = registry.get_manual_tables(model_name)
    print(f"Tables found: {len(tables)}")
else:
    print("❌ No definition found!")
