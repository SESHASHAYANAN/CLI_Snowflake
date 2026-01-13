
import json
import os
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.config.settings import load_settings

def inspect_definition():
    dataset_id = "d0e5ea6d-f17a-49c0-a331-eda1cb2feeb3"
    print(f"Fetching definition for dataset: {dataset_id}")
    
    settings = load_settings()
    client = FabricClient(settings.get_fabric_config())
    
    try:
        # Fetch definition (handles LRO internally)
        definition = client.get_semantic_model_definition(dataset_id)
        
        if definition:
            # Save raw response
            with open("demo_table_raw.json", "w") as f:
                json.dump(definition, f, indent=2)
            print("Successfully saved definition to demo_table_raw.json")
            
            # Helper to inspect BIM content
            if "definition" in definition and "parts" in definition["definition"]:
                parts = definition["definition"]["parts"]
                print(f"Found {len(parts)} parts in definition.")
                for part in parts:
                    print(f" - Path: {part.get('path')} (Type: {part.get('payloadType')})")
                    if part.get("path") == "model.bim":
                         from base64 import b64decode
                         payload = part.get("payload")
                         decoded = b64decode(payload).decode("utf-8")
                         bim = json.loads(decoded)
                         
                         model = bim.get("model", {})
                         tables = model.get("tables", [])
                         print(f"   [BIM Analysis] Found {len(tables)} tables in model.bim")
                         for t in tables:
                             print(f"    - {t.get('name')}")
        else:
            print("Failed to retrieve definition (None returned).")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_definition()
