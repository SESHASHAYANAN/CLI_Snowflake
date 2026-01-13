import base64
import json
import logging
import requests
from semantic_sync.config.settings import load_settings
from semantic_sync.auth.oauth import FabricOAuthClient

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_fabric_token(fabric_config):
    # Use internal OAuth client with Fabric scopes
    client = FabricOAuthClient(
        config=fabric_config,
        scopes=["https://api.fabric.microsoft.com/.default"]
    )
    return client.get_access_token()

def get_item_definition(fabric_config, item_id):
    workspace_id = fabric_config.workspace_id
    token = get_fabric_token(fabric_config)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Fabric V1 API: POST /workspaces/{workspaceId}/items/{itemId}/getDefinition
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition"
    
    import time
    
    logger.info(f"Requesting definition from: {url}")
    response = requests.post(url, headers=headers)
    
    if response.status_code == 200:
        logger.info("Successfully retrieved definition.")
        return response.json()
    elif response.status_code == 202:
        logger.info("Request accepted (LRO). Polling for result...")
        location = response.headers.get("Location")
        retry_after = int(response.headers.get("Retry-After", 10))
        
        if not location:
             # Some APIs put operation ID in body or Azure-AsyncOperation header
             location = response.headers.get("Azure-AsyncOperation")
             
        if not location:
            logger.error("202 received but no Location header found.")
            return None
            
        logger.info(f"Operation URL: {location}")
        
        while True:
            logger.info(f"Waiting {retry_after}s...")
            time.sleep(retry_after)
            
            op_response = requests.get(location, headers=headers)
            if op_response.status_code == 200:
                op_data = op_response.json()
                status = op_data.get("status")
                logger.info(f"Operation Status: {status}")
                
                if status == "Succeeded":
                    # For getDefinition, the result might be in the operation response or require another call?
                    # Docs say: Operation response body contains the result if successful?
                    # Or it might return "created" result?
                    # Let's check the structure. Usually "result": { ... definition ... }
                    # If the operation returns the definition directly in successful response:
                    if "definition" in op_data:
                         return op_data
                    # Sometimes the result is nested
                    if "result" in op_data and "definition" in op_data["result"]:
                         return op_data["result"]
                    
                    return op_data # Return whole body for inspection if unsure
                    
                elif status in ["Failed", "Canceled"]:
                    logger.error(f"Operation failed: {op_data.get('error')}")
                    return None
            else:
                 logger.warning(f"Poll failed: {op_response.status_code}")
                 
    else:
        logger.error(f"Failed to get definition: {response.status_code} - {response.text}")
        return None

def main():
    settings = load_settings()
    fabric_config = settings.get_fabric_config()
    
    # demo Table ID
    item_id = "d0e5ea6d-f17a-49c0-a331-eda1cb2feeb3"
    
    definition = get_item_definition(fabric_config, item_id)
    
    if definition:
        parts = definition.get("definition", {}).get("parts", [])
        for part in parts:
            path = part.get("path")
            logger.info(f"Found part: {path}")
            
            if path == "model.bim":
                payload = part.get("payload")
                payload_type = part.get("payloadType")
                
                if payload_type == "InlineBase64":
                    decoded = base64.b64decode(payload).decode("utf-8")
                    model_json = json.loads(decoded)
                    
                    # Inspect model content
                    model = model_json.get("model", {})
                    tables = model.get("tables", [])
                    logger.info(f"Extracted {len(tables)} tables from model.bim")
                    
                    for t in tables:
                        logger.info(f" - Table: {t.get('name')}")
                        cols = t.get("columns", [])
                        logger.info(f"   Columns: {len(cols)}")
                        # Debug first few columns
                        for c in cols[:3]:
                             logger.info(f"     * {c.get('name')} ({c.get('dataType')})")

if __name__ == "__main__":
    main()
