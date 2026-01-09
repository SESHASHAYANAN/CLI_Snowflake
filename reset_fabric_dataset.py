
import logging
from semantic_sync.config.settings import get_settings
from semantic_sync.core.fabric_client import FabricClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    settings = get_settings()
    config = settings.get_fabric_config()
    client = FabricClient(config)
    
    logger.info(f"Dataset ID: {config.dataset_id}")
    
    # Get current tables via REST (names only)
    tables = client.get_dataset_tables(config.dataset_id)
    logger.info(f"Found {len(tables)} tables to delete")
    
    for table in tables:
        table_name = table["name"]
        logger.info(f"Deleting table: {table_name}")
        try:
            # DELETE /datasets/{datasetId}/tables/{tableName}
            endpoint = f"/groups/{config.workspace_id}/datasets/{config.dataset_id}/tables/{table_name}"
            client.delete(endpoint)
            logger.info(f"Deleted {table_name}")
        except Exception as e:
            logger.error(f"Failed to delete {table_name}: {e}")

if __name__ == "__main__":
    main()
