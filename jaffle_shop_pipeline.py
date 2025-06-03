import dlt
import time
from datetime import datetime
import requests
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)
logger = logging.getLogger(__name__)

# Base URL for the Jaffle Shop API
BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"

def get_paginated_data(endpoint: str):
    """Helper function to handle pagination for any endpoint"""
    url = f"{BASE_URL}/{endpoint}"
    page = 1
    
    while True:
        response = requests.get(url, params={'page': page})
        response.raise_for_status()
        data = response.json()
        
        yield data
        
        if not data or len(data) == 0:
            logger.info(f"Finished fetching {endpoint} data")
            break
            
        page += 1

@dlt.resource(table_name="customers", write_disposition="merge", primary_key="id")
def get_customers():
    """Get all customers from the Jaffle Shop API"""
    logger.info("Starting customers data extraction")
    for page in get_paginated_data('customers'):
        yield page
    logger.info("Completed customers data extraction")

@dlt.resource(table_name="orders", write_disposition="merge", primary_key="id")
def get_orders():
    """Get all orders from the Jaffle Shop API"""
    logger.info("Starting orders data extraction")
    for page in get_paginated_data('orders'):
        yield page
    logger.info("Completed orders data extraction")

@dlt.source
def jaffle_shop_source():
    """Source combining all Jaffle Shop resources"""
    return get_customers #, get_orders()

def run_pipeline():
    """Run the pipeline and measure performance"""
    # Initialize stats dictionary
    stats = {
        "pipeline_name": "jaffle_shop",
        "timestamp": datetime.now().isoformat(),
        "stages": {}
    }
    
    logger.info("Initializing pipeline")
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop",
        destination="duckdb",
        dataset_name="jaffle_shop_data",
        progress="log"
    )
    
    # Measure extract time
    logger.info("Starting extraction stage")
    extract_start = time.time()
    pipeline.extract(jaffle_shop_source())
    extract_time = time.time() - extract_start
    stats["stages"]["extract"] = {
        "time_seconds": extract_time
    }
    logger.info(f"Extraction completed in {extract_time:.2f}s")
    
    # Measure normalize time
    logger.info("Starting normalization stage")
    normalize_start = time.time()
    pipeline.normalize()
    normalize_time = time.time() - normalize_start
    stats["stages"]["normalize"] = {
        "time_seconds": normalize_time
    }
    logger.info(f"Normalization completed in {normalize_time:.2f}s")
    
    # Measure load time
    logger.info("Starting load stage")
    load_start = time.time()
    load_time = time.time() - load_start
    stats["stages"]["load"] = {
        "time_seconds": load_time
    }
    logger.info(f"Load completed in {load_time:.2f}s")
    
    # Add total time and trace
    stats["total_time_seconds"] = extract_time + normalize_time + load_time
    stats["pipeline_trace"] = pipeline.last_trace
    
    # Print detailed stats
    logger.info("\n\n=== Pipeline Performance Stats ===")
    logger.info(f"Pipeline: {stats['pipeline_name']}")
    logger.info(f"Timestamp: {stats['timestamp']}")
    logger.info("\nStage Times:")
    logger.info(f"Extract: {extract_time:.2f}s")
    logger.info(f"Normalize: {normalize_time:.2f}s")
    logger.info(f"Load: {load_time:.2f}s")
    logger.info(f"\nTotal Time: {stats['total_time_seconds']:.2f}s")

if __name__ == "__main__":
    run_pipeline() 