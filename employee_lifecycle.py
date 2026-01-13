import logging
import os
import time
from scripts.db_connector import *
from scripts.generate_dirty_data import *
# Bronze
from scripts.bronze_main_load import *
from scripts.bronze_tmp_load import *
# Silver
from scripts.silver_transformations import *
from scripts.silver_main_load import *
from scripts.silver_tmp_load import *
# CDC
from scripts.silver_cdc_detect import *
from scripts.process_cdc import *
# Gold
from scripts.gold_main_load import *

# Configuration
SIMULATE_SOURCE_SYSTEM = True 

# Create log directory if it doesn't exist
log_dir = os.path.join("output", "logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

# Logging configuration for both file and console output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'pipeline_debug.log'), mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("MAIN_PIPELINE")

def execute_etl_steps(cycle_name):
    """Orchestrates all ETL steps for a specific cycle."""
    try:
        logger.info(f"=== Starting {cycle_name} ETL ===")
        
        # Bronze
        load_bronze_layer()
        load_bronze_tmp_layer()
        
        # Silver
        load_silver_layer()
        load_silver_tmp_layer()
        
        # CDC
        create_cdc_view()
        process_cdc_changes()
        
        # Gold
        load_gold_layer()
        
        logger.info(f"=== Completed {cycle_name} ETL ===")
    except Exception as e:
        logger.error(f"Error during {cycle_name}: {e}")
        raise e

def run_pipeline():
    logger.info("--------------------------------------------------")
    logger.info("Employee Lifecycle Data Pipeline Started")
    logger.info("--------------------------------------------------")
    
    start_time = time.time()
    
    # Check if this is the first run for the Demo mode
    is_first_run = not os.path.exists(os.path.join("sources", "source_master.csv"))

    try:
        # CYCLE 1: Initial Load (or normal run)
        if SIMULATE_SOURCE_SYSTEM:
            logger.info("Step 1: Simulation Mode - Generating Data...")
            generate_data()
            logger.info("Data generated successfully.")
        else:
            source_file = os.path.join("sources", "employees_incoming.csv")
            if not os.path.exists(source_file):
                raise FileNotFoundError(f"Source file '{source_file}' not found!")

        execute_etl_steps("CYCLE 1 (Initial)")

        # CYCLE 2: Auto-Demo Mode for CDC (Only if first time and simulation is ON)
        if is_first_run and SIMULATE_SOURCE_SYSTEM:
            logger.info("--------------------------------------------------")
            logger.info("DEMO MODE: Simulating Day 2 (CDC Updates)...")
            logger.info("--------------------------------------------------")
            
            time.sleep(2) # For better log readability
            
            # Generate new data (If master file exists, it will switch to UPDATE mode)
            generate_data() 
            
            # Run ETL again to process updates
            execute_etl_steps("CYCLE 2 (Updates)")

        # Final stats
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        logger.info("--------------------------------------------------")
        logger.info(f"Pipeline Finished Successfully. Total Duration: {duration} seconds")
        logger.info("--------------------------------------------------")

    except Exception as e:
        logger.error(f"Critical Pipeline Failure: {e}")
        raise e

if __name__ == "__main__":
    run_pipeline()