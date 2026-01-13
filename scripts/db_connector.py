import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

def setup_logging(script_name):
    """
    Configures logging for standalone script execution.
    When called from main.py, this setup is usually bypassed 
    to maintain a single pipeline log file.
    """
    log_dir = os.path.join("output", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f"{script_name}.log")

    # Clear existing handlers to avoid duplicate logs
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename, mode='a', encoding='utf-8'), 
            logging.StreamHandler() 
        ]
    )
    logger.info(f"--- Log Session Started for {script_name} ---")

def db_connection():
    """
    Establishes a connection to the PostgreSQL database
    """
    try:
        conn = psycopg2.connect(
            host="localhost",
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("POSTGRES_PORT", "5433")
        )
        return conn
    
    except Exception as e:
        error_msg = f"Database Connection Failed: {e}"
        if logger.hasHandlers() or logging.getLogger().hasHandlers():
            logger.error(error_msg)
        else:
            print(f"CRITICAL: {error_msg}")
        raise e