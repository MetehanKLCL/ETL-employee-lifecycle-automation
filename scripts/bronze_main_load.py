import pandas as pd
import logging
import os
from scripts.db_connector import db_connection, setup_logging

#Logger Setup
logger = logging.getLogger(__name__)

def load_bronze_layer():
    conn = None
    try:
        conn = db_connection()
        cursor = conn.cursor()
        logger.info("Database connection established for Bronze Layer.")

        #STEP 1: READ DATA FROM NEW SOURCE FOLDER
        csv_path = os.path.join("sources", "employees_incoming.csv")
        
        if not os.path.exists(csv_path):
            error_msg = f"Source file not found at: {csv_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        df = pd.read_csv(csv_path)
        
        #NaN to None
        df = df.where(pd.notnull(df), None)
        logger.info(f"CSV file read successfully from '{csv_path}'. Rows: {len(df)}")

        #STEP 2: SCHEMA REFRESH
        cursor.execute("DROP TABLE IF EXISTS bronze.employees;")
        conn.commit()
        logger.info("Old Bronze table dropped.")

        create_table_query = '''
        CREATE TABLE IF NOT EXISTS bronze.employees (
            employee_id INT,
            first_name TEXT,
            last_name TEXT,
            department TEXT,
            email TEXT,
            phone TEXT,
            status TEXT,
            salary TEXT,
            joining_date TEXT,
            termination_date TEXT,
            address TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Bronze table created successfully.")

        #STEP 3: DATA INGESTION
        cursor.execute("TRUNCATE TABLE bronze.employees;")
        conn.commit()
        
        insert_query = '''
        INSERT INTO bronze.employees (
            employee_id, first_name, last_name, department, email, phone, 
            status, salary, joining_date, termination_date, address
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        data_tuples = []
        for _, row in df.iterrows():
            data_tuples.append((
                row["employee_id"],
                row["first_name"],
                row["last_name"],
                row["department"],
                row["email"],
                row["phone"],
                row["status"],
                row["salary"],
                row["joining_date"],
                row["termination_date"],
                row["address"]
            ))

        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        logger.info(f"{len(data_tuples)} rows inserted into Bronze Layer successfully.")

    except Exception as e:
        logger.error(f"Bronze Load Failed: {e}")
        if conn:
            conn.rollback()
            logger.warning("Transaction rolled back due to error.")
        raise e

    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("Database connection closed.")

# Only setup logging if run directly (Standalone Mode)
if __name__ == "__main__":
    setup_logging("bronze_main_load")
    load_bronze_layer()