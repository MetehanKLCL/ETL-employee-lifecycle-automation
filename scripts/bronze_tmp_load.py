import pandas as pd
import logging
import os
from scripts.db_connector import db_connection, setup_logging

logger = logging.getLogger(__name__)

def load_bronze_tmp_layer():
    conn = None
    try:
        conn = db_connection()
        cursor = conn.cursor()
        logger.info("Database connection established for Bronze TMP Layer.")

        #STEP 1: SCHEMA REFRESH
        cursor.execute("DROP TABLE IF EXISTS bronze.tmp_employees;")
        conn.commit()
        logger.info("Old TMP table dropped.")

        # Creating the temporary table
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS bronze.tmp_employees (
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
            address TEXT
        )
        '''
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Bronze TMP table created successfully.")

        # Ensure table is empty
        cursor.execute("TRUNCATE TABLE bronze.tmp_employees;")
        conn.commit()
        
        #STEP 2: READ SOURCE FILE
        file_path = os.path.join("sources", "employees_incoming.csv")
        
        if not os.path.exists(file_path):
            error_msg = f"Source file not found: {file_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        df = pd.read_csv(file_path)
        
        #NaN to Null
        df = df.where(pd.notnull(df), None)
        logger.info(f"CSV file read successfully. Rows to insert: {len(df)}")

        # STEP 3: BULK INSERTION
        insert_query = '''
        INSERT INTO bronze.tmp_employees (
            employee_id, first_name, last_name, department, email, phone, 
            status, salary, joining_date, termination_date, address
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        # Converting DataFrame rows to a list of tuples for executemany
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

        #Executemany is significantly faster than looping with execute
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        logger.info(f"Successfully inserted {len(data_tuples)} rows into 'bronze.tmp_employees'.")

    except Exception as e:
        logger.error(f"Bronze TMP Load Failed: {e}")
        if conn:
            conn.rollback()
            logger.warning("Transaction rolled back.")
        raise e

    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("Connection closed.")

# Only run setup_logging if this file is executed directly
if __name__ == "__main__":
    setup_logging("bronze_tmp_load")
    load_bronze_tmp_layer()