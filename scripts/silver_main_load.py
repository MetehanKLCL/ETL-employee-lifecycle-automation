import logging
from datetime import date
from scripts.silver_transformations import *
from scripts.db_connector import db_connection, setup_logging

logger = logging.getLogger(__name__)

def load_silver_layer():
    conn = None
    try:
        conn = db_connection()
        cursor = conn.cursor()
        logger.info("Connection established.")

        cursor.execute("DROP TABLE IF EXISTS silver.employees CASCADE;")
        conn.commit()

        create_silver_table = '''
        CREATE TABLE IF NOT EXISTS silver.employees(
            employee_id INT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            department VARCHAR(100),
            email VARCHAR(200),
            phone VARCHAR(20),
            status VARCHAR(15),
            salary INT,
            joining_date DATE,
            termination_date DATE,
            status_flag BOOLEAN,
            address TEXT,
            updated_at DATE
        )
        '''
        cursor.execute(create_silver_table)
        conn.commit()
        logger.info("Silver table created.")

        cursor.execute("TRUNCATE TABLE silver.employees;")
        conn.commit()

        cursor.execute("SELECT * FROM bronze.employees;")
        raw_data = cursor.fetchall()

        insert_list = []
        for row in raw_data:
            employee_id = row[0]
            first_name = clean_names(row[1])
            last_name = clean_names(row[2])
            department = row[3]
            email = clean_email(row[4])
            phone = clean_phone(row[5])
            status = row[6]
            salary = clean_salary(row[7]) if row[7] is not None else None
            joining_date, termination_date, status_flag = clean_employment_dates(status, row[8], row[9])
            address = clean_address(row[10])
            updated_at = date.today()

            clean_list = (employee_id, first_name, last_name, department, email, phone, status, salary, joining_date, termination_date, status_flag, address, updated_at)
            insert_list.append(clean_list)

        silver_insert_query = '''
        INSERT INTO silver.employees
        (employee_id, first_name, last_name, department, email, phone,
          status, salary, joining_date, termination_date, status_flag, address, updated_at)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        cursor.executemany(silver_insert_query, insert_list)
        conn.commit()
        logger.info(f"Inserted {len(insert_list)} rows into Silver.")

    except Exception as e:
        logger.error(f"Silver load failed: {e}")
        if conn:
            conn.rollback()
        raise e

    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("Connection closed.")

if __name__ == "__main__":
    setup_logging("silver_main_load")
    load_silver_layer()