import logging
from scripts.db_connector import db_connection, setup_logging

logger = logging.getLogger(__name__)

def process_insert(cursor):
    logger.info("Checking for INSERT actions...")
    cdc_insert_query = '''
    INSERT INTO silver.employees (
        employee_id, first_name, last_name, department, email, phone,
        status, salary, joining_date, termination_date, status_flag, address, updated_at
    )
    SELECT
        employee_id, first_name, last_name, department, email, phone,
        status, salary, CURRENT_DATE, NULL, status_flag, address, CURRENT_DATE
    FROM employees_cdc_view
    WHERE cdc_action = 'INSERT'
    '''
    cursor.execute(cdc_insert_query)
    logger.info(f" -> {cursor.rowcount} rows inserted.")

def process_update(cursor):
    logger.info("Checking for UPDATE actions...")
    cdc_update_query = '''
    UPDATE silver.employees AS main
    SET 
        first_name = view.first_name,
        last_name = view.last_name,
        department = view.department,
        email = view.email,
        phone = view.phone,
        address = view.address,
        status_flag = view.status_flag,
        termination_date = CASE
            WHEN view.status = 'Terminated' AND main.status != 'Terminated' THEN CURRENT_DATE
            WHEN view.status = 'Active' THEN NULL
            ELSE main.termination_date
        END,
        updated_at = CURRENT_DATE,
        status = view.status
    FROM employees_cdc_view AS view
    WHERE main.employee_id = view.employee_id 
        AND view.cdc_action = 'UPDATE';
    '''
    cursor.execute(cdc_update_query)
    logger.info(f" -> {cursor.rowcount} rows updated.")

def process_delete(cursor):
    logger.info("Checking for DELETE actions...")
    cdc_delete_query = '''
    DELETE FROM silver.employees
    WHERE employee_id IN (
        SELECT employee_id
        FROM employees_cdc_view
        WHERE cdc_action = 'DELETE'
    )
    '''
    cursor.execute(cdc_delete_query)
    logger.info(f" -> {cursor.rowcount} rows deleted.")

def process_cdc_changes():
    conn = None
    try: 
        conn = db_connection()
        cursor = conn.cursor()
        logger.info("CDC transaction started.")

        process_insert(cursor)
        process_update(cursor)
        process_delete(cursor)

        cursor.execute("TRUNCATE TABLE silver.tmp_employees;")
        logger.info("Temporary silver table cleared.")

        conn.commit()
        logger.info("CDC transactions committed successfully.")

    except Exception as e:
        logger.error(f"CDC transaction failed: {e}")
        if conn:
            conn.rollback()
        raise e
        
    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("Connection closed.")

if __name__ == "__main__":
    setup_logging("process_cdc")
    process_cdc_changes()