import logging
from scripts.db_connector import db_connection, setup_logging

logger = logging.getLogger(__name__)

def create_cdc_view():
    conn = None
    try:
        conn = db_connection()
        cursor = conn.cursor()
        logger.info("Connection established for CDC View creation.")

        cdc_view_query = '''
        CREATE OR REPLACE VIEW employees_cdc_view AS
        SELECT
            COALESCE(new.employee_id, old.employee_id) AS employee_id, 

            CASE 
                WHEN old.employee_id IS NULL THEN 'INSERT'
                WHEN new.employee_id IS NULL THEN 'DELETE'
                WHEN MD5(CAST((new.first_name, new.last_name, new.salary, new.department, new.status) AS TEXT)) 
                    IS DISTINCT FROM 
                    MD5(CAST((old.first_name, old.last_name, old.salary, old.department, old.status) AS TEXT))
                    THEN 'UPDATE'
                ELSE 
                    'NO_CHANGE'
            END AS cdc_action,

            new.first_name,
            new.last_name,
            new.department,
            new.email,
            new.phone,
            new.status,
            new.salary,
            new.joining_date,
            new.termination_date,
            new.address,
            new.status_flag
            
        FROM silver.tmp_employees AS new
        FULL OUTER JOIN silver.employees AS old
            ON new.employee_id = old.employee_id;
        '''
        
        cursor.execute(cdc_view_query)
        conn.commit()
        logger.info("CDC View (employees_cdc_view) created or updated successfully.")

    except Exception as e:
        logger.error(f"Error creating CDC View: {e}")
        if conn:
            conn.rollback()
        raise e

    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("Connection closed.")

if __name__ == "__main__":
    setup_logging("silver_cdc_detect")
    create_cdc_view()