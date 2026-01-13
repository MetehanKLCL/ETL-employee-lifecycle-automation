import logging
from scripts.db_connector import db_connection, setup_logging

# 1. Module-level logger setup
logger = logging.getLogger(__name__)

def create_department_kpi(cursor):
    """
    Scenario 1: Department-level salary and workforce analysis.
    Visibility into operational costs and employee distribution per department.
    """
    logger.info("--- Starting: Department KPI Table ---")
    
    cursor.execute("DROP TABLE IF EXISTS gold.department_kpi;")
    
    create_query = """
    CREATE TABLE gold.department_kpi (
        department VARCHAR(100),
        total_employees INT,
        active_employees INT,
        avg_salary NUMERIC(10, 2),
        total_salary_cost INT,
        avg_tenure_days INT,
        last_updated DATE
    );
    """
    cursor.execute(create_query)
    
    # Finding the total salary for active employees
    insert_query = """
    INSERT INTO gold.department_kpi
    SELECT 
        department,
        COUNT(*) as total_employees,
        COUNT(CASE WHEN status = 'Active' THEN 1 END) as active_employees,
        ROUND(AVG(salary), 2) as avg_salary,
        SUM(CASE WHEN status = 'Active' THEN salary ELSE 0 END) as total_salary_cost,
        ROUND(AVG(
                CASE 
                    WHEN status = 'Terminated' AND termination_date IS NOT NULL THEN termination_date - joining_date
                    ELSE CURRENT_DATE - joining_date 
                END
            ) FILTER (WHERE joining_date IS NOT NULL))::INT as avg_tenure_days,
        CURRENT_DATE
    FROM silver.employees
    WHERE status_flag = FALSE
    GROUP BY department
    ORDER BY total_salary_cost DESC;
    """
    
    cursor.execute(insert_query)
    logger.info(f"Department KPIs calculated. {cursor.rowcount} departments processed.")


def create_hiring_trends(cursor):
    """
    Scenario 2: Yearly growth and hiring trend analysis.
    Tracking company expansion, hiring volume, and salary trends over time.
    """
    logger.info("--- Starting: Hiring Trends Table ---")
    
    cursor.execute("DROP TABLE IF EXISTS gold.hiring_trends;")
    
    create_query = """
    CREATE TABLE gold.hiring_trends (
        hiring_year INT,
        total_hires INT,
        avg_starting_salary NUMERIC(10, 2),
        most_hired_dept VARCHAR(100),
        last_updated DATE
    );
    """
    cursor.execute(create_query)
    
    # Finding which year has the most hiring value for each department
    insert_query = """
    INSERT INTO gold.hiring_trends
    SELECT 
        COALESCE(EXTRACT(YEAR FROM joining_date), -1) as hiring_year,
        COUNT(*) as total_hires,
        ROUND(AVG(salary), 2) as avg_starting_salary,
        mode() WITHIN GROUP (ORDER BY department) as most_hired_dept,
        CURRENT_DATE as last_updated
    FROM silver.employees
    GROUP BY 1
    ORDER BY 1 DESC;
    """
    
    cursor.execute(insert_query)
    logger.info(f"Hiring trends calculated. {cursor.rowcount} years processed.")


def load_gold_layer():
    conn = None
    try:
        conn = db_connection()
        cursor = conn.cursor()
        logger.info("Gold Layer ETL Started.")

        create_department_kpi(cursor)
        create_hiring_trends(cursor)

        conn.commit()
        logger.info("Gold Layer processing complete.")

    except Exception as e:
        logger.error(f"Gold Layer Failed: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("Connection closed.")

# 2. Standalone setup moved to the bottom
if __name__ == "__main__":
    setup_logging("gold_main_load")
    load_gold_layer()