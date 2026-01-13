import pandas as pd
import random
import logging
import os
from faker import Faker
from datetime import datetime

# 1. Logger Setup: Uses the file's own identity for logging
logger = logging.getLogger(__name__)

def generate_data():
    fake = Faker()
    
    # --- NEW FOLDER STRUCTURE ---
    DATA_FOLDER = "sources"
    
    # Create folder if it doesn't exist
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        logger.info(f"Folder '{DATA_FOLDER}' created.")

    MASTER_DB_FILE = os.path.join(DATA_FOLDER, "source_master.csv")
    OUTPUT_FILE = os.path.join(DATA_FOLDER, "employees_incoming.csv")
    
    try:
        # --- SCENARIO 1: UPDATE SIMULATION (System already exists) ---
        if os.path.exists(MASTER_DB_FILE):
            logger.info(f"Master data found. Running UPDATE simulation...")
            
            df = pd.read_csv(MASTER_DB_FILE)
            
            # Select 20 random people to update
            sample_size = min(20, len(df))
            rows_to_update = df.sample(n=sample_size).index 
            
            updated_count = 0
            for index in rows_to_update:
                # 50% Chance: Salary Change
                if random.choice([True, False]):
                    try:
                        # Clean dirty data string (simple cleanup)
                        old_val = str(df.at[index, 'salary']).replace("$", "").replace(",", "")
                        current_val = int(float(old_val))
                        # Apply raise
                        df.at[index, 'salary'] = current_val + random.randint(500, 2000)
                        updated_count += 1
                    except: 
                        pass # Ignore if data is too corrupt

                # 30% Chance: Department Change
                if random.choice([True, False, False]):
                    deps = ["Engineering", "Sales", "Marketing", "HR", "Finance"]
                    df.at[index, 'department'] = random.choice(deps)

            # Update Master File (Refresh memory)
            df.to_csv(MASTER_DB_FILE, index=False)
            
            # Create fresh file for Pipeline ingestion
            df.to_csv(OUTPUT_FILE, index=False)
            
            logger.info(f"Data updated ({updated_count} records modified). '{OUTPUT_FILE}' prepared.")

        # --- SCENARIO 2: INITIAL LOAD (First run) ---
        else:
            logger.info("Master data not found. GENERATING DATA FROM SCRATCH (Initial Load)...")
            
            employee_list = []
            status_options = ["Active", "Terminated"]
            departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Legal", "Ops"]

            for i in range(200):
                current_status = random.choice(status_options)
                term_date = fake.date_between(start_date="-5y", end_date="today") if current_status == "Terminated" else None

                person = {
                    "employee_id": i + 1001,
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "department": random.choice(departments),
                    "email": fake.email(),
                    "phone": fake.phone_number(),
                    "status": current_status,
                    "salary": random.randint(3000, 10000),
                    "joining_date": fake.date_between(start_date="-5y", end_date="today"),
                    "termination_date": term_date,
                    "address": fake.address(),
                }
                
                # --- Corruption Logic (Legacy logic) ---
                corrupt = random.randint(1, 100)
                if corrupt <= 10: person["salary"] = f"${person['salary']}"
                elif corrupt > 90: person["email"] = "user_no_domain"

                employee_list.append(person)

            df = pd.DataFrame(employee_list)
            
            # Save to both Master (Storage) and Output (Pipeline Input)
            df.to_csv(MASTER_DB_FILE, index=False)
            df.to_csv(OUTPUT_FILE, index=False)
            
            logger.info(f"Initial dataset created and saved into '{DATA_FOLDER}'.")

    except Exception as e:
        logger.error(f"Data generation failed: {e}")
        raise e

# Setup logging if run standalone
if __name__ == "__main__":
    from db_connector import setup_logging
    setup_logging("generate_dirty_data")
    generate_data()