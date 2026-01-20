# ETL Employee Lifecycle Automation
### Medallion Architecture | CDC Implementation
**Employee Lifecycle ETL Automation with CDC & Medallion Architecture (Bronze-Silver-Gold)**

## Directory Structure

```text
.
├── scripts/                        # Core Data Engineering Logic
│   ├── db_connector.py             # Centralized database connection management
│   ├── bronze_main_load.py         # Full ingestion from CSV to Bronze (Raw) layer
│   ├── bronze_tmp_load.py          # Staging area for initial data loads
│   ├── silver_main_load.py         # Primary logic for Silver layer processing
│   ├── silver_tmp_load.py          # Temporary staging for CDC transformations
│   ├── silver_cdc_detect.py        # Logic for identifying New vs Updated records
│   ├── silver_transformations.py   # Data cleaning, casting, and validation rules
│   ├── process_cdc.py              # Orchestrator for the Change Data Capture flow
│   ├── gold_main_load.py           # Final aggregations for business KPIs
│   └── generate_dirty_data.py      # Script to simulate real-world data issues
│
├── sources/                        # Data Input Files
│   ├── employees_incoming.csv      # Current HR data input
│   └── source_master.csv           # Previous snapshot for delta detection
│
├── output/logs/                    # Execution Artifacts
│   └── pipeline_debug.log          # Detailed technical logs of the ETL process
│
├── docker-compose.yaml             # PostgreSQL container orchestration
├── requirements.txt                # Python library dependencies (Pandas, Psycopg2, etc.)
├── .env.example                    # Template for environment variables (DB Credentials)
├── .gitignore                      # Git exclusion rules (Excludes __pycache__, .env, etc.)
└── README.md                       # Project documentation
```

## Architecture & Data Modeling

The project follows the **Medallion Architecture** to ensure data quality and traceability as it moves through different stages:

| Layer | Table Name | Purpose | Model Type |
| :--- | :--- | :--- | :--- |
| **Bronze** | `raw_employees` | Initial ingestion of raw CSV data  | Source (Raw) |
| **Silver** | `dim_employees` | Cleaned, validated, and de-duplicated data  | Dimension |
| **Gold** | `fact_salary_analytics` | Aggregated business KPIs (Dept costs, hiring trends)  | Fact |

### CDC & Simulation Logic
Unlike static pipelines, this system identifies changes between the incoming data and the existing database:
- **New Records:** Automatically identified and inserted into the system.
- **Updates:** Tracks changes in employee details (e.g., department changes) and updates the Silver layer accordingly.
- **Dirty Data Simulation:** Includes a custom script to generate synthetic "dirty" data to test ETL robustness against edge cases.

## 1) Summary

**Employee Lifecycle ETL Automation** is a production-grade data engineering pipeline designed to orchestrate the end-to-end lifecycle of employee records. The project implements a modern **Medallion Architecture** on a PostgreSQL environment, transforming volatile raw inputs into reliable, analytics-ready datasets.

### **Key Technical Highlights:**
* **Medallion Architecture:** Data flows through **Bronze** (Raw Ingestion), **Silver** (Cleaned/Validated), and **Gold** (Aggregated Business KPIs) layers to ensure high data integrity.
* **Advanced CDC (Change Data Capture):** Features a robust delta-processing mechanism that identifies and synchronizes changes (Inserts/Updates) between incoming snapshots and the existing database state.
* **Infrastructure as Code (IaC):** The entire database environment is fully containerized using **Docker Compose**, ensuring seamless deployment and reproducibility across any local or cloud environment.
* **Data Quality & Stress Testing:** Includes a dedicated simulation engine to generate "dirty" datasets, testing the pipeline's resilience against real-world data anomalies and edge cases.
* **Industrial Logging:** Implements a comprehensive logging system for full observability, tracking every stage of the ETL process for auditing and debugging.

## 2) How to Run

### Prerequisites
Docker & Docker Compose: For PostgreSQL orchestration.

Python 3.x: To execute ETL scripts.

Python Libraries: (Installed via requirements.txt)

- pandas: For data manipulation and CSV processing.

- psycopg2-binary: For PostgreSQL database connectivity.

- python-dotenv: For managing environment variables.

### Infrastructure Setup

Spin up the PostgreSQL instance on port 5433:
```bash
docker-compose up -d
```

### Environment Configuration

Rename .env.example to .env and provide your credentials:
```bash
cp .env.example .env
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Execute the Pipeline

Run the main orchestrator to trigger the full flow:
```bash
python employee_lifecycle.py
```

## 3) Test Safely
The project is designed with a "Safe-to-Test" mindset:

- Containerized DB: No changes are made to your local machine; everything stays inside Docker.

- Dry Run Logic: The simulation scripts allow you to see how CDC reacts to changes before moving to production-scale data.
