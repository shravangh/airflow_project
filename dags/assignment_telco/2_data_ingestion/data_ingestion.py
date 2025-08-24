import os
import sqlite3
import pandas as pd
import logging

# -------------------------
# Setup logging
# -------------------------
logger = logging.getLogger("data_ingestion")
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

# -------------------------
# Writable directories (inside /opt/airflow/logs, not dags/)
# -------------------------
BASE_DIR = "/opt/airflow/logs/assignment_telco"
RAW_DIR_CSV = os.path.join(BASE_DIR, "3_raw_data", "csv")
RAW_DIR_DB = os.path.join(BASE_DIR, "3_raw_data", "db")

os.makedirs(RAW_DIR_CSV, exist_ok=True)
os.makedirs(RAW_DIR_DB, exist_ok=True)

# -------------------------
# Ingestion functions
# -------------------------
def ingest_csv(csv_path):
    try:
        df = pd.read_csv(csv_path)

        raw_file = os.path.join(RAW_DIR_CSV, "latest.csv")
        df.to_csv(raw_file, index=False)

        logger.info(f"CSV ingestion successful: {raw_file}")
        return df
    except Exception as e:
        logger.error(f"CSV ingestion failed: {e}")
        return None

def ingest_sqlite(sqlite_path, table_name):
    try:
        conn = sqlite3.connect(sqlite_path)
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        raw_file = os.path.join(RAW_DIR_DB, "latest.csv")
        df.to_csv(raw_file, index=False)

        logger.info(f"SQLite ingestion successful from table {table_name}: {raw_file}")
        return df
    except Exception as e:
        logger.error(f"SQLite ingestion failed from table {table_name}: {e}")
        return None

# -------------------------
# Main ingestion pipeline
# -------------------------
def run_ingestion():
    # These should stay in /opt/airflow/dags (mounted from your host)
    CSV_PATH = "/opt/airflow/dags/assignment_telco/csv_data.csv"
    SQLITE_PATH = "/opt/airflow/dags/assignment_telco/customer_db.sqlite"
    TABLE_NAME = "customer_data"

    logger.info("Starting data ingestion job")
    
    df_csv = ingest_csv(CSV_PATH)
    df_sql = ingest_sqlite(SQLITE_PATH, TABLE_NAME)

    if df_csv is not None and df_sql is not None:
        logger.info("Data ingestion completed successfully.")
    else:
        logger.warning("Data ingestion completed with errors.")

if __name__ == "__main__":
    run_ingestion()
    logger.info("Ingestion job finished.")
