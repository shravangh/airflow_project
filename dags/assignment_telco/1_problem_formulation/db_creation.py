import sqlite3
import pandas as pd
import os
import stat
import logging

# Setup logging
logger = logging.getLogger("create_sqlite_from_sql")
logger.setLevel(logging.INFO)
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

# File paths
# BASE_DIR = "/opt/airflow/logs/assignment_telco"  # Use logs directory for writable storage
DB_FILE = "/opt/airflow/logs/assignment_telco/customer_db_test.sqlite"
SQL_FILE = "/opt/airflow/dags/assignment_telco/1_problem_formulation/insert_data.sql"
TABLE_NAME = "customer_data"

def create_db_file(db_file, sql_file):
    # Connect to DB (creates file if not exists)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Read SQL file
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_script = f.read()

    # Execute SQL script
    cursor.executescript(sql_script)

    # Commit and close
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_db_file(DB_FILE, SQL_FILE)