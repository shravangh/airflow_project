import os
import sqlite3
import pandas as pd
import logging
import stat

# Paths
BASE_DIR = "/opt/airflow/logs/assignment_telco"
CSV_FILE = os.path.join(BASE_DIR, "5_data_preparation/cleaned_processed_data.csv")
DB_FILE = "/opt/airflow/logs/assignment_telco/customer_db_test.sqlite"
TABLE_NAME = "processed_data"
SCHEMA_FILE = "/opt/airflow/dags/assignment_telco/6_data_storage/schema.sql"
DATA_STORAGE_PATH = os.path.join(BASE_DIR, "6_data_storage")
os.makedirs(DATA_STORAGE_PATH, exist_ok=True)
SUMMARY_FILE = os.path.join(DATA_STORAGE_PATH, "transformation_summary.txt")

# Setup logging
logger = logging.getLogger("data_storage")
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

def ensure_db_writable(db_path: str):
    """
    Ensure the database file and its directory are writable.
    """
    try:
        db_dir = os.path.dirname(db_path)
        os.makedirs(db_dir, exist_ok=True)

        # Create the database file if it doesn't exist
        if not os.path.exists(db_path):
            with open(db_path, 'a'):
                os.utime(db_path, None)  # Update timestamp
            logger.info(f"Created new database file: {db_path}")

        # Check if the file is writable
        if os.access(db_path, os.W_OK):
            logger.info(f"Database file is already writable: {db_path}")
        else:
            # Attempt to set writable permissions (rw-rw-r--)
            try:
                os.chmod(db_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
                logger.info(f"Set writable permissions for: {db_path}")
            except PermissionError as e:
                logger.warning(f"Could not change permissions for {db_path}: {e}. Checking if file is still usable.")
                if not os.access(db_path, os.W_OK):
                    raise PermissionError(f"Database file {db_path} is not writable and permissions could not be changed.")

    except Exception as e:
        logger.error(f"Failed to ensure database is writable: {e}")
        raise

def add_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds aggregated spend features and interaction features 
    to the Telco Customer Churn dataset.
    """
    # Avoid division by zero
    df["AvgChargesPerMonth"] = df.apply(
        lambda x: x["TotalCharges"] / x["tenure"] if x["tenure"] > 0 else 0, axis=1
    )
    
    # Difference between billed and expected
    df["ExtraCharges"] = df["TotalCharges"] - (df["MonthlyCharges"] * df["tenure"])
    
    # Lifetime value approximation
    df["LifetimeValue"] = df["tenure"] * df["MonthlyCharges"]
    
    # Interaction of tenure & charges (scaled a bit)
    df["Tenure_Charges_Interaction"] = df["tenure"] * (df["MonthlyCharges"] / 100.0)

    df[["AvgChargesPerMonth", "ExtraCharges", "LifetimeValue", "Tenure_Charges_Interaction"]] = \
        df[["AvgChargesPerMonth", "ExtraCharges", "LifetimeValue", "Tenure_Charges_Interaction"]].round(2)
    
    return df

def write_summary():
    """
    Writes a human-readable summary of the transformation logic to a.txt
    """
    summary_text = """Summary of Transformation Logic Applied:

                    1. AvgChargesPerMonth:
                    - Computed as TotalCharges / tenure (avoiding division by zero).
                    - Represents the average charges per customer per month.

                    2. ExtraCharges:
                    - Difference between TotalCharges and (MonthlyCharges * tenure).
                    - Captures any extra/unexpected charges compared to expected billing.

                    3. LifetimeValue:
                    - tenure * MonthlyCharges.
                    - A proxy for the customer's total lifetime value to the company.

                    4. Tenure_Charges_Interaction:
                    - tenure * (MonthlyCharges / 100).
                    - Captures the interaction between tenure and monthly charges, scaled down.

                    5. All engineered features are rounded to 2 decimal places.

                    The transformed dataset is then stored in SQLite database:
                    - Database file: customer_db.sqlite
                    - Table name: processed_data
                    """
    with open(SUMMARY_FILE, "w") as f:
        f.write(summary_text)
    print(f"Transformation summary written to {SUMMARY_FILE}")
    logger.info(f"Transformation summary written to {SUMMARY_FILE}")

def store_data():
    """
    Process the CSV file, add engineered features, and store in SQLite database.
    """
    if not os.path.exists(CSV_FILE):
        logger.error(f"{CSV_FILE} not found. Run Data Preparation step first.")
        print(f"{CSV_FILE} not found. Run Data Preparation step first.")
        return

    # Load CSV into DataFrame
    df = pd.read_csv(CSV_FILE)

    # Add engineered features
    df = add_engineered_features(df)

    # Ensure database is writable
    ensure_db_writable(DB_FILE)

    # Connect to SQLite DB
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
    except sqlite3.OperationalError as e:
        logger.error(f"Failed to connect to database {DB_FILE}: {e}")
        print(f"Failed to connect to database {DB_FILE}: {e}")
        return

    # Run schema.sql to reset/create the schema
    if os.path.exists(SCHEMA_FILE):
        try:
            with open(SCHEMA_FILE, "r") as f:
                cursor.executescript(f.read())
            print("Schema applied successfully from schema.sql")
            logger.info("Schema applied successfully from schema.sql")
        except sqlite3.OperationalError as e:
            logger.error(f"Failed to apply schema from {SCHEMA_FILE}: {e}")
            print(f"Failed to apply schema from {SCHEMA_FILE}: {e}")
            conn.close()
            return

    # Insert transformed data into table
    try:
        df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
        conn.commit()
        print(f"Transformed data successfully stored in {DB_FILE}, table: {TABLE_NAME}")
        logger.info(f"Transformed data successfully stored in {DB_FILE}, table: {TABLE_NAME}")
    except sqlite3.OperationalError as e:
        logger.error(f"Failed to store data in {DB_FILE}: {e}")
        print(f"Failed to store data in {DB_FILE}: {e}")
    finally:
        conn.close()

    # Write transformation summary
    write_summary()

if __name__ == "__main__":
    store_data()