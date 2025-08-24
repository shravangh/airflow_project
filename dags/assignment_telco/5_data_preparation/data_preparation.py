import os
import pandas as pd
import numpy as np
import logging
import warnings
warnings.filterwarnings('ignore')

# --- Paths ---
BASE_DIR = "/opt/airflow/logs/assignment_telco"
RAW_CSV_PATH = os.path.join(BASE_DIR, "3_raw_data/csv/latest.csv")
RAW_DB_PATH = os.path.join(BASE_DIR, "3_raw_data/db/latest.csv")
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "5_data_preparation")
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

# -------------------------
# Setup logging
# -------------------------
logger = logging.getLogger("data_preparation")
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

def process_data(csv_path, db_path, output_path):
    """
    Process and merge CSV and database data, clean TotalCharges column, and save the result.
    
    Args:
        csv_path (str): Path to the CSV data file
        db_path (str): Path to the database data file
        output_path (str): Path to save the merged and cleaned data
    
    Returns:
        pandas.DataFrame: The processed and merged DataFrame
    """
    try:
        # Load data
        csv_data = pd.read_csv(csv_path)
        db_data = pd.read_csv(db_path)

        # Print column names
        print(f"CSV data columns: {csv_data.columns.tolist()}")
        logger.info(f"CSV data columns: {csv_data.columns.tolist()}")
        print(f"Database data columns: {db_data.columns.tolist()}")
        logger.info(f"Database data columns: {db_data.columns.tolist()}")

        # Clean TotalCharges column in db_data
        db_data["TotalCharges"] = db_data["TotalCharges"].astype(str).str.strip()
        db_data_dropped = db_data[~db_data["TotalCharges"].isna() & (db_data["TotalCharges"] != "")].reset_index(drop=True)

        # Print shape information
        print(f"Original db_data shape: {db_data.shape}")
        logger.info(f"Original db_data shape: {db_data.shape}")
        print(f"After dropping missing TotalCharges: {db_data_dropped.shape}")
        logger.info(f"After dropping missing TotalCharges: {db_data_dropped.shape}")

        # Merge datasets
        df = pd.merge(csv_data, db_data_dropped, how='inner', on='customerID')

        # Save the merged DataFrame
        cleaned_processed_file = os.path.join(PROCESSED_DATA_PATH, "cleaned_processed_data.csv")

        df.to_csv(cleaned_processed_file, index=False)
        print(f"Merged and cleaned data saved to: {output_path}")
        logger.info(f"Merged and cleaned data saved to: {output_path}")

        return df

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return None

if __name__ == "__main__":
    process_data(RAW_CSV_PATH, RAW_DB_PATH, PROCESSED_DATA_PATH)