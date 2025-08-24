import os
import pandas as pd
import logging
from datetime import datetime

# --- Paths ---
BASE_DIR = "/opt/airflow/logs/assignment_telco/"
RAW_DATA_PATH = os.path.join(BASE_DIR, "3_raw_data")
VALIDATION_REPORTS_PATH = os.path.join(BASE_DIR, "4_data_validation/validation_reports")
os.makedirs(VALIDATION_REPORTS_PATH, exist_ok=True)

# -------------------------
# Setup logging
# -------------------------
logger = logging.getLogger("data_validation")
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

def validate_csv(file_path: str) -> str:
    """
    Validate a CSV file and generate a validation report.
    Returns the path of the validation report.
    """
    try:
        df = pd.read_csv(file_path)

        validation_results = {}

        # Missing values
        validation_results["missing_values"] = df.isnull().sum().to_dict()

        # Duplicates
        validation_results["duplicates_count"] = int(df.duplicated().sum())

        # Data types
        validation_results["data_types"] = df.dtypes.astype(str).to_dict()

        # Basic statistics (flatten into plain dict of scalars)
        desc = df.describe(include="all").to_dict()
        summary_stats = {}
        for col, stats in desc.items():
            summary_stats[col] = {k: (v.item() if hasattr(v, "item") else v) for k, v in stats.items()}
        validation_results["summary_stats"] = summary_stats

        subfolder_name = os.path.basename(os.path.dirname(file_path))
        report_file = os.path.join(
            VALIDATION_REPORTS_PATH,
            f"{subfolder_name}_validation_report_{os.path.basename(file_path).split('.')[0]}.csv"
        )

        # Convert nested dicts into DataFrame
        pd.json_normalize(validation_results).to_csv(report_file, index=False)

        logger.info(f"Validation successful. Report saved: {report_file}")
        print(f"Validation report generated: {report_file}")

        return report_file

    except Exception as e:
        logger.error(f"Error validating {file_path}: {e}")
        print(f"Error validating {file_path}: {e}")
        return None

def run_validation():
    """
    Run validation on specified CSV files in the raw data directory.
    """
    logger.info("Starting validation process...")

    # Directories to process
    SUBFOLDERS = ["csv", "db"]
    WATCH_FILES = [os.path.join(RAW_DATA_PATH, sub, "latest.csv") for sub in SUBFOLDERS]

    for file in WATCH_FILES:
        if os.path.exists(file):
            logger.info(f"Processing file: {file}")
            try:
                validate_csv(file)
                logger.info(f"Validation completed for {file}")
            except Exception as e:
                logger.error(f"Validation failed for {file}: {e}")
        else:
            logger.warning(f"File not found: {file}")
            print(f"File not found: {file}")

if __name__ == "__main__":
    run_validation()