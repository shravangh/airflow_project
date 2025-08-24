import sqlite3
import pandas as pd
import logging
from datetime import datetime

DB_FILE = "/opt/airflow/logs/assignment_telco/customer_db_test.sqlite"

# Setup logging
logger = logging.getLogger("feature_store")
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

# ---------- Central Feature Definitions ----------
FEATURE_DEFINITIONS = [
    ("AvgChargesPerMonth", "Average charges per tenure month", "processed_data", "v1"),
    ("ExtraCharges", "Difference between billed and expected charges", "processed_data", "v1"),
    ("LifetimeValue", "Total expected value of customer", "processed_data", "v1"),
    ("Tenure_Charges_Interaction", "Interaction between tenure and charges", "processed_data", "v1"),
]


# ---------- 1. Create metadata table ----------
def init_feature_store():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS feature_metadata (
        feature_name TEXT PRIMARY KEY,
        description TEXT,
        source TEXT,
        version TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    logger.info(f"Successfully created table feature_metadata for feature store")
    print(f"Successfully created table feature_metadata for feature store")

    conn.commit()
    conn.close()


# ---------- 2. Insert metadata ----------
def register_features():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    for f in FEATURE_DEFINITIONS:
        cursor.execute("""
        INSERT OR IGNORE INTO feature_metadata (feature_name, description, source, version)
        VALUES (?, ?, ?, ?)
        """, f)

    logger.info(f"Successfully inserted table features in feature_metadata table")
    print(f"Successfully inserted table features in feature_metadata table")
    conn.commit()
    conn.close()


# ---------- 3. Retrieve metadata ----------
def get_feature_metadata():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM feature_metadata", conn)
    conn.close()
    return df


# ---------- 4. Retrieve features for a customer ----------
def get_customer_features(customer_id: str):
    conn = sqlite3.connect(DB_FILE)
    feature_names = [f[0] for f in FEATURE_DEFINITIONS]
    query = f"""
    SELECT customerID, {', '.join(feature_names)}
    FROM processed_data
    WHERE customerID = ?
    """
    df = pd.read_sql_query(query, conn, params=(customer_id,))
    conn.close()
    return df


# ---------- 5. Bulk retrieval ----------
def get_bulk_features(customer_ids: list[str]):
    if not customer_ids:
        return pd.DataFrame()

    conn = sqlite3.connect(DB_FILE)
    feature_names = [f[0] for f in FEATURE_DEFINITIONS]
    placeholders = ",".join("?" * len(customer_ids))
    query = f"""
    SELECT customerID, {', '.join(feature_names)}
    FROM processed_data
    WHERE customerID IN ({placeholders})
    """
    df = pd.read_sql_query(query, conn, params=customer_ids)
    conn.close()
    return df


# ---------- Run once to initialize ----------
if __name__ == "__main__":
    print("Initializing Feature Store...")
    logger.info(f"Initializing Feature Store...")
    init_feature_store()
    register_features()

    print("\n Feature Metadata:")
    logger.info("\n Feature Metadata:")
    print(get_feature_metadata())

    # Example retrieval
    sample_customer = "7590-VHVEG"  # replace with an ID from your DB
    print(f"\n Features for customer {sample_customer}:")
    print(get_customer_features(sample_customer))
