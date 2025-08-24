import os
import sqlite3
import pandas as pd
import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.svm import SVC
from sklearn.metrics import classification_report, accuracy_score


BASE_DIR = "/opt/airflow/logs/assignment_telco"
DB_FILE_PATH = os.path.join(BASE_DIR, "customer_db_test.sqlite")
MLRUNS_PATH = os.path.join(BASE_DIR, "mlruns")
# Ensure mlruns folder exists
os.makedirs(MLRUNS_PATH, exist_ok=True)

# Set local tracking URI using MLRUNS_PATH
mlflow.set_tracking_uri(f"file:{MLRUNS_PATH}")

# Create / set experiment
mlflow.set_experiment("Churn_Prediction")

# --------------------------
# Step 1. Load Processed Data
# --------------------------
def load_processed_data(db_path) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql("SELECT * FROM processed_data", conn)
    finally:
        conn.close()
    return df


# --------------------------
# Step 2. Feature Encoding
# --------------------------
def prepare_train_test(df: pd.DataFrame):
    # Convert SeniorCitizen to string (so it’s categorical)
    df['SeniorCitizen'] = df['SeniorCitizen'].astype(str)

    # Encode target variable
    le = LabelEncoder()
    df['Churn'] = le.fit_transform(df['Churn'])

    # Split
    X = df.drop(columns=['customerID', 'Churn'])
    y = df['Churn']
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.30, random_state=40, stratify=y
    )

    # Categorical vs numerical split
    num_cols = ['tenure', 'MonthlyCharges', 'TotalCharges',
                'AvgChargesPerMonth', 'ExtraCharges',
                'LifetimeValue', 'Tenure_Charges_Interaction']

    # Train set
    X_train_cat = pd.get_dummies(
        X_train.drop(columns=num_cols), dtype=int
    )
    scaler = StandardScaler()
    X_train_num = pd.DataFrame(
        scaler.fit_transform(X_train[num_cols]),
        columns=num_cols, index=X_train.index
    )
    X_train_encoded = pd.concat([X_train_cat, X_train_num], axis=1)

    # Test set
    X_test_cat = pd.get_dummies(
        X_test.drop(columns=num_cols), dtype=int
    )
    # Align columns (important if categories differ between train/test)
    X_test_cat = X_test_cat.reindex(columns=X_train_cat.columns, fill_value=0)

    X_test_num = pd.DataFrame(
        scaler.transform(X_test[num_cols]),  # use transform (not fit_transform)
        columns=num_cols, index=X_test.index
    )
    X_test_encoded = pd.concat([X_test_cat, X_test_num], axis=1)

    return X_train_encoded, X_test_encoded, y_train, y_test


# --------------------------
# Step 3. Train & Log Model
# --------------------------
def train_and_log(X_train, X_test, y_train, y_test):

    # Train SVC model
    svc = SVC(probability=True, random_state=42)
    svc.fit(X_train, y_train)
    y_pred = svc.predict(X_test)
    acc = accuracy_score(y_test, y_pred)

    # Track run
    experiment = mlflow.get_experiment_by_name("Churn_Prediction")
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
    run_number = len(runs) + 1

    with mlflow.start_run(run_name=f"run_{run_number}"):
        mlflow.log_param("kernel", "rbf")
        mlflow.log_param("probability", True)
        mlflow.log_metric("accuracy", acc)

        mlflow.sklearn.log_model(
            sk_model=svc,
            artifact_path="svc_model",
            registered_model_name="Churn_SVC_Model"
        )

        print("Classification Report:\n", classification_report(y_test, y_pred))
        print(f"Logged to MLflow as run_{run_number} with Accuracy: {acc:.4f}")


# --------------------------
# Step 4. Run Script
# --------------------------
if __name__ == "__main__":
    df = load_processed_data(DB_FILE_PATH)
    X_train, X_test, y_train, y_test = prepare_train_test(df)
    train_and_log(X_train, X_test, y_train, y_test)
