DROP TABLE IF EXISTS processed_data;

CREATE TABLE processed_data (
    customerID TEXT PRIMARY KEY,
    gender TEXT,
    SeniorCitizen INTEGER,
    Partner TEXT,
    Dependents TEXT,
    PhoneService TEXT,
    MultipleLines TEXT,
    InternetService TEXT,
    OnlineSecurity TEXT,
    OnlineBackup TEXT,
    DeviceProtection TEXT,
    TechSupport TEXT,
    StreamingTV TEXT,
    StreamingMovies TEXT,
    Contract TEXT,
    PaperlessBilling TEXT,
    PaymentMethod TEXT,
    Churn TEXT,
    tenure INTEGER,
    MonthlyCharges REAL,
    TotalCharges REAL,
    
    -- Engineered Features
    AvgChargesPerMonth REAL,
    ExtraCharges REAL,
    LifetimeValue REAL,
    Tenure_Charges_Interaction REAL
);
