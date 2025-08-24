# from fastapi import FastAPI, HTTPException, Query
# import sqlite3
# import pandas as pd
# from feature_store import (
#     init_feature_store,
#     register_features,
#     get_feature_metadata,
#     FEATURE_DEFINITIONS,
# )

# # DB_FILE = "../customer_db.sqlite"
# DB_FILE = "/opt/airflow/dags/assignment_telco/customer_db.sqlite"

# app = FastAPI(title="Feature Store API", version="1.3.0")


# # ---------- Helper: Get DB connection ----------
# def get_connection():
#     return sqlite3.connect(DB_FILE)


# # ---------- Startup: Initialize metadata + generate docs ----------
# @app.on_event("startup")
# def startup_event():
#     init_feature_store()
#     register_features()

#     # Generate documentation file dynamically from metadata
#     df = get_feature_metadata()
#     with open("feature_store.md", "w", encoding="utf-8") as f:
#         f.write("Feature Store Metadata\n\n")
#         f.write(df.to_markdown(index=False))
#     print("feature_store.md generated")


# # ---------- API: Get feature metadata ----------
# @app.get("/metadata")
# def get_metadata():
#     df = get_feature_metadata()
#     return df.to_dict(orient="records")


# # ---------- API: Get features for a single customer ----------
# @app.get("/features/{customer_id}")
# def get_features(customer_id: str):
#     conn = get_connection()
#     feature_names = [f[0] for f in FEATURE_DEFINITIONS]  # use central list
#     query = f"""
#     SELECT customerID, {', '.join(feature_names)}
#     FROM processed_data
#     WHERE customerID = ?
#     """
#     df = pd.read_sql_query(query, conn, params=(customer_id,))
#     conn.close()

#     if df.empty:
#         raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")

#     return df.to_dict(orient="records")[0]


# # ---------- API: Bulk feature retrieval (POST, JSON body) ----------
# @app.post("/features")
# def get_features_bulk(customer_ids: list[str]):
#     if not customer_ids:
#         raise HTTPException(status_code=400, detail="Customer IDs list cannot be empty")

#     conn = get_connection()
#     feature_names = [f[0] for f in FEATURE_DEFINITIONS]
#     placeholders = ",".join("?" * len(customer_ids))
#     query = f"""
#     SELECT customerID, {', '.join(feature_names)}
#     FROM processed_data
#     WHERE customerID IN ({placeholders})
#     """
#     df = pd.read_sql_query(query, conn, params=customer_ids)
#     conn.close()

#     if df.empty:
#         raise HTTPException(status_code=404, detail="No matching customers found")

#     return df.to_dict(orient="records")


# # ---------- API: Bulk feature retrieval (GET, query param) ----------
# @app.get("/features_bulk")
# def get_features_bulk_query(ids: str = Query(..., description="Comma-separated list of customer IDs")):
#     customer_ids = ids.split(",")
#     if not customer_ids:
#         raise HTTPException(status_code=400, detail="Customer IDs list cannot be empty")

#     conn = get_connection()
#     feature_names = [f[0] for f in FEATURE_DEFINITIONS]
#     placeholders = ",".join("?" * len(customer_ids))
#     query = f"""
#     SELECT customerID, {', '.join(feature_names)}
#     FROM processed_data
#     WHERE customerID IN ({placeholders})
#     """
#     df = pd.read_sql_query(query, conn, params=customer_ids)
#     conn.close()

#     if df.empty:
#         raise HTTPException(status_code=404, detail="No matching customers found")

#     return df.to_dict(orient="records")


# # ---------- API: Health check ----------
# @app.get("/health")
# def health_check():
#     return {"status": "Feature Store is running!"}
