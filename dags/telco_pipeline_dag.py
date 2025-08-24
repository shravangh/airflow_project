from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
}

with DAG(
    dag_id='telco_data_pipeline',
    default_args=default_args,
    description='Telco End-to-End Data Pipeline',
    schedule_interval=None,
    catchup=False,
) as dag:

    db_creation = BashOperator(
        task_id='db_creation',
        bash_command='python /opt/airflow/dags/assignment_telco/1_problem_formulation/db_creation.py'
    )

    ingestion = BashOperator(
        task_id='data_ingestion',
        bash_command='python /opt/airflow/dags/assignment_telco/2_data_ingestion/data_ingestion.py'
    )

    validation = BashOperator(
        task_id='data_validation',
        bash_command='python /opt/airflow/dags/assignment_telco/4_data_validation/data_validation.py'
    )

    preparation = BashOperator(
        task_id='data_preparation',
        bash_command='python /opt/airflow/dags/assignment_telco/5_data_preparation/data_preparation.py'
    )

    storage = BashOperator(
        task_id='data_storage',
        bash_command='python /opt/airflow/dags/assignment_telco/6_data_storage/data_storage.py'
    )

    feature_store = BashOperator(
        task_id='feature_store',
        bash_command='python /opt/airflow/dags/assignment_telco/7_feature_store/feature_store.py'
    )

    model_building = BashOperator(
        task_id='model_building',
        bash_command='python /opt/airflow/dags/assignment_telco/9_model_building/model_building.py'
    )

    db_creation >> ingestion >> validation >> preparation >> storage >> feature_store >> model_building
