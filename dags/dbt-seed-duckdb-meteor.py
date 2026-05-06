from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import logging

# --- IMPORT DBT NATIVE RUNNER ---
from dbt.cli.main import dbtRunner, dbtRunnerResult

def execute_dbt_seed_natively():
    """
    Native Execution: Uses dbtRunner to execute the seed command directly 
    inside the Airflow worker's Python environment. No Docker required.
    """
    logger = logging.getLogger(__name__)
    
    # 1. Environment Variable Setup
    # If your Airflow worker already has these loaded via its Dockerfile/docker-compose, 
    # you don't need to re-declare them. Included here for safety.
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    
    # Ensure OCI credentials are in the environment (adjust if using Airflow Variables/Connections)
    # os.environ["OCI_ACCESS_KEY"] = "..." 
    # os.environ["OCI_SECRET_KEY"] = "..."

    # 2. Build the dbt arguments
    # CRITICAL: These paths must point to where the dbt project lives INSIDE the Airflow worker
    dbt_project_path = '/opt/airflow/physical_meteor' 
    
    dbt_cli_args = [
        "seed",
        "--project-dir", dbt_project_path,
        "--profiles-dir", dbt_project_path,
        "--target", "dev_duckdb"
    ]
    
    logger.info(f"Executing dbt natively via Python API: dbt {' '.join(dbt_cli_args)}")
    
    # 3. Initialize and invoke the dbt runner
    dbt = dbtRunner()
    result: dbtRunnerResult = dbt.invoke(dbt_cli_args)
    
    # 4. Handle the results natively
    if result.success:
        logger.info("✅ dbt seed completed successfully.")
    else:
        logger.error("❌ DBT Error. The seed failed to execute.")
        if result.exception:
            logger.error(f"Exception details: {result.exception}")
        raise Exception("dbt DuckDB seed command failed.")

with DAG(
    dag_id='bootstrap_and_seed_duckdb_dw',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['infrastructure', 'duckdb', 'dbt']
) as dag:

    # Task: Trigger dbt seed natively
    dbt_seed_task = PythonOperator(
        task_id='dbt_seed_meteor_anchors',
        python_callable=execute_dbt_seed_natively
    )

    # Execution Order
    dbt_seed_task