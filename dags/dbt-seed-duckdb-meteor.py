from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import logging

# --- IMPORT DBT NATIVE RUNNER ---
from dbt.cli.main import dbtRunner, dbtRunnerResult

def execute_dbt_seed_natively(**context):
    """
    Executes dbt seed via the Python API and parses the results 
    to log the exact database, schema, table, and row counts updated.
    """
    logger = logging.getLogger(__name__)
    
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

    dbt_project_path = '/opt/airflow/physical_meteor' 
    dbt_cli_args = [
        "seed",
        "--project-dir", dbt_project_path,
        "--profiles-dir", dbt_project_path,
        "--target", "dev_duckdb"
    ]
    
    logger.info(f"Executing dbt natively: dbt {' '.join(dbt_cli_args)}")
    
    dbt = dbtRunner()
    result: dbtRunnerResult = dbt.invoke(dbt_cli_args)
    
    if result.success:
        logger.info("✅ dbt seed completed successfully.")
        
        # FIX: The dbtRunnerResult wrapper contains the execution payload in `.result`.
        # We must access result.result.results.
        execution_payload = result.result
        
        # Safeguard to ensure the payload actually contains 'results'
        if hasattr(execution_payload, 'results'):
            for res in execution_payload.results:
                
                db_name = getattr(res.node, 'database', 'default_db')
                schema_name = getattr(res.node, 'schema', 'default_schema')
                table_name = getattr(res.node, 'name', 'unknown_table')
                
                full_table_path = f"{db_name}.{schema_name}.{table_name}"
                status = res.status
                
                adapter_resp = res.adapter_response or {}
                rows_affected = adapter_resp.get("rows_affected", "N/A")
                
                logger.info("--------------------------------------------------")
                logger.info(f"🎯 Target: {full_table_path}")
                logger.info(f"📊 Status: {status}")
                logger.info(f"📈 Rows Inserted/Updated: {rows_affected}")
                logger.info("--------------------------------------------------")
        else:
            logger.info("ℹ️ dbt executed successfully, but no node-level results were returned.")
            
    else:
        logger.error("❌ DBT Error. The seed failed to execute.")
        if result.exception:
            logger.error(f"Exception details: {result.exception}")
        raise Exception("dbt DuckDB seed command failed.")

# --- DAG DEFINITION ---
with DAG(
    dag_id='bootstrap_and_seed_duckdb_dw',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['infrastructure', 'duckdb', 'dbt']
) as dag:

    dbt_seed_task = PythonOperator(
        task_id='dbt_seed_meteor_anchors',
        python_callable=execute_dbt_seed_natively
    )

    dbt_seed_task