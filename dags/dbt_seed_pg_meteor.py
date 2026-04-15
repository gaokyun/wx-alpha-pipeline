from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from dbt.cli.main import dbtRunner
import logging

# Standardize logging
logger = logging.getLogger("airflow.task")

def init_db():
    """Physically initialize the local Postgres environment."""
    # Ensure 'postgres_default' exists in Airflow UI -> Admin -> Connections
    primary_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check if database exists
    exists_sql = "SELECT 1 FROM pg_database WHERE datname='PHYSICAL_METEOR_DB'"
    exists = primary_hook.get_first(exists_sql)
    
    if not exists:
        logger.info("Creating database PHYSICAL_METEOR_DB...")
        # Databases cannot be created inside a transaction block
        primary_hook.run('CREATE DATABASE "PHYSICAL_METEOR_DB"', autocommit=True)
    
    # Initialize Schema
    db_hook = PostgresHook(
        postgres_conn_id='postgres_default',
        schema='PHYSICAL_METEOR_DB'
    )
    db_hook.run("CREATE SCHEMA IF NOT EXISTS RAW;")
    logger.info("Schema RAW initialized successfully.")

def execute_dbt_seed_natively_pg():
    """Executes dbt seed natively using the dbtRunner API."""
    
    # Use the absolute path where your dbt project is mounted in the airflow-worker container
    dbt_project_path = '/opt/airflow/physical_meteor'
    
    # Define the dbt command arguments
    # Note: 'dev_postgres' must match the target name in your profiles.yml
    dbt_args = [
        "seed",
        "--project-dir", dbt_project_path,
        "--profiles-dir", dbt_project_path,
        "--target", "dev_postgres"
    ]

    logger.info(f"Invoking dbt natively: dbt {' '.join(dbt_args)}")
    
    dbt = dbtRunner()
    result = dbt.invoke(dbt_args)

    # Handle results (dbtRunner returns a result object, not an exit code)
    if not result.success:
        if result.exception:
            logger.error(f"Internal dbt Error: {result.exception}")
        raise Exception("dbt Postgres seed failed. Check logs above for details.")
    
    logger.info("✅ dbt Postgres seed completed successfully!")

with DAG(
    dag_id='bootstrap_dbt_seed_pg_meteor',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['local', 'postgres', 'dbt-native']
) as dag:

    init_task = PythonOperator(
        task_id='init_postgres_meteor',
        python_callable=init_db
    )

    dbt_seed_pg = PythonOperator(
        task_id='dbt_seed_postgres_anchors',
        python_callable=execute_dbt_seed_natively_pg
    )

    init_task >> dbt_seed_pg