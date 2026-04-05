from airflow import DAG
# In Airflow 2.x, SQLExecuteQueryOperator is found here:
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# --- AIRFLOW 2.x COMPATIBLE IMPORT ---
from airflow.operators.python import PythonOperator
from datetime import datetime
import docker

def execute_dbt_seed_in_container():
    """
    Physical Execution: Connects to the host Docker daemon via the mounted socket,
    finds the dbt-snowflake-runner, and executes the seed command.
    """
    # Connect to the Docker daemon via the socket mounted in the YAML
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    
    try:
        # Locate your specific container
        container = client.containers.get('dbt-snowflake-runner')
        
        # Execute the seed command inside the working directory of the container
        exit_code, output = container.exec_run(
            cmd='bash -c "dbt seed --profiles-dir . --target prod"',
            workdir='/usr/app/physical_meteor' 
        )
        
        # Log the output for the Airflow UI logs
        print(output.decode('utf-8'))
        
        # Fail the Airflow task if the dbt exit code is non-zero
        if exit_code != 0:
            raise Exception(f"dbt seed failed with exit code {exit_code}")
            
    except docker.errors.NotFound:
        raise Exception("Container 'dbt-snowflake-runner' not found. Is it running?")

with DAG(
    dag_id='bootstrap_and_seed_snowflake_meteor',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None, # In Airflow 2, use schedule_interval instead of schedule
    catchup=False,
    tags=['infrastructure', 'snowflake', 'dbt']
) as dag:

    # Task 1: Initialize Snowflake Environment
    init_snowflake = SQLExecuteQueryOperator(
        task_id='init_snowflake_meteor',
        conn_id='snowflake_conn',
        sql="""
            CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE='XSMALL';
            CREATE DATABASE IF NOT EXISTS PHYSICAL_METEOR_DB;
            CREATE SCHEMA IF NOT EXISTS PHYSICAL_METEOR_DB.RAW;
            GRANT ALL PRIVILEGES ON DATABASE PHYSICAL_METEOR_DB TO ROLE ACCOUNTADMIN;
        """
    )

    # Task 2: Trigger dbt seed via Python Docker API
    dbt_seed_task = PythonOperator(
        task_id='dbt_seed_meteor_anchors',
        python_callable=execute_dbt_seed_in_container
    )

    # Execution Order
    init_snowflake >> dbt_seed_task