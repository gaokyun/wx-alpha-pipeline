from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Corrected Airflow 3.x import to silence the warning
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import docker

def execute_dbt_seed_in_container():
    """
    Physical Execution: Connects to the host Docker daemon via the mounted socket,
    finds the dbt-snowflake-runner, and executes the seed command.
    """
    # Connect to the Docker daemon via the socket we just mounted in the YAML
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    
    try:
        # Locate your specific container
        container = client.containers.get('dbt-snowflake-runner')
        
        # STRUCTURAL FIX: We must point workdir to the specific subfolder 
        # inside /usr/app where your dbt_project.yml actually lives.
        # Replace 'dbt_meteor' below with the actual name of your folder.
        exit_code, output = container.exec_run(
            cmd='bash -c "dbt seed --profiles-dir . --target prod"',
            workdir='/usr/app/physical_meteor' # <-- UPDATE THIS PATH
        )
        
        # Log the physical output for the Airflow UI
        print(output.decode('utf-8'))
        
        # Ground Truth Validation: Fail the Airflow task if dbt fails
        if exit_code != 0:
            raise Exception(f"dbt seed failed with exit code {exit_code}")
            
    except docker.errors.NotFound:
        raise Exception("Container 'dbt-snowflake-runner' not found. Is it running?")

with DAG(
    dag_id='bootstrap_and_seed_meteorology',
    start_date=datetime(2026, 1, 1),
    schedule=None, 
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