# This maintenance DAG cleans up old metadata from the Airflow database.
# It runs directly inside an isolated container to bypass Airflow 3 database constraints.

import os
import pendulum
from airflow.sdk import dag
# from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk.bases.hook import BaseHook

default_args = {
    'owner': 'admin',
    'retries': 1,
}

# Dynamically fetch the connection details at parse time
def get_dynamic_conn_uri():
    try:
        # Resolves the connection named 'postgres_default' or your custom conn_id
        conn = BaseHook.get_connection("postgres_default")
        return conn.get_uri()
    except Exception:
        # Fallback to a safe empty string or default if not found
        return ""

@dag(
    dag_id='ops.metadata_db_cleanup',
    schedule='0 22 * * *',
    # Setting the timezone to America/New_York ensures automatic EDT/EST compliance
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    # start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['ops', 'maintenance', 'cleanup']
)
def cleanup_dag():

    # This bypasses the worker isolation rules by spinning up a clean CLI execution thread
    purge_metadata = DockerOperator(
        task_id="purge_airflow_db_history",
        image="apache/airflow:3.2.1",  # Match your active Airflow image version
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,           # Fixes the "bind source path does not exist" Docker-in-Docker error
        # Override the entrypoint to a raw bash shell execution loop
        entrypoint=[r"/bin/bash", "-s"],
        
        # Pass the clean routine directly as the command argument
        command=r"""<<'EOF'
            THRESHOLD_DATE=$(date -u -d '2 days ago' +'%Y-%m-%d %H:%M:%S')
            
            echo "🧹 Executing clean routine for timestamps older than: ${THRESHOLD_DATE} UTC"
            
            airflow db clean --clean-before-timestamp "${THRESHOLD_DATE}" --yes
            EOF
            """,
        environment={
            # Seamlessly injects the URI fetched from the Airflow connection manager
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": get_dynamic_conn_uri()
        },
        network_mode="wx-alpha-pipeline_default", # Match your docker-compose network name
    )

    purge_metadata

cleanup_dag = cleanup_dag()