# This maintenance DAG cleans up old metadata from the Airflow database.
# It runs directly inside an isolated container to bypass Airflow 3 database constraints.

import os
import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'admin',
    'retries': 1,
}

@dag(
    dag_id='ops.metadata_db_cleanup',
    schedule='0 11 * * *',
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
        command="""
            /bin/bash -c "
            THRESHOLD_DATE=\$(date -u -d '2 days ago' +'%Y-%m-%d %H:%M:%S')
            airflow db clean --clean-before-timestamp \"\${THRESHOLD_DATE}\" --yes
            "
        """,
        environment={
            # This isolated container bypasses the task runner block
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        },
        network_mode="wx-alpha-pipeline_default", # Match your docker-compose network name
    )

    purge_metadata

cleanup_dag = cleanup_dag()