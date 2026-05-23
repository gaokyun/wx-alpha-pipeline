import pendulum

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import docker

@task(task_id='dbt_seed_meteor_anchors')
def execute_dbt_seed_in_container():
    """
    Physical Execution: Connects to the host Docker daemon via the mounted socket,
    finds the dbt-snowflake-runner, and executes the seed command.
    """
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')

    try:
        container = client.containers.get('dbt-snowflake-runner')
        exit_code, output = container.exec_run(
            cmd='bash -c "dbt seed --profiles-dir . --target prod"',
            workdir='/usr/app/physical_meteor'
        )

        print(output.decode('utf-8'))

        if exit_code != 0:
            raise Exception(f"dbt seed failed with exit code {exit_code}")

    except docker.errors.NotFound:
        raise Exception("Container 'dbt-snowflake-runner' not found. Is it running?")

@dag(
    dag_id='bootstrap_and_seed_snowflake_meteor',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['infrastructure', 'snowflake', 'dbt']
)
def bootstrap_and_seed_snowflake_meteor():
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

    dbt_seed_task = execute_dbt_seed_in_container()
    init_snowflake >> dbt_seed_task

bootstrap_and_seed_snowflake_meteor = bootstrap_and_seed_snowflake_meteor()