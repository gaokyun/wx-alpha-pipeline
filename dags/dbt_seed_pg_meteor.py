# dags/bootstrap_pg_meteor.py
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import docker

def init_db():
    """Physically initialize the local Postgres environment."""
    primary_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check if database exists
    exists_sql = "SELECT 1 FROM pg_database WHERE datname='PHYSICAL_METEOR_DB'"
    exists = primary_hook.get_first(exists_sql)
    
    if not exists:
        primary_hook.run('CREATE DATABASE "PHYSICAL_METEOR_DB"', autocommit=True)
        
    # Initialize Schema
    db_hook = PostgresHook(
        postgres_conn_id='postgres_default',
        schema='PHYSICAL_METEOR_DB'
    )
    db_hook.run("CREATE SCHEMA IF NOT EXISTS RAW;")

def execute_dbt_seed_in_pg_container():
    """Triggers dbt seed inside the dbt-postgres runner container."""
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    try:
        # Note: The service name in your yaml is 'dbt-postgres'
        # Docker Compose usually names the container 'project-dbt-postgres-1' 
        # unless 'container_name' is explicitly set.
        # docker ps --filter "ancestor=dbt-postgres" --format "{{.Names}}"
        container = client.containers.get('wx-alpha-pipeline-dbt-postgres-1') 
        
        exit_code, output = container.exec_run(
            cmd='bash -c "dbt seed --profiles-dir . --target dev"',
            workdir='/usr/app/physical_meteor'
        )
        
        print(output.decode('utf-8'))
        
        if exit_code != 0:
            raise Exception(f"dbt pg seed failed with exit code {exit_code}")
            
    except docker.errors.NotFound:
        raise Exception("Container 'dbt-postgres' not found.")

with DAG(
    dag_id='bootstrap_dbt_seed_pg_meteor',
    start_date=datetime(2026, 1, 1),
    schedule=None, 
    catchup=False,
    tags=['local', 'postgres', 'infrastructure']
) as dag:

    init_task = PythonOperator(
        task_id='init_postgres_meteor',
        python_callable=init_db
    )

    dbt_seed_pg = PythonOperator(
        task_id='dbt_seed_postgres_anchors',
        python_callable=execute_dbt_seed_in_pg_container
    )

    init_task >> dbt_seed_pg