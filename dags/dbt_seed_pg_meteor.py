import logging

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dbt.cli.main import dbtRunner
from airflow.sdk import task
from airflow.sdk import dag

logger = logging.getLogger("airflow.task")

@task(task_id='init_postgres_meteor')
def init_db():
    """Physically initialize the local Postgres environment."""
    primary_hook = PostgresHook(postgres_conn_id='postgres_default')

    exists_sql = "SELECT 1 FROM pg_database WHERE datname='PHYSICAL_METEOR_DB'"
    exists = primary_hook.get_first(exists_sql)

    if not exists:
        logger.info("Creating database PHYSICAL_METEOR_DB...")
        primary_hook.run('CREATE DATABASE "PHYSICAL_METEOR_DB"', autocommit=True)

    db_hook = PostgresHook(
        postgres_conn_id='postgres_default',
        schema='PHYSICAL_METEOR_DB'
    )
    db_hook.run("CREATE SCHEMA IF NOT EXISTS RAW;")
    logger.info("Schema RAW initialized successfully.")
    
    # Import and initialize data governance / metadata logging tables
    from utils.governance import init_metadata_table
    init_metadata_table()

@task(task_id='dbt_seed_postgres_anchors')
def execute_dbt_seed_natively_pg():
    """Executes dbt seed natively using the dbtRunner API."""
    dbt_project_path = '/opt/airflow/physical_meteor'
    dbt_args = [
        "seed",
        "--project-dir", dbt_project_path,
        "--profiles-dir", dbt_project_path,
        "--target", "dev_postgres"
    ]

    logger.info(f"Invoking dbt natively: dbt {' '.join(dbt_args)}")
    dbt = dbtRunner()
    result = dbt.invoke(dbt_args)

    if not result.success:
        if result.exception:
            logger.error(f"Internal dbt Error: {result.exception}")
        raise Exception("dbt Postgres seed failed. Check logs above for details.")

    logger.info("✅ dbt Postgres seed completed successfully!")

@dag(
    dag_id='bootstrap_dbt_seed_pg_meteor',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['local', 'postgres', 'dbt-native']
)
def bootstrap_dbt_seed_pg_meteor():
    init_task = init_db()
    dbt_seed_pg = execute_dbt_seed_natively_pg()

    init_task >> dbt_seed_pg

bootstrap_dbt_seed_pg_meteor = bootstrap_dbt_seed_pg_meteor()