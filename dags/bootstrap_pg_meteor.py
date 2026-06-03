import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag
from airflow.sdk import task

@dag(
    dag_id='bootstrap_pg_meteor',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['local', 'postgres', 'infrastructure']
)
def bootstrap_pg_meteor():

    @task(task_id='init_postgres_meteor')
    def init_db():
        """
        Physically initialize the local Postgres environment.
        """
        primary_hook = PostgresHook(postgres_conn_id='postgres_default')

        exists_sql = "SELECT 1 FROM pg_database WHERE datname='PHYSICAL_METEOR_DB'"
        exists = primary_hook.get_first(exists_sql)

        if not exists:
            print("Postgres: Creating PHYSICAL_METEOR_DB...")
            primary_hook.run('CREATE DATABASE "PHYSICAL_METEOR_DB"', autocommit=True)
        else:
            print("Postgres: PHYSICAL_METEOR_DB already exists.")

        db_hook = PostgresHook(
            postgres_conn_id='postgres_default',
            schema='PHYSICAL_METEOR_DB'
        )
        db_hook.run("CREATE SCHEMA IF NOT EXISTS RAW;")
        print("Postgres: RAW schema verified.")
        
        # Import and initialize data governance / metadata logging tables
        from utils.governance import init_metadata_table
        init_metadata_table()

    init_db()

bootstrap_pg_meteor = bootstrap_pg_meteor()