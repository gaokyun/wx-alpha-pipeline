from airflow import DAG
# --- AIRFLOW 2.x COMPATIBLE IMPORT ---
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

with DAG(
    dag_id='bootstrap_pg_meteor',
    start_date=datetime(2026, 1, 1),
    # Reverting to schedule_interval for maximum 2.x stability
    schedule_interval=None, 
    catchup=False,
    tags=['local', 'postgres', 'infrastructure']
) as dag:

    def init_db():
        """
        Physically initialize the local Postgres environment.
        """
        # Finds the 'postgres_default' connection from your docker-compose.yaml
        primary_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Check if database exists before trying to create it
        exists_sql = "SELECT 1 FROM pg_database WHERE datname='PHYSICAL_METEOR_DB'"
        exists = primary_hook.get_first(exists_sql)
        
        if not exists:
            print("Postgres: Creating PHYSICAL_METEOR_DB...")
            # autocommit=True is mandatory for CREATE DATABASE in Postgres
            primary_hook.run('CREATE DATABASE "PHYSICAL_METEOR_DB"', autocommit=True)
        else:
            print("Postgres: PHYSICAL_METEOR_DB already exists.")

        try:
            # Connect specifically to the newly created DB to initialize schema
            db_hook = PostgresHook(
                postgres_conn_id='postgres_default',
                schema='PHYSICAL_METEOR_DB'
            )
            db_hook.run("CREATE SCHEMA IF NOT EXISTS RAW;")
            print("Postgres: RAW schema verified.")
        except Exception as e:
            print(f"Postgres Error: {e}")
            raise

    init_task = PythonOperator(
        task_id='init_postgres_meteor',
        python_callable=init_db
    )

    init_task