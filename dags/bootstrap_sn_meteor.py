import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id='bootstrap_sn_meteor',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['cloud', 'snowflake', 'infrastructure']
)
def bootstrap_sn_meteor():
    initialize_snowflake_objects = SQLExecuteQueryOperator(
        task_id='init_snowflake_meteor',
        conn_id='snowflake_conn',
        sql="""
            CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE='XSMALL';
            CREATE DATABASE IF NOT EXISTS PHYSICAL_METEOR_DB;
            CREATE SCHEMA IF NOT EXISTS PHYSICAL_METEOR_DB.RAW;

            -- Set permissions for the account admin role
            GRANT ALL PRIVILEGES ON DATABASE PHYSICAL_METEOR_DB TO ROLE ACCOUNTADMIN;
        """
    )

    initialize_snowflake_objects

bootstrap_sn_meteor = bootstrap_sn_meteor()