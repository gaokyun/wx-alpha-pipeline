from airflow import DAG
# This provider path is standard in Airflow 2.x
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id='bootstrap_sn_meteor',
    start_date=datetime(2026, 1, 1),
    # In Airflow 2.11.2, schedule=None is the standard for manual triggers.
    # While 'schedule' was introduced in 2.4, schedule_interval=None remains common.
    schedule_interval=None, 
    catchup=False,
    tags=['cloud', 'snowflake', 'infrastructure']
) as dag:

    # The SQLExecuteQueryOperator is highly portable and works 
    # perfectly with the snowflake_conn defined in your Compose file.
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