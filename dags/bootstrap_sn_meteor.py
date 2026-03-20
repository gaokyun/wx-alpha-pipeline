# dags/bootstrap_sn_meteor.py
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id='bootstrap_sn_meteor',
    start_date=datetime(2026, 1, 1),
    # In Airflow 3.x, 'schedule_interval' is replaced by 'schedule'
    schedule=None, 
    catchup=False,
    tags=['cloud', 'snowflake', 'infrastructure']
) as dag:

    # Using the universal SQL operator compatible with Airflow 3.x
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