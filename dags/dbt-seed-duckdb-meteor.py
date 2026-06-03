import logging
import os

import pendulum
from dbt.cli.main import dbtRunner, dbtRunnerResult
from airflow.sdk import task
from airflow.sdk import dag

logger = logging.getLogger("airflow.task")

@task(task_id='dbt_seed_meteor_anchors')
def execute_dbt_seed_natively():
    """Executes dbt seed via the Python API and logs dbt results."""
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

    dbt_project_path = '/opt/airflow/physical_meteor'
    dbt_cli_args = [
        "seed",
        "--project-dir", dbt_project_path,
        "--profiles-dir", dbt_project_path,
        "--target", "dev_duckdb_postgres"
    ]

    logger.info(f"Executing dbt natively: dbt {' '.join(dbt_cli_args)}")
    dbt = dbtRunner()
    result: dbtRunnerResult = dbt.invoke(dbt_cli_args)

    if result.success:
        logger.info("✅ dbt seed completed successfully.")
        execution_payload = result.result

        if hasattr(execution_payload, 'results'):
            for res in execution_payload.results:
                db_name = getattr(res.node, 'database', 'default_db')
                schema_name = getattr(res.node, 'schema', 'default_schema')
                table_name = getattr(res.node, 'name', 'unknown_table')
                full_table_path = f"{db_name}.{schema_name}.{table_name}"
                status = res.status
                adapter_resp = res.adapter_response or {}
                rows_affected = adapter_resp.get("rows_affected", "N/A")

                logger.info("--------------------------------------------------")
                logger.info(f"🎯 Target: {full_table_path}")
                logger.info(f"📊 Status: {status}")
                logger.info(f"📈 Rows Inserted/Updated: {rows_affected}")
                logger.info("--------------------------------------------------")
        else:
            logger.info("ℹ️ dbt executed successfully, but no node-level results were returned.")
    else:
        logger.error("❌ DBT Error. The seed failed to execute.")
        if result.exception:
            logger.error(f"Exception details: {result.exception}")
        raise Exception("dbt DuckDB seed command failed.")

@dag(
    dag_id='bootstrap_and_seed_duckdb_dw',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['infrastructure', 'duckdb', 'dbt']
)
def bootstrap_and_seed_duckdb_dw():
    dbt_seed_task = execute_dbt_seed_natively()
    return dbt_seed_task

bootstrap_and_seed_duckdb_dw = bootstrap_and_seed_duckdb_dw()