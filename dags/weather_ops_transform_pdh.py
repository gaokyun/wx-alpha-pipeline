import os
import pendulum
from airflow.sdk import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount

HOST_PROJECT_PATH = os.getenv('HOST_PROJECT_PATH', '/home/airflow/dev/wx-alpha-pipeline')

default_args = {
    'owner': 'meteorologist',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5),
}

# PDH DISABLED 2026-06-17: duckdb_fdw causes Postgres container OOM crash on at_aifs_upper
# (166M row double full-table scan from OCI S3 Delta Lake, no partition pushdown possible).
# To re-enable: fix forecast_reference_time pushdown in staging models, then remove
# is_paused_upon_creation=True and re-enable trigger_postgres_marts in master control.
@dag(
    dag_id='weather_ops.transform.pdh_dbt_postgres',
    default_args=default_args,
    schedule=None,  # Manual trigger or master control trigger
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,  # DISABLED: see comment above
    tags=['pdh', 'postgres', 'dbt', 'gold']
)
def pdh_transform_dag():

    @task(task_id='bootstrap_fdw', pool='dph_single_writer')
    def run_bootstrap():
        # Execute the bootstrap script inside the airflow worker
        import subprocess
        result = subprocess.run(
            ["python3", "/opt/airflow/dags/utils/bootstrap_pdh_fdw.py"],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise Exception("FDW Bootstrapping failed")

    # Run the dbt models using DockerOperator with pool='dph_single_writer'
    dbt_run = DockerOperator(
        task_id='dbt_run_pdh',
        image="dbt-postgres:latest",
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="wx-alpha-pipeline_default",
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_PATH}/physical_meteor",
                target="/usr/app/physical_meteor",
                type="bind",
            ),
            Mount(
                source=f"{HOST_PROJECT_PATH}/data",
                target="/opt/airflow/data",
                type="bind",
            ),
        ],
        environment={
            "OCI_ACCESS_KEY": os.getenv("OCI_ACCESS_KEY"),
            "OCI_SECRET_KEY": os.getenv("OCI_SECRET_KEY"),
            "POSTGRES_USERNAME": os.getenv("POSTGRES_USERNAME", "airflow"),
            "POSTGRES_PASS": os.getenv("POSTGRES_PASS", "airflow"),
        },
        command="dbt run --project-dir /usr/app/physical_meteor --profiles-dir /usr/app/physical_meteor --target dev_postgres --select tag:pdh",
        pool='dph_single_writer'  # Restrict to pool with slots=1 to prevent VM overload
    )

    run_bootstrap() >> dbt_run

pdh_transform_dag()
