import os
import pendulum
from airflow.sdk import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST_PROJECT_PATH = os.getenv('HOST_PROJECT_PATH', '/home/airflow/dev/wx-alpha-pipeline')

default_args = {
    'owner': 'meteorologist',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

@dag(
    dag_id='weather_ops.transform.dph_dbt_postgres',
    default_args=default_args,
    schedule=None,  # Manual trigger or master control trigger
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    tags=['dph', 'postgres', 'dbt', 'gold']
)
def dph_transform_dag():

    # Run the dbt models using DockerOperator
    dbt_run = DockerOperator(
        task_id='dbt_run_dph',
        image="dbt-postgres:latest",
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        user="50000:0",
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
            "POSTGRES_USERNAME": os.getenv("POSTGRES_USERNAME", "airflow"),
            "POSTGRES_PASS": os.getenv("POSTGRES_PASS", "airflow"),
        },
        # Selects all models under models/dph (staging + atomic marts)
        command="dbt run --project-dir /usr/app/physical_meteor --profiles-dir /usr/app/physical_meteor --target dev_postgres --select path:models/dph",
        pool='dph_single_writer',
    )

    dbt_run

dph_transform_dag()
