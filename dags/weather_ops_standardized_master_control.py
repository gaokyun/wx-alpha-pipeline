import os
import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from utils.check_data_readiness import check_prior_day_data_readiness
from airflow.sdk import task
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import TaskGroup

DBT_PROJECT_PATH = os.getenv('DBT_PROJECT_PATH', '/opt/airflow/physical_meteor')
HOST_PROJECT_PATH = os.getenv('HOST_PROJECT_PATH', '/home/airflow/dev/wx-alpha-pipeline')

default_args = {
    'owner': 'meteorologist',
    'start_date': pendulum.datetime(2026, 3, 20, tz="America/New_York"),
    'retries': 0,
    'depends_on_past': False,
}

@dag(
    dag_id='weather_ops.standardized_master_control',
    default_args=default_args,
    schedule="0 4 * * *",
    catchup=False,
    doc_md="Centralized Master Dashboard to orchestrate extractions, transformations, and gold unified updates.",
    tags=['control', 'weather_ops', 'dashboard', 'standardized']
)
def standardized_master_control():

    # A. Data Completeness check
    wait_for_data = PythonSensor(
        task_id='wait_for_prior_day_data',
        python_callable=check_prior_day_data_readiness,
        poke_interval=300,
        timeout=14400,
        mode='reschedule',
    )

    # A.1 Maintenance: Pre-allocate Future Postgres Partitions
    generate_future_partitions = BashOperator(
        task_id='generate_future_partitions',
        bash_command='python3 /opt/airflow/dags/add_postgres_partitions.py',
    )

    # B. Extractions TaskGroup (Triggers 8 extraction DAGs and waits for completion)
    with TaskGroup(group_id='extractions') as extractions_group:
        models = {
            'gfs': ['upper', 'surface'],
            'aifs': ['upper', 'surface', 'spread'],
            'ifs': ['upper', 'surface', 'spread']
        }
        for model, ttypes in models.items():
            for ttype in ttypes:
                TriggerDagRunOperator(
                    task_id=f'trigger_{model}_{ttype}',
                    trigger_dag_id=f'weather_ops.extract.{model}.{ttype}',
                    wait_for_completion=True,
                    poke_interval=30,
                    reset_dag_run=True,
                )

    # C. Transformations TaskGroup (Triggers the child model mart DAGs and waits for completion)
    with TaskGroup(group_id='transformations') as transformations_group:
        TriggerDagRunOperator(
            task_id='trigger_mysql_marts',
            trigger_dag_id='weather_ops.transform.unified_forecast_refresh_mysql',
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=True,
        )
        TriggerDagRunOperator(
            task_id='trigger_adw_marts',
            trigger_dag_id='weather_ops.transform.unified_forecast_refresh_adw',
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=True,
        )
        TriggerDagRunOperator(
            task_id='trigger_postgres_marts',
            trigger_dag_id='weather_ops.transform.all_models_dbt_duckdb', #'weather_ops.transform.dph_dbt_postgres',
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=True,
        )
    
    # D. Unified Refreshes TaskGroup (Runs final gold unified forecast updates)
    with TaskGroup(group_id='unified_refreshes') as unified_refreshes_group:
        # 1. MySQL Unified Gold
        BashOperator(
            task_id='refresh_gold_unified_mysql',
            bash_command=f"""
                python3 /opt/airflow/dags/utils/bootstrap_mysql_schemas.py && \
                dbt run --project-dir {DBT_PROJECT_PATH} \
                        --profiles-dir {DBT_PROJECT_PATH} \
                        --target dev_duckdb_mysql \
                        --select fct_dmh_upper_forecast fct_dmh_surface_forecast fct_dmh_spread_forecast
            """,
            pool='dmh_single_writer'
        )

        # 2. Oracle ADW Unified Gold
        BashOperator(
            task_id='refresh_gold_unified_adw',
            bash_command=f"""
                python3 /opt/airflow/dags/utils/bootstrap_adw_schemas.py && \
                python3 /opt/airflow/dags/utils/update_adw_external_tables.py && \
                dbt run --project-dir {DBT_PROJECT_PATH} \
                        --profiles-dir {DBT_PROJECT_PATH} \
                        --target adw_prod \
                        --select fct_adw_upper_forecast fct_adw_surface_forecast fct_adw_spread_forecast
            """,
            pool='adw_dbt_pool'
        )

        # 3. Postgres DPH Unified Gold (reads from Postgres gold atomic tables, no S3/FDW needed)
        DockerOperator(
            task_id='refresh_gold_unified_postgres',
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
                "POSTGRES_USERNAME": os.getenv("POSTGRES_USERNAME", "airflow"),
                "POSTGRES_PASS": os.getenv("POSTGRES_PASS", "airflow"),
            },
            command="dbt run --project-dir /usr/app/physical_meteor --profiles-dir /usr/app/physical_meteor --target dev_postgres --select fct_upper_forecast fct_surface_forecast fct_spread_forecast",
            pool='dph_single_writer'
        )

    @task(task_id='verify_postgres_gold_marts')
    def run_verify_gold_datasets():
        from utils.governance import verify_gold_datasets
        verify_gold_datasets()

    # Establish full workflow dependency
    wait_for_data >> generate_future_partitions >> extractions_group >> transformations_group >> unified_refreshes_group >> run_verify_gold_datasets()

master_dag = standardized_master_control()
