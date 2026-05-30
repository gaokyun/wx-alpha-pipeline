import os
import pendulum
from airflow.sdk import dag, task, Asset
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from utils.check_data_readiness import check_prior_day_data_readiness

OCI_BUCKET = os.getenv('OCI_OBJECT_STORAGE_BUCKET', 'oci-s3-ykg-storage')
DBT_PROJECT_PATH = os.getenv('DBT_PROJECT_PATH', '/opt/airflow/physical_meteor')

ASSETS = {
    'gfs-upper': Asset(uri=f's3://{OCI_BUCKET}/weather_data/delta_lake/gfs_raw/gfs_upper/', name='gfs_upper'),
    'gfs-surface': Asset(uri=f's3://{OCI_BUCKET}/weather_data/delta_lake/gfs_raw/gfs_surface/', name='gfs_surface'),
    'aifs-upper': Asset(uri=f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/', name='at_aifs_upper'),
    'aifs-surface': Asset(uri=f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_aifs_surface/', name='at_aifs_surface'),
    'aifs-spread': Asset(uri=f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/aifs_spread/', name='aifs_spread'),
    'ifs-upper': Asset(uri=f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_upper/', name='at_ifs_upper'),
    'ifs-surface': Asset(uri=f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_surface/', name='at_ifs_surface'),
    'ifs-spread': Asset(uri=f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/ifs_spread/', name='ifs_spread'),
}

default_args = {
    'owner': 'meteorologist',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5),
}

@dag(
    dag_id='weather_ops.transform.unified_forecast_refresh_mysql',
    default_args=default_args,
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 3, 20, tz="America/New_York"),
    catchup=False,
    doc_md="Refreshes the final consensus MySQL views for both Upper Air and Surface metrics.",
    tags=['dbt', 'mysql', 'heatwave', 'gold']
)
def refresh_unified_forecasts_mysql():

    def dbt_task(task_id, select_statement):
        return BashOperator(
            task_id=task_id,
            bash_command=f"""
                python3 /opt/airflow/dags/utils/bootstrap_mysql_schemas.py && \
                dbt run --project-dir {DBT_PROJECT_PATH} \
                        --profiles-dir {DBT_PROJECT_PATH} \
                        --target dev_duckdb_mysql \
                        --select {select_statement}
            """
        )

    wait_for_data = PythonSensor(
        task_id='wait_for_prior_day_data',
        python_callable=check_prior_day_data_readiness,
        poke_interval=300,        # Poll every 5 minutes
        timeout=14400,            # 4 hour timeout
        mode='reschedule',        # Frees up worker slots while waiting
    )

    refresh_marts = dbt_task('refresh_mysql_marts', 'dim_dmh_locations dim_dmh_times fct_dmh_gfs_upper fct_dmh_gfs_surface fct_dmh_aifs_upper fct_dmh_aifs_surface fct_dmh_aifs_spread fct_dmh_ifs_upper fct_dmh_ifs_surface fct_dmh_ifs_spread')
    unified_gold = dbt_task('gold_unified_mysql', 'fct_dmh_upper_forecast fct_dmh_surface_forecast fct_dmh_spread_forecast')

    wait_for_data >> refresh_marts >> unified_gold

dag_obj = refresh_unified_forecasts_mysql()
