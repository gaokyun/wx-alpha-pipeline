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

WEATHER_MODELS_ADW = {
    'gfs': {
        'asset_trigger': [ASSETS['gfs-upper'], ASSETS['gfs-surface']],
        'selector': 'stg_adw_gfs_upper+ stg_adw_gfs_surface+',
        'tags': ['gfs'],
        'desc': 'Global Forecast System'
    },
    'aifs': {
        'asset_trigger': [ASSETS['aifs-upper'], ASSETS['aifs-surface'], ASSETS['aifs-spread']],
        'selector': 'stg_adw_aifs_upper+ stg_adw_aifs_surface+ stg_adw_aifs_spread+',
        'tags': ['aifs', 'ai'],
        'desc': 'ECMWF Artificial Intelligence Forecast'
    },
    'ifs': {
        'asset_trigger': [ASSETS['ifs-upper'], ASSETS['ifs-surface'], ASSETS['ifs-spread']],
        'selector': 'stg_adw_ifs_upper+ stg_adw_ifs_surface+ stg_adw_ifs_spread+',
        'tags': ['ifs', 'high_res'],
        'desc': 'ECMWF Integrated Forecasting System'
    }
}

def create_weather_dag_adw(model_id, config):
    @dag(
        dag_id=f'weather_ops.transform.{model_id}_dbt_adw',
        default_args=default_args,
        schedule=config['asset_trigger'],
        start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
        catchup=False,
        doc_md=f"### {config['desc']} ADW Transformation\nSurgical dbt run for {model_id} family targeting Oracle ADW.",
        tags=['dbt', 'adw', 'oracle', 'gold'] + config['tags']
    )
    def transform_dag():
        
        run_dbt = BashOperator(
            task_id=f'dbt_run_{model_id}_adw',
            bash_command=f"""
                python3 /opt/airflow/dags/utils/bootstrap_adw_schemas.py && \
                python3 /opt/airflow/dags/utils/update_adw_external_tables.py && \
                dbt run --project-dir {DBT_PROJECT_PATH} \
                        --profiles-dir {DBT_PROJECT_PATH} \
                        --target adw_prod \
                        --select {config['selector']}
            """
        )
        
    return transform_dag()

for model_id, config in WEATHER_MODELS_ADW.items():
    globals()[f"dag_transform_adw_{model_id}"] = create_weather_dag_adw(model_id, config)


@dag(
    dag_id='weather_ops.transform.unified_forecast_refresh_adw',
    default_args=default_args,
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 3, 20, tz="America/New_York"),
    catchup=False,
    doc_md="Refreshes the final consensus ADW views for both Upper Air and Surface metrics.",
    tags=['dbt', 'adw', 'oracle', 'consensus', 'gold']
)
def refresh_unified_forecasts_adw():

    def dbt_task(task_id, select_statement):
        return BashOperator(
            task_id=task_id,
            bash_command=f"""
                python3 /opt/airflow/dags/utils/bootstrap_adw_schemas.py && \
                python3 /opt/airflow/dags/utils/update_adw_external_tables.py && \
                dbt run --project-dir {DBT_PROJECT_PATH} \
                        --profiles-dir {DBT_PROJECT_PATH} \
                        --target adw_prod \
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

    refresh_marts = dbt_task('refresh_adw_marts', 'dim_adw_locations dim_adw_times fct_adw_gfs_upper fct_adw_gfs_surface fct_adw_aifs_upper fct_adw_aifs_surface fct_adw_aifs_spread fct_adw_ifs_upper fct_adw_ifs_surface fct_adw_ifs_spread')
    unified_gold = dbt_task('gold_unified_adw', 'fct_adw_upper_forecast fct_adw_surface_forecast fct_adw_spread_forecast')

    wait_for_data >> refresh_marts >> unified_gold

globals()["dag_unified_refresh_adw"] = refresh_unified_forecasts_adw()
