import docker
import pendulum
from airflow.sdk import dag, task
from airflow.sdk import Asset
import os
# The modern Airflow 3 import

# Fetch the bucket name from the environment variables you set up earlier
# The second argument is a fallback just in case the env var fails to load
S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'amzn-s3-ykg-storage')

# ---------------- 1. Centralized Schedule Config ----------------
PIPELINE_CONFIG = {
    'gfs': {
        'cron': '30 3,9,15,20 * * *',
        'cycle_map': {3: 0, 9: 6, 15: 12, 20: 18},
        'steps': [192, 240, 288], 
        'asset': Asset(f's3://{S3_BUCKET}/weather_data/delta_lake/gfs_raw'),
        'tags': ['meteorology', 'gfs'] 
    },
    'ecmwf': {
        'cron': '30 0,6,12,18 * * *',
        'cycle_map': {6: 0, 12: 6, 18: 12, 0: 18},
        'steps': [192, 240, 288], 
        'asset': Asset(f's3://{S3_BUCKET}/weather_data/delta_lake/ecmwf_raw'),
        'tags': ['meteorology', 'ecmwf'] 
    }
}

default_args = {
    'owner': 'meteorologist',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5), 
}

# --- Helper Function ---
def get_cycle_and_date(trigger_time: pendulum.DateTime, source_name: str):
    """Dynamically looks up the cycle based on our central config using Pendulum."""
    trigger_time_utc = trigger_time.in_tz('UTC')
    trigger_hour = trigger_time_utc.hour
    config = PIPELINE_CONFIG[source_name]
    
    cycle = config['cycle_map'].get(trigger_hour)
    
    if source_name == 'ecmwf' and trigger_hour == 0:
        target_date = trigger_time_utc.subtract(days=1).start_of('day')
    else:
        target_date = trigger_time_utc.start_of('day')
        
    return target_date, cycle


# ---------------- 2. Extraction DAG: GFS ----------------
@dag(
    dag_id='extract_gfs_data',
    default_args=default_args,
    schedule=PIPELINE_CONFIG['gfs']['cron'],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['extract', *PIPELINE_CONFIG['gfs']['tags']] # Pulled from config
)
def extract_gfs():
    @task(task_id='download_gfs', outlets=[PIPELINE_CONFIG['gfs']['asset']], pool='ecmwf_api_pool')
    def run_gfs_download(data_interval_end: pendulum.DateTime = None):
        from etl.meteorology import download_gfs_robust
        
        target_date, current_cycle = get_cycle_and_date(data_interval_end, 'gfs')
        steps = PIPELINE_CONFIG['gfs']['steps'] # Pulled from config

        for step in steps:
            print(f"Triggering GFS: Date {target_date.format('YYYY-MM-DD')}, Cycle {current_cycle}z, Step {step}h")
            success = download_gfs_robust(target_date, current_cycle, step)
            if not success: 
                raise Exception(f"GFS download failed or returned False for step {step}")            
            
        return f"GFS_CYCLE_{current_cycle}_READY"
    

    run_gfs_download()

# ---------------- 3. Extraction DAG: ECMWF ----------------
@dag(
    dag_id='extract_ecmwf_data',
    default_args=default_args,
    schedule=PIPELINE_CONFIG['ecmwf']['cron'],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['extract', *PIPELINE_CONFIG['ecmwf']['tags']] # Pulled from config
)
def extract_ecmwf():
    @task(task_id='download_ecmwf', outlets=[PIPELINE_CONFIG['ecmwf']['asset']], pool='ecmwf_api_pool')
    def run_ecmwf_download(data_interval_end: pendulum.DateTime = None):
        from etl.meteorology import download_ecmwf_unified
        
        target_date, current_cycle = get_cycle_and_date(data_interval_end, 'ecmwf')
        steps = PIPELINE_CONFIG['ecmwf']['steps'] # Pulled from config

        task_types = ['upper', 'surface'] if current_cycle in [6, 18] else ['upper', 'surface', 'spread']

        for step in steps:
            print(f"Triggering ECMWF: Date {target_date.format('YYYY-MM-DD')}, Cycle {current_cycle}z, Step {step}h")
            success = download_ecmwf_unified(
                target_date, current_cycle, step,
                target_models=['AIFS', 'IFS', 'EPS'], 
                task_type=['upper', 'surface', 'spread']
            )
            if not success:
                raise Exception("ECMWF download failed: Lacking upper-level vertical support")
            
        return f"ECMWF_CYCLE_{current_cycle}_READY"

    run_ecmwf_download()

# ---------------- 4a. Transformation DAG: GFS ----------------
@dag(
    dag_id='transform_gfs_dbt',
    default_args=default_args,
    # Triggers immediately and ONLY when GFS dataset is flagged
    schedule=[PIPELINE_CONFIG['gfs']['asset']], 
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dbt', 'transform', *PIPELINE_CONFIG['gfs']['tags']]
)
def transform_gfs():
    
    @task(task_id='dbt_run_gfs_postgres')
    def execute_dbt_run_pg():
        print("GFS dataset updated. Running local Postgres GFS dbt models...")
        # Note the --select flag to restrict dbt to only GFS models
        # cmd='bash -c "dbt run --select path:models/gfs --profiles-dir . --target dev"'
        pass

    @task(task_id='dbt_run_gfs_snowflake')
    def execute_dbt_run_sn():
        print("GFS dataset updated. Running Cloud Snowflake GFS dbt models...")
        # cmd='bash -c "dbt run --select path:models/gfs --profiles-dir . --target prod"'
        pass

    execute_dbt_run_pg()
    execute_dbt_run_sn()

# ---------------- 4b. Transformation DAG: ECMWF ----------------
@dag(
    dag_id='transform_ecmwf_dbt',
    default_args=default_args,
    # Triggers immediately and ONLY when ECMWF dataset is flagged
    schedule=[PIPELINE_CONFIG['ecmwf']['asset']], 
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dbt', 'transform', *PIPELINE_CONFIG['ecmwf']['tags']]
)
def transform_ecmwf():
    
    @task(task_id='dbt_run_ecmwf_postgres')
    def execute_dbt_run_pg():
        print("ECMWF dataset updated. Running local Postgres ECMWF dbt models...")
        # Note the --select flag to restrict dbt to only ECMWF models
        # cmd='bash -c "dbt run --select path:models/ecmwf --profiles-dir . --target dev"'
        pass

    @task(task_id='dbt_run_ecmwf_snowflake')
    def execute_dbt_run_sn():
        print("ECMWF dataset updated. Running Cloud Snowflake ECMWF dbt models...")
        # cmd='bash -c "dbt run --select path:models/ecmwf --profiles-dir . --target prod"'
        pass

    execute_dbt_run_pg()
    execute_dbt_run_sn()

# Instantiate all DAGs
gfs_extract_dag = extract_gfs()
ecmwf_extract_dag = extract_ecmwf()
gfs_transform_dag = transform_gfs()
ecmwf_transform_dag = transform_ecmwf()