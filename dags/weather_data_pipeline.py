import os
import pendulum
from airflow.sdk import dag, task, Asset

S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'amzn-s3-ykg-storage')

# ---------------- 1. Exact Release Timelines (Buffer Hours) ----------------
SCHEDULES = {
    'aifs-upper': 6.93, 'aifs-surface': 6.93, 'aifs-spread': 7.57,
    'ifs-upper': 7.57, 'ifs-surface': 6.93, 'ifs-spread': 7.67, 
    'gfs-upper': 4.67
}

# ---------------- 2. Granular Asset Definitions ----------------
ASSETS = {
    'gfs-upper': Asset(f's3://{S3_BUCKET}/weather_data/delta_lake/gfs_raw/'),
    'aifs-upper': Asset(f's3://{S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/'),
    'aifs-surface': Asset(f's3://{S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_aifs_surface/'),
    'aifs-spread': Asset(f's3://{S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/aifs_spread/'),
    'ifs-upper': Asset(f's3://{S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_upper/'),
    'ifs-surface': Asset(f's3://{S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_surface/'),
    'ifs-spread': Asset(f's3://{S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/ifs_spread/')
}

default_args = {
    'owner': 'meteorologist',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=5), 
}

# ---------------- 3. Time & Schedule Translators ----------------
def generate_cron(buffer_hours: float, model: str) -> str:
    """Translates floating point hours (e.g., 6.93) into exact cron expressions."""
    # IFS only releases at 00z and 12z. GFS/AIFS run 4 times a day.
    cycles = [0, 12] if model == 'ifs' else [0, 6, 12, 18]
    
    minutes = int(round((buffer_hours % 1) * 60))
    hours_offset = int(buffer_hours)
    
    cron_hours = [(c + hours_offset) % 24 for c in cycles]
    cron_hours_str = ",".join(map(str, sorted(cron_hours)))
    
    return f"{minutes} {cron_hours_str} * * *"

def get_cycle_and_date(trigger_time: pendulum.DateTime, task_key: str):
    """Rewinds the exact buffer amount to snap back to the origin model cycle (00z, 06z, etc.)"""
    trigger_time_utc = trigger_time.in_tz('UTC')
    buffer_hours = SCHEDULES.get(task_key, 4.67)
    
    # Rewind by the buffer duration to find nominal time
    nominal_time = trigger_time_utc.subtract(minutes=int(buffer_hours * 60))
    
    # Round to the nearest 6-hour block (0, 6, 12, 18)
    cycle = round(nominal_time.hour / 6) * 6
    target_date = nominal_time.start_of('day')
    
    # Handle day boundary wrap-around (e.g., if rounded up to 24)
    if cycle == 24:
        cycle = 0
        target_date = target_date.add(days=1)
        
    return target_date, cycle

# ---------------- 4. Dynamic DAG Generation Engine ----------------
def create_extraction_dag(t_key: str, mod: str, ttyp: str, buf_hours: float):
    """Factory function to isolate scope and generate highly specific extraction DAGs."""
    dag_id = f'extract_{mod}_{ttyp}'
    cron_expr = generate_cron(buf_hours, mod)
    
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule=cron_expr,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        tags=['extract', 'meteorology', mod, ttyp]
    )
    def dynamic_extract():
        @task(task_id=f'download_{mod}_{ttyp}', outlets=[ASSETS[t_key]])
        def run_download(data_interval_end: pendulum.DateTime = None):
            target_date, cycle = get_cycle_and_date(data_interval_end, t_key)
            steps = [192, 240, 288]
            
            print(f"Triggering {mod.upper()} {ttyp.upper()} | Date: {target_date.format('YYYY-MM-DD')} | Cycle: {cycle}z")
            
            if mod == 'gfs':
                from etl.meteorology import download_gfs_robust
                for step in steps:
                    success = download_gfs_robust(target_date, cycle, step)
                    if not success: 
                        raise Exception(f"GFS download failed at step {step}h")
            else:
                from etl.meteorology import download_ecmwf_unified
                
                # Prevent EPS spread from running on intermediate cycles
                if ttyp == 'spread' and cycle not in [0, 12]:
                    print(f"Skipping {mod}-spread for cycle {cycle}z.")
                    return f"SKIPPED_{mod}_SPREAD"
                    
                for step in steps:
                    success = download_ecmwf_unified(
                        target_date, 
                        cycle, 
                        step,
                        target_model=mod, 
                        task_type=ttyp
                    )
                    if not success:
                        raise Exception(f"ECMWF {mod}-{ttyp} download failed at step {step}h")
                        
            return f"{mod.upper()}_{ttyp.upper()}_CYCLE_{cycle}_READY"
            
        run_download()
        
    return dynamic_extract()

# ---------------- 5. Spawn Extraction DAGs ----------------
# Loops through our exact schedules to spawn the 7 independent extraction DAGs dynamically
for task_key, buffer_hours in SCHEDULES.items():
    model, ttype = task_key.split('-')
    globals()[f"extract_{model}_{ttype}_dag"] = create_extraction_dag(task_key, model, ttype, buffer_hours)


# ---------------- 6a. Transformation DAG: GFS ----------------
@dag(
    dag_id='transform_gfs_dbt',
    default_args=default_args,
    schedule=[ASSETS['gfs-upper']], # Triggers strictly when GFS is fully materialized
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dbt', 'transform', 'gfs']
)
def transform_gfs():
    
    @task(task_id='dbt_run_gfs_snowflake')
    def execute_dbt_run_sn():
        print("GFS dataset updated. Running Cloud Snowflake GFS dbt models...")
        # cmd='bash -c "dbt run --select path:models/gfs --target prod"'
        pass

    execute_dbt_run_sn()


# ---------------- 6b. Transformation DAG: ECMWF ----------------
@dag(
    dag_id='transform_ecmwf_dbt',
    default_args=default_args,
    # Triggers only when ALL 6 underlying ECMWF assets have been successfully updated
    schedule=[
        ASSETS['aifs-upper'], ASSETS['aifs-surface'], ASSETS['aifs-spread'],
        ASSETS['ifs-upper'], ASSETS['ifs-surface'], ASSETS['ifs-spread']
    ], 
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dbt', 'transform', 'ecmwf']
)
def transform_ecmwf():

    @task(task_id='dbt_run_ecmwf_snowflake')
    def execute_dbt_run_sn():
        print("All ECMWF assets (AIFS/IFS - Upper/Surface/Spread) updated. Running Snowflake dbt models...")
        # cmd='bash -c "dbt run --select path:models/ecmwf --target prod"'
        pass

    execute_dbt_run_sn()


# Instantiate Transformation DAGs
gfs_transform_dag = transform_gfs()
ecmwf_transform_dag = transform_ecmwf()