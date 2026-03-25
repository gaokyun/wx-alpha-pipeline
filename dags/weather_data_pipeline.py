import os
import requests
import pendulum
from bs4 import BeautifulSoup
from airflow.sdk import dag, task, Asset
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'amzn-s3-ykg-storage')
SNOWFLAKE_CONN_ID = "snowflake_default"

# ---------------- 1. Global Pipeline Configuration ----------------
# Buffer hours act as the "Wake Up" time for the Airflow Sensors.
SCHEDULES = {
    'aifs-upper': 6.93, 'aifs-surface': 6.93, 'aifs-spread': 7.57,
    'ifs-upper': 7.57, 'ifs-surface': 6.93, 'ifs-spread': 7.67, 
    'gfs-upper': 4.67
}

# The forecast hours to extract. The sensors will look for the max step (360).
TARGET_STEPS = [192, 240, 288, 360]

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
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5), 
}

# ---------------- 3. Time & Schedule Translators ----------------
def generate_cron(buffer_hours: float, model: str) -> str:
    """Translates floating point hours into exact cron expressions."""
    cycles = [0, 12] if model == 'ifs' else [0, 6, 12, 18]
    minutes = int(round((buffer_hours % 1) * 60))
    hours_offset = int(buffer_hours)
    cron_hours = [(c + hours_offset) % 24 for c in cycles]
    cron_hours_str = ",".join(map(str, sorted(cron_hours)))
    return f"{minutes} {cron_hours_str} * * *"

def get_cycle_and_date(trigger_time: pendulum.DateTime, task_key: str):
    """Rewinds the buffer amount to snap back to the origin model cycle."""
    # Safety fallback: If Airflow context injection fails, use 'now'
    if trigger_time is None:
        trigger_time = pendulum.now("UTC")
        
    trigger_time_utc = trigger_time.in_tz('UTC')
    buffer_hours = SCHEDULES.get(task_key, 4.67)
    nominal_time = trigger_time_utc.subtract(minutes=int(buffer_hours * 60))
    cycle = round(nominal_time.hour / 6) * 6
    target_date = nominal_time.start_of('day')
    
    if cycle == 24:
        cycle = 0
        target_date = target_date.add(days=1)
        
    return target_date, cycle

# ---------------- 4. Web Scraper Sensors ----------------
def check_gfs_nomads_ready(**kwargs):
    """Pokes NOMADS to ensure the specific run has reached the final target step."""
    target_date, cycle = get_cycle_and_date(kwargs['data_interval_end'], kwargs['task_key'])
    
    date_str = target_date.format('YYYYMMDD')
    cycle_str = f"{cycle:02d}"
    url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{date_str}/{cycle_str}/atmos/"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return False
            
        max_step = max(TARGET_STEPS)
        sentinel_file = f"gfs.t{cycle_str}z.pgrb2.0p25.f{max_step}"
        
        if sentinel_file in response.text:
            print(f"✅ GFS {cycle_str}z fully available. Found sentinel: {sentinel_file}.")
            return True
        return False
    except Exception as e:
        print(f"⚠️ NOMADS Sensor Error: {e}")
        return False

def check_ecmwf_index_ready(**kwargs):
    """Pokes ECMWF Open Data index to ensure all required steps are published."""
    mod, ttyp = kwargs['mod'], kwargs['ttyp']
    target_date, cycle = get_cycle_and_date(kwargs['data_interval_end'], kwargs['task_key'])
    
    date_str = target_date.format('YYYYMMDD')
    cycle_str = f"{cycle:02d}z"
    
    # Product mapping
    model_path = "aifs-single" if mod == 'aifs' else "ifs"
    if mod == 'aifs' and ttyp == 'spread':
        model_path = "aifs-ens"
    product = "enfo" if ttyp == 'spread' else "oper"
    
    url = f"https://data.ecmwf.int/forecasts/{date_str}/{cycle_str}/{model_path}/0p25/{product}/"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return False
            
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr')
        files = [row.find('td').text.strip() for row in rows if row.find('td')]
        
        max_step = max(TARGET_STEPS)
        target_file = f"{max_step}h-{product}-fc.grib2"
        
        if any(target_file in f for f in files):
            print(f"✅ ECMWF {mod}-{ttyp} {cycle_str} is ready. Found {target_file}.")
            return True
        return False
    except Exception as e:
        print(f"⚠️ ECMWF Sensor Error: {e}")
        return False

# ---------------- 5. Dynamic Extraction Engine ----------------
def create_extraction_dag(t_key: str, mod: str, ttyp: str, buf_hours: float):
    dag_id = f'weather_ops.extract.{mod}.{ttyp}'
    cron_expr = generate_cron(buf_hours, mod)
    
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule=cron_expr,
        start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
        catchup=False,
        tags=['extract', 'meteorology', mod, ttyp]
    )
    def dynamic_extract():
        
        # 1. The Sensor Gate
        sensor_callable = check_gfs_nomads_ready if mod == 'gfs' else check_ecmwf_index_ready
        wait_for_data = PythonSensor(
            task_id=f"sensor_wait_for_{mod}_{ttyp}",
            python_callable=sensor_callable,
            op_kwargs={'task_key': t_key, 'mod': mod, 'ttyp': ttyp},
            mode="reschedule",
            poke_interval=180, 
            timeout=7200       
        )

        # 2. Download Task
        @task(task_id=f'download_{mod}_{ttyp}')
        def run_download(data_interval_end: pendulum.DateTime = None):
            target_date, cycle = get_cycle_and_date(data_interval_end, t_key)
            print(f"🚀 Extracting {mod.upper()} {ttyp.upper()} | Date: {target_date.format('YYYY-MM-DD')} | Cycle: {cycle}z")
            
            if mod == 'gfs':
                from etl.meteorology import download_gfs_robust
                for step in TARGET_STEPS:
                    if not download_gfs_robust(target_date, cycle, step): 
                        raise Exception(f"GFS failed at step {step}h")
            else:
                from etl.meteorology import download_ecmwf_unified
                if ttyp == 'spread' and cycle not in [0, 12]:
                    print(f"Skipping {mod}-spread for cycle {cycle}z.")
                    return "SKIPPED"
                    
                for step in TARGET_STEPS:
                    if not download_ecmwf_unified(target_date, cycle, step, mod, ttyp):
                        raise Exception(f"ECMWF failed at step {step}h")
                        
            return "SUCCESS"

        # 3. Hybrid Snowflake Refresh Task
        @task(task_id=f'refresh_snowflake_{mod}_{ttyp}', outlets=[ASSETS[t_key]])
        def refresh_metadata(data_interval_end: pendulum.DateTime = None):
            target_date, cycle = get_cycle_and_date(data_interval_end, t_key)
            
            # SPEED PATH: Real-time refresh for the high-priority 12z run
            if cycle == 12:
                hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
                table_name = f"{mod}_{ttyp}"
                # Handle edge case where mapping needs 'at_' prefix for ECMWF
                if mod != 'gfs' and ttyp != 'spread':
                    table_name = f"{table_name}"
                    
                sql = f"ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.{table_name} REFRESH;"
                print(f"⚡ 12z REAL-TIME MODE: Syncing {table_name} immediately.")
                hook.run(sql)
            else:
                print(f"⏳ {cycle}z BATCH MODE: Skipping individual refresh to save Snowflake compute.")
                pass 
                
        wait_for_data >> run_download() >> refresh_metadata()
        
    return dynamic_extract()

# Spawn Extraction DAGs
for task_key, buffer_hours in SCHEDULES.items():
    model, ttype = task_key.split('-')
    globals()[f"extract_{model}_{ttype}_dag"] = create_extraction_dag(task_key, model, ttype, buffer_hours)

# ---------------- 6. Transformation DAGs (Silver/Gold Layer) ----------------

@dag(
    dag_id='weather_ops.transform.gfs_dbt_snowflake',
    default_args=default_args,
    schedule=[ASSETS['gfs-upper']], 
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    tags=['dbt', 'snowflake', 'gfs']
)
def transform_gfs_snowflake():
    
    @task(task_id='cost_optimized_batch_refresh_gfs')
    def batch_refresh_metadata(data_interval_end: pendulum.DateTime = None):
        target_date, cycle = get_cycle_and_date(data_interval_end, 'gfs-upper')
        
        # COST PATH: Execute bulk refresh
        if cycle != 12:
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            sql = "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.gfs_upper REFRESH;"
            print(f"💰 {cycle}z BATCH MODE: Executing GFS refresh.")
            hook.run(sql)
        else:
            print(f"⚡ 12z REAL-TIME MODE: GFS table already refreshed. Skipping.")

    @task(task_id='dbt_run_gfs_snowflake')
    def execute_gfs_models():
        from etl.meteorology import run_dbt_command
        run_dbt_command(command="run", select_path="path:models/staging/gfs+")

    batch_refresh_metadata() >> execute_gfs_models()

@dag(
    dag_id='weather_ops.transform.ecmwf_dbt_snowflake',
    default_args=default_args,
    # FIX: Require both essential datasets to finish before triggering DBT
    schedule=[ASSETS['aifs-upper'], ASSETS['aifs-surface']], 
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    tags=['dbt', 'snowflake', 'ecmwf']
)
def transform_ecmwf_snowflake():

    @task(task_id='cost_optimized_batch_refresh_ecmwf')
    def batch_refresh_metadata(data_interval_end: pendulum.DateTime = None):
        target_date, cycle = get_cycle_and_date(data_interval_end, 'aifs-upper')
        
        if cycle != 12:
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            sql_commands = [
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.AIFS_UPPER REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.AIFS_SURFACE REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.AIFS_SPREAD REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.IFS_UPPER REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.IFS_SURFACE REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.IFS_SPREAD REFRESH;"
            ]
            print(f"💰 {cycle}z BATCH MODE: Executing bulk ECMWF refresh.")
            hook.run(sql_commands) 
            # Note: The duplicate sql = """...""" block is completely removed.
        else:
            print(f"⚡ 12z REAL-TIME MODE: ECMWF tables already refreshed. Skipping bulk.")

        target_date, cycle = get_cycle_and_date(data_interval_end, 'aifs-upper')
        
        # COST PATH: Execute bulk refresh
        if cycle != 12:
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            
            # 🛠️ FIX: Pass as a list of independent commands to satisfy the Snowflake Connector
            sql_commands = [
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.aifs_upper REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.aifs_surface REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.aifs_spread REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.ifs_upper REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.ifs_surface REFRESH;",
                "ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.ifs_spread REFRESH;"
            ]
            print(f"💰 {cycle}z BATCH MODE: Executing bulk ECMWF refresh.")
            hook.run(sql_commands) 
        else:
            print(f"⚡ 12z REAL-TIME MODE: ECMWF tables already refreshed. Skipping bulk.")

        target_date, cycle = get_cycle_and_date(data_interval_end, 'aifs-upper')
        
        # COST PATH: Execute bulk refresh
        if cycle != 12:
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            # Refreshes all tables simultaneously, utilizing a single Warehouse spin-up
            sql = """
                ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.aifs_upper REFRESH;
                ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.aifs_surface REFRESH;
                ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.aifs_spread REFRESH;
                ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.ifs_upper REFRESH;
                ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.ifs_surface REFRESH;
                ALTER EXTERNAL TABLE PHYSICAL_METEOR_DB.RAW.ifs_spread REFRESH;
            """
            print(f"💰 {cycle}z BATCH MODE: Executing bulk ECMWF refresh.")
            hook.run(sql)
        else:
            print(f"⚡ 12z REAL-TIME MODE: ECMWF tables already refreshed. Skipping bulk.")

    @task(task_id='dbt_run_ecmwf_snowflake')
    def execute_ecmwf_models():
        from etl.meteorology import run_dbt_command
        run_dbt_command(command="run", select_path="path:models/staging/ecmwf+")

    batch_refresh_metadata() >> execute_ecmwf_models()

gfs_transform_snowflake_dag = transform_gfs_snowflake()
ecmwf_transform_snowflake_dag = transform_ecmwf_snowflake()