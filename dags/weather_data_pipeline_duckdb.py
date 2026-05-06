import os
import requests
import pendulum
from bs4 import BeautifulSoup

# --- AIRFLOW 2.x COMPATIBLE IMPORTS ---
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.sensors.python import PythonSensor

# Use the OCI bucket variable to match the new OCI Object Storage architecture
OCI_BUCKET = os.getenv('OCI_OBJECT_STORAGE_BUCKET', 'oci-s3-ykg-storage')

# ---------------- 1. Global Pipeline Configuration ----------------
SCHEDULES = {
    'aifs-upper': 6.93, 'aifs-surface': 6.93, 'aifs-spread': 7.57,
    'ifs-upper': 7.57, 'ifs-surface': 6.93, 'ifs-spread': 7.67, 
    'gfs-upper': 4.67
}

TARGET_STEPS = [192, 240, 288, 360]

# ---------------- 2. Granular Dataset Definitions ----------------
# Airflow Datasets now track the raw Delta Lake paths on OCI Object Storage.
# When the extraction completes, it updates these datasets, which automatically triggers dbt.
ASSETS = {
    'gfs-upper': Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/gfs_raw/'),
    'aifs-upper': Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/'),
    'aifs-surface': Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_aifs_surface/'),
    'aifs-spread': Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/aifs_spread/'),
    'ifs-upper': Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_upper/'),
    'ifs-surface': Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_surface/'),
    'ifs-spread': Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/ifs_spread/')
}

default_args = {
    'owner': 'meteorologist',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5), 
}

# ---------------- 3. Time & Schedule Translators ----------------
def generate_cron(buffer_hours: float, model: str) -> str:
    cycles = [0, 12] if model == 'ifs' else [0, 6, 12, 18]
    minutes = int(round((buffer_hours % 1) * 60))
    hours_offset = int(buffer_hours)
    cron_hours = [(c + hours_offset) % 24 for c in cycles]
    cron_hours_str = ",".join(map(str, sorted(cron_hours)))
    return f"{minutes} {cron_hours_str} * * *"

def get_cycle_and_date(trigger_time: pendulum.DateTime, task_key: str):
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
            return True
        return False
    except Exception as e:
        print(f"⚠️ NOMADS Sensor Error: {e}")
        return False

def check_ecmwf_index_ready(**kwargs):
    mod, ttyp = kwargs['mod'], kwargs['ttyp']
    # Use data_interval_end as you had it, but ensure get_cycle_and_date is robust
    target_date, cycle = get_cycle_and_date(kwargs['data_interval_end'], kwargs['task_key'])
    
    date_str = target_date.format('YYYYMMDD')
    cycle_str = f"{cycle:02d}z"
    
    model_path = "aifs-single" if mod == 'aifs' else "ifs"
    if mod == 'aifs' and ttyp == 'spread':
        model_path = "aifs-ens"
    
    product = "enfo" if ttyp == 'spread' else "oper"
    
    url = f"https://data.ecmwf.int/forecasts/{date_str}/{cycle_str}/{model_path}/0p25/{product}/"
    
    # 1. ADDED: Clear visibility in Airflow logs
    print(f"📡 Poking ECMWF URL: {url}")
    
    try:
        response = requests.get(url, timeout=15)
        if response.status_code != 200:
            print(f"⚠️ URL not found yet (Status: {response.status_code})")
            return False

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 2. FIXED: Scrape <a> tags because the file list is in a <pre> block, not a <tr> table
        links = soup.find_all('a')
        filenames = [a.text.strip() for a in links if a.text]
        
        # 3. FIXED: Naming convention. 'enfo' (Spread) uses -ep/-pf, not -fc
        suffix = "-ep.grib2" if product == "enfo" else "-fc.grib2"
        
        max_step = max(TARGET_STEPS)
        # We use a suffix match because filenames have a 14-digit timestamp prefix
        target_pattern = f"-{max_step}h-{product}{suffix}"
        
        if any(target_pattern in f for f in filenames):
            print(f"✅ Found target file matching: {target_pattern}")
            return True
        
        print(f"⏳ Index exists, but {target_pattern} is not yet published.")
        return False

    except Exception as e:
        # This will now catch that DNS error we saw earlier
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
        tags=['extract', 'meteorology', mod, ttyp, 'oci']
    )
    def dynamic_extract():
        
        sensor_callable = check_gfs_nomads_ready if mod == 'gfs' else check_ecmwf_index_ready
        
        wait_for_data = PythonSensor(
            task_id=f"sensor_wait_for_{mod}_{ttyp}",
            python_callable=sensor_callable,
            op_kwargs={'task_key': t_key, 'mod': mod, 'ttyp': ttyp},
            mode="reschedule",
            poke_interval=180,
            timeout=7200
        )

        # The 'outlets' property automatically signals Airflow that this OCI dataset has been updated
        @task(task_id=f'download_{mod}_{ttyp}', outlets=[ASSETS[t_key]])
        def run_download(data_interval_end: pendulum.DateTime = None):
            target_date, cycle = get_cycle_and_date(data_interval_end, t_key)
            if mod == 'gfs':
                from etl.meteorology_duckdb import download_gfs_robust
                if not download_gfs_robust(target_date, cycle, TARGET_STEPS): 
                    raise Exception("GFS extraction batch failed")
            else:
                from etl.meteorology_duckdb import download_ecmwf_unified
                if ttyp == 'spread' and cycle not in [0, 12]:
                    return "SKIPPED"
                if not download_ecmwf_unified(target_date, cycle, TARGET_STEPS, mod, ttyp):
                    raise Exception("ECMWF extraction batch failed")
            return "SUCCESS"
                
        wait_for_data >> run_download()
        
    return dynamic_extract()

for task_key, buffer_hours in SCHEDULES.items():
    model, ttype = task_key.split('-')
    globals()[f"extract_{model}_{ttype}_dag"] = create_extraction_dag(task_key, model, ttype, buffer_hours)

# ---------------- 6. Transformation DAGs (Silver/Gold Layer) ----------------
# 6a. GFS Transformation (The Early Bird)
@dag(
    dag_id='weather_ops.transform.gfs_dbt_duckdb',
    default_args=default_args,
    schedule=[ASSETS['gfs-upper']], # Triggered the moment GFS raw lands in OCI
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    tags=['dbt', 'duckdb', 'gfs', 'gold_staging']
)
def transform_gfs_duckdb():
    
    @task(task_id='dbt_run_gfs_atomic')
    def execute_gfs_models():
        from etl.meteorology_duckdb import run_dbt_duckdb
        # ✅ SURGICAL STRIKE: Run GFS staging and only the GFS atomic mart.
        # This ignores IFS/AIFS entirely, saving OCI egress and CPU.
        run_dbt_duckdb(command="run", select_path="stg_gfs_upper+")

    execute_gfs_models()

# 6b. AIFS Transformation (The Mid-Day AI Model)
@dag(
    dag_id='weather_ops.transform.aifs_dbt_duckdb',
    default_args=default_args,
    schedule=[ASSETS['aifs-upper']], 
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    tags=['dbt', 'duckdb', 'aifs', 'gold_staging']
)
def transform_aifs_duckdb():

    @task(task_id='dbt_run_aifs_atomic')
    def execute_aifs_models():
        from etl.meteorology_duckdb import run_dbt_duckdb
        # ✅ SURGICAL STRIKE: Build the AI model slice only.
        run_dbt_duckdb(command="run", select_path="stg_ecmwf_aifs_upper+")

    execute_aifs_models()

# 6c. IFS Transformation (The King - Usually Last)
@dag(
    dag_id='weather_ops.transform.ifs_dbt_duckdb',
    default_args=default_args,
    schedule=[ASSETS['ifs-upper']], 
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    tags=['dbt', 'duckdb', 'ifs', 'gold_staging']
)
def transform_ifs_duckdb():

    @task(task_id='dbt_run_ifs_atomic')
    def execute_ifs_models():
        from etl.meteorology_duckdb import run_dbt_duckdb
        # ✅ SURGICAL STRIKE: Build the high-resolution IFS slice.
        run_dbt_duckdb(command="run", select_path="stg_ecmwf_ifs_upper+")

    execute_ifs_models()

# 6d. Unified View Refresh (Optional / Zero-Cost)
@dag(
    dag_id='weather_ops.transform.unified_view_refresh',
    default_args=default_args,
    # This triggers whenever ANY of the atomic marts are updated
    schedule=[Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/gfs_raw/'), 
              Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/'),
              Dataset(f's3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_upper/')],
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    tags=['dbt', 'duckdb', 'consensus']
)
def refresh_unified_view():
    @task(task_id='dbt_run_unified_view')
    def run_view():
        from etl.meteorology_duckdb import run_dbt_duckdb
        # This just ensures the View definition is healthy. Costs near 0.
        run_dbt_duckdb(command="run", select_path="fct_unified_forecast")
    
    run_view()

# Instantiate the DAGs
gfs_dag = transform_gfs_duckdb()
aifs_dag = transform_aifs_duckdb()
ifs_dag = transform_ifs_duckdb()
unified_dag = refresh_unified_view()