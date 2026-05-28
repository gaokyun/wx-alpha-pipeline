import os
import requests
import pendulum
from bs4 import BeautifulSoup

from airflow.sdk import dag, task, Asset
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

OCI_BUCKET = os.getenv('OCI_OBJECT_STORAGE_BUCKET', 'oci-s3-ykg-storage')
DBT_PROJECT_PATH = os.getenv('DBT_PROJECT_PATH', '/opt/airflow/physical_meteor')
HOST_PROJECT_PATH = os.getenv('HOST_PROJECT_PATH', '/home/airflow/dev/wx-alpha-pipeline')

DUCKDB_POOL = 'duckdb_single_writer'

SCHEDULES = {
    'aifs-upper': 6.93,
    'aifs-surface': 6.93,
    'aifs-spread': 7.57,
    'ifs-upper': 7.57,
    'ifs-surface': 6.93,
    'ifs-spread': 7.67,
    'gfs-upper': 4.67,
    'gfs-surface': 4.67,
}

TARGET_STEPS = [192, 240, 288, 360]

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

        @task.sensor(
            task_id=f"sensor_wait_for_{mod}_{ttyp}",
            mode="reschedule",
            poke_interval=180,
            timeout=7200
        )
        def wait_for_data(data_interval_end: pendulum.DateTime) -> bool:
            target_date, cycle = get_cycle_and_date(data_interval_end, t_key)
            date_str = target_date.format('YYYYMMDD')
            
            if mod == 'gfs':
                cycle_str = f"{cycle:02d}"
                url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{date_str}/{cycle_str}/atmos/"
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code != 200:
                        return False
                    max_step = max(TARGET_STEPS)
                    sentinel_file = f"gfs.t{cycle_str}z.pgrb2.0p25.f{max_step}"
                    return sentinel_file in response.text
                except Exception as e:
                    print(f"⚠️ NOMADS Sensor Error: {e}")
                    return False
            else:
                cycle_str = f"{cycle:02d}z"
                model_path = "aifs-single" if mod == 'aifs' else "ifs"
                if mod == 'aifs' and ttyp == 'spread':
                    model_path = "aifs-ens"

                product = "enfo" if ttyp == 'spread' else "oper"
                url = f"https://data.ecmwf.int/forecasts/{date_str}/{cycle_str}/{model_path}/0p25/{product}/"
                print(f"📡 Poking ECMWF URL: {url}")
                try:
                    response = requests.get(url, timeout=15)
                    if response.status_code != 200:
                        print(f"⚠️ URL not found yet (Status: {response.status_code})")
                        return False

                    soup = BeautifulSoup(response.text, 'html.parser')
                    links = soup.find_all('a')
                    filenames = [a.text.strip() for a in links if a.text]

                    suffix = "-ep.grib2" if product == "enfo" else "-fc.grib2"
                    max_step = max(TARGET_STEPS)
                    target_pattern = f"-{max_step}h-{product}{suffix}"

                    if any(target_pattern in f for f in filenames):
                        print(f"✅ Found target file matching: {target_pattern}")
                        return True

                    print(f"⏳ Index exists, but {target_pattern} is not yet published.")
                    return False
                except Exception as e:
                    print(f"⚠️ ECMWF Sensor Error: {e}")
                    return False

        @task(task_id=f'download_{mod}_{ttyp}', outlets=[ASSETS[t_key]], pool=DUCKDB_POOL)
        def run_download(data_interval_end: pendulum.DateTime = None):
            target_date, cycle = get_cycle_and_date(data_interval_end, t_key)
            if mod == 'gfs':
                from etl.meteorology_duckdb import download_gfs_robust
                if not download_gfs_robust(target_date, cycle, TARGET_STEPS, task_type=ttyp):
                    raise Exception(f"GFS {ttyp} extraction batch failed")
            else:
                from etl.meteorology_duckdb import download_ecmwf_unified
                if ttyp == 'spread' and cycle not in [0, 12]:
                    return "SKIPPED"
                if not download_ecmwf_unified(target_date, cycle, TARGET_STEPS, mod, ttyp):
                    raise Exception(f"ECMWF {ttyp} extraction batch failed")
            return "SUCCESS"

        wait_for_data() >> run_download()

    return dynamic_extract()


for task_key, buffer_hours in SCHEDULES.items():
    model, ttype = task_key.split('-')
    globals()[f"extract_{model}_{ttype}_dag"] = create_extraction_dag(task_key, model, ttype, buffer_hours)

WEATHER_MODELS = {
    'gfs': {
        'asset_trigger': [ASSETS['gfs-upper'], ASSETS['gfs-surface']],
        'selector': 'stg_gfs_upper+ stg_gfs_surface+',
        'tags': ['gfs'],
        'desc': 'Global Forecast System'
    },
    'aifs': {
        'asset_trigger': [ASSETS['aifs-upper'], ASSETS['aifs-surface'], ASSETS['aifs-spread']],
        'selector': 'stg_ecmwf_aifs_upper+ stg_ecmwf_aifs_surface+ stg_ecmwf_aifs_spread+',
        'tags': ['aifs', 'ai'],
        'desc': 'ECMWF Artificial Intelligence Forecast'
    },
    'ifs': {
        'asset_trigger': [ASSETS['ifs-upper'], ASSETS['ifs-surface'], ASSETS['ifs-spread']],
        'selector': 'stg_ecmwf_ifs_upper+ stg_ecmwf_ifs_surface+ stg_ecmwf_ifs_spread+',
        'tags': ['ifs', 'high_res'],
        'desc': 'ECMWF Integrated Forecasting System'
    }
}


def create_weather_dag(model_id, config):
    @dag(
        dag_id=f'weather_ops.transform.{model_id}_dbt_duckdb',
        default_args=default_args,
        schedule=config['asset_trigger'],
        start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
        catchup=False,
        doc_md=f"### {config['desc']} Transformation\nSurgical dbt run for {model_id} family.",
        tags=['dbt', 'duckdb', 'gold'] + config['tags']
    )
    def transform_dag():

        @task(task_id=f'dbt_run_{model_id}_atomic', pool=DUCKDB_POOL)
        def execute_models():
            from etl.meteorology_duckdb import run_dbt_duckdb
            run_dbt_duckdb(
                command="run",
                select_path=config['selector']
            )

        execute_models()

    return transform_dag()


for model_id, config in WEATHER_MODELS.items():
    globals()[f"dag_transform_{model_id}"] = create_weather_dag(model_id, config)


@dag(
    dag_id='weather_ops.transform.unified_forecast_refresh_v2',
    default_args=default_args,
    schedule=[
        ASSETS['gfs-upper'], ASSETS['gfs-surface'],
        ASSETS['aifs-upper'], ASSETS['aifs-surface'],
        ASSETS['ifs-upper'], ASSETS['ifs-surface'],
        ASSETS['ifs-spread'], ASSETS['aifs-spread']
    ],
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    doc_md="Refreshes the final consensus views for both Upper Air and Surface metrics.",
    tags=['dbt', 'duckdb', 'consensus', 'gold']
)
def refresh_unified_forecasts_v2():

    def dbt_task(task_id, select_statement):
        return DockerOperator(
            task_id=task_id,
            image="dbt-duckdb:latest",
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
            command=f"dbt run --project-dir /usr/app/physical_meteor --profiles-dir /usr/app/physical_meteor --target dev_duckdb --select {select_statement}",
            pool=DUCKDB_POOL,
        )

    stg_refresh = dbt_task('refresh_silver_layer', 'tag:silver')
    aifs_gold = dbt_task('gold_aifs', 'fct_aifs_upper fct_aifs_surface fct_aifs_spread')
    ifs_gold = dbt_task('gold_ifs', 'fct_ifs_upper fct_ifs_surface fct_ifs_spread')
    gfs_gold = dbt_task('gold_gfs', 'fct_gfs_upper fct_gfs_surface')
    unified_gold = dbt_task('gold_unified', 'fct_upper_forecast fct_surface_forecast fct_spread_forecast')

    stg_refresh >> gfs_gold >> aifs_gold >> ifs_gold >> unified_gold


refresh_unified_forecasts_v2()