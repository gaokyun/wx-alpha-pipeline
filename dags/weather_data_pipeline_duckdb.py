import os
import requests
import pendulum
from bs4 import BeautifulSoup
from functools import reduce
import operator

from airflow.sdk import dag, task, Asset
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.providers.standard.sensors.python import PythonSensor

OCI_BUCKET = os.getenv('OCI_OBJECT_STORAGE_BUCKET', 'oci-s3-ykg-storage')
DBT_PROJECT_PATH = os.getenv('DBT_PROJECT_PATH', '/opt/airflow/physical_meteor')
HOST_PROJECT_PATH = os.getenv('HOST_PROJECT_PATH', '/home/airflow/dev/wx-alpha-pipeline')

DUCKDB_POOL = 'dph_single_writer'

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
            soft_fail=True,
            timeout=7200
        )
        def wait_for_data(data_interval_end: pendulum.DateTime = None) -> bool:
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

        @task(task_id=f'download_{mod}_{ttyp}', outlets=[ASSETS[t_key]])
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

from airflow.sdk import TaskGroup

WEATHER_MODELS = {
    'gfs_upper': {'asset_trigger': ASSETS['gfs-upper'], 'selector': 'stg_gfs_upper+', 'match_key': 'gfs_raw/gfs_upper'},
    'gfs_surface': {'asset_trigger': ASSETS['gfs-surface'], 'selector': 'stg_gfs_surface+', 'match_key': 'gfs_raw/gfs_surface'},
    'aifs_upper': {'asset_trigger': ASSETS['aifs-upper'], 'selector': 'stg_ecmwf_aifs_upper+', 'match_key': 'ecmwf_raw/at_aifs_upper'},
    'aifs_surface': {'asset_trigger': ASSETS['aifs-surface'], 'selector': 'stg_ecmwf_aifs_surface+', 'match_key': 'ecmwf_raw/at_aifs_surface'},
    'aifs_spread': {'asset_trigger': ASSETS['aifs-spread'], 'selector': 'stg_ecmwf_aifs_spread+', 'match_key': 'ecmwf_raw/aifs_spread'},
    'ifs_upper': {'asset_trigger': ASSETS['ifs-upper'], 'selector': 'stg_ecmwf_ifs_upper+', 'match_key': 'ecmwf_raw/at_ifs_upper'},
    'ifs_surface': {'asset_trigger': ASSETS['ifs-surface'], 'selector': 'stg_ecmwf_ifs_surface+', 'match_key': 'ecmwf_raw/at_ifs_surface'},
    'ifs_spread': {'asset_trigger': ASSETS['ifs-spread'], 'selector': 'stg_ecmwf_ifs_spread+', 'match_key': 'ecmwf_raw/ifs_spread'},
}

@dag(
    dag_id='weather_ops.transform.all_models_dbt_duckdb',
    default_args=default_args,
    schedule=reduce(operator.or_, ASSETS.values()), # Trigger on ANY of the 8 assets updating
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    max_active_runs=1, # Ensure only one run executes at a time to queue subsequent trigger events
    doc_md="Consolidated DuckDB transformation pipeline with TaskGroup visual layout, 5-minute debounce window, and Variable-based event coalescing.",
    tags=['dbt', 'duckdb', 'gold', 'consensus']
)
def unified_forecast_transform_duckdb():

    # 1. Debounce task: sleeps until 5 minutes have elapsed since the DAG run start time
    # This allows all incoming assets triggered within a 5-minute window to register before processing.
    def check_debounce_time(dag_run, **context):
        import pendulum
        elapsed = pendulum.now("UTC") - dag_run.start_date
        return elapsed.total_seconds() >= 300

    wait_node = PythonSensor(
        task_id='wait_for_pooling',
        python_callable=check_debounce_time,
        poke_interval=60,
        timeout=600,
        mode='reschedule'
    )

    # 2. Branching task to determine which targets to execute based on new asset events
    @task.branch(task_id='choose_branch')
    def determine_branches(**context):
        # Allow manual override via dag_run.conf (e.g. triggered via UI/CLI with config)
        dag_run = context.get("dag_run")
        if dag_run and dag_run.conf and "branches" in dag_run.conf:
            return dag_run.conf["branches"]

        import psycopg2

        # 1. Connect to PHYSICAL_METEOR_DB to get the last processed event ID
        conn_pm = psycopg2.connect(host='postgres', user='airflow', password='airflow', database='PHYSICAL_METEOR_DB', port=5432)
        cursor_pm = conn_pm.cursor()
        cursor_pm.execute("CREATE TABLE IF NOT EXISTS raw.transform_coalesce_state (last_processed_event_id BIGINT);")
        cursor_pm.execute("SELECT last_processed_event_id FROM raw.transform_coalesce_state LIMIT 1;")
        row_pm = cursor_pm.fetchone()
        last_id = row_pm[0] if row_pm else 0
        cursor_pm.close()
        conn_pm.close()

        # 2. Connect to airflow metadata DB to query new asset events
        conn_af = psycopg2.connect(host='postgres', user='airflow', password='airflow', database='airflow', port=5432)
        cursor_af = conn_af.cursor()
        sql = """
            SELECT ae.id, a.uri FROM asset_event ae
            JOIN asset a ON ae.asset_id = a.id
            WHERE ae.id > %s 
              AND a.name IN ('gfs_upper', 'gfs_surface', 'at_ifs_upper', 'at_ifs_surface', 'ifs_spread', 'at_aifs_upper', 'at_aifs_surface', 'aifs_spread')
            ORDER BY ae.id ASC;
        """
        cursor_af.execute(sql, (last_id,))
        res = cursor_af.fetchall()
        cursor_af.close()
        conn_af.close()

        run_type = dag_run.run_type if dag_run else "manual"
        is_manual = run_type == "manual"

        # If it was manual run, run all models
        if is_manual:
            all_tasks = []
            for model_id in WEATHER_MODELS.keys():
                family = model_id.split('_')[0]
                all_tasks.append(f"{family}.dbt_run_{model_id}")
            return all_tasks

        # If no new asset events since last run, skip all downstream processing
        if not res:
            return []

        # Track the latest event ID we are processing in this batch
        max_id = max([row[0] for row in res])
        ready_uris = [row[1] for row in res]

        # 3. Update the state in PHYSICAL_METEOR_DB
        conn_pm = psycopg2.connect(host='postgres', user='airflow', password='airflow', database='PHYSICAL_METEOR_DB', port=5432)
        cursor_pm = conn_pm.cursor()
        cursor_pm.execute("DELETE FROM raw.transform_coalesce_state;")
        cursor_pm.execute("INSERT INTO raw.transform_coalesce_state (last_processed_event_id) VALUES (%s);", (max_id,))
        conn_pm.commit()
        cursor_pm.close()
        conn_pm.close()

        branches = []
        for uri in ready_uris:
            for model_id, config in WEATHER_MODELS.items():
                if config['match_key'] in uri:
                    family = model_id.split('_')[0]
                    branches.append(f"{family}.dbt_run_{model_id}")

        return list(set(branches))

    # B. Define groups and dbt execution tasks
    branch_node = determine_branches()

    # C. Unified Postgres gold views refresh task (Early Bird Mode)
    unified_gold = DockerOperator(
        task_id='refresh_gold_unified',
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
        command="dbt run --project-dir /usr/app/physical_meteor --profiles-dir /usr/app/physical_meteor --target dev_postgres --select fct_upper_forecast fct_surface_forecast fct_spread_forecast",
        pool='default_pool',
        trigger_rule='none_failed_min_one_success'
    )
    
    def _make_dbt_task(mid: str, sel: str):
        """Factory that creates a uniquely-scoped @task per model, avoiding the
        duplicate qualname bug that occurs when @task functions are redefined
        inside a loop with the same name."""
        @task(task_id=f'dbt_run_{mid}', pool=DUCKDB_POOL)
        def _run_dbt_model(selector: str = sel):
            from etl.meteorology_duckdb import run_dbt_duckdb
            run_dbt_duckdb(command="run", select_path=selector)
        return _run_dbt_model

    # 1. Initialize a list to hold the TaskGroup objects
    dbt_task_groups = []
    
    families = ['aifs', 'gfs', 'ifs']
    for family in families:
        # 2. Assign the context manager to a variable (tg)
        with TaskGroup(group_id=family) as tg:
            family_models = {k: v for k, v in WEATHER_MODELS.items() if k.startswith(family)}

            for model_id, config in family_models.items():
                # Simply instantiate the task; it automatically binds to the current TaskGroup (tg)
                t_instance = _make_dbt_task(model_id, config['selector'])()

        # 3. Append the populated TaskGroup to our list
        dbt_task_groups.append(tg)

    # 4. Map the dependencies at the root level using the TaskGroup objects
    wait_node >> branch_node >> dbt_task_groups >> unified_gold

unified_forecast_transform_duckdb()


@dag(
    dag_id='weather_ops.transform.unified_forecast_refresh_v2',
    default_args=default_args,
    schedule=None,  # Triggered by centralized master DAG
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    doc_md="Refreshes the final consensus views for both Upper Air and Surface metrics.",
    tags=['dbt', 'duckdb', 'consensus', 'gold']
)
def refresh_unified_forecasts_v2():

    def dbt_task(task_id, select_statement, image="dbt-duckdb:latest", target="dev_duckdb_postgres", pool=DUCKDB_POOL):
        return DockerOperator(
            task_id=task_id,
            image=image,
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
            command=f"dbt run --project-dir /usr/app/physical_meteor --profiles-dir /usr/app/physical_meteor --target {target} --select {select_statement}",
            pool=pool,
        )

    stg_refresh = dbt_task('refresh_silver_layer', 'tag:silver')
    aifs_gold = dbt_task('gold_aifs', 'fct_aifs_upper fct_aifs_surface fct_aifs_spread')
    ifs_gold = dbt_task('gold_ifs', 'fct_ifs_upper fct_ifs_surface fct_ifs_spread')
    gfs_gold = dbt_task('gold_gfs', 'fct_gfs_upper fct_gfs_surface')
    unified_gold = dbt_task('gold_unified', 'fct_upper_forecast fct_surface_forecast fct_spread_forecast', image="dbt-postgres:latest", target="dev_postgres", pool='default_pool')

    stg_refresh >> [gfs_gold, aifs_gold, ifs_gold] >> unified_gold


refresh_unified_forecasts_v2()
