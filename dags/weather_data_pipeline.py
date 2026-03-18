import docker
from datetime import datetime, timedelta

# Retaining your Airflow 3.x / SDK imports
from airflow.sdk import dag, task

# Macro Configuration Anchors
default_args = {
    'owner': 'meteorologist',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dbt_ecmwf_gfs_data_pipeline',
    default_args=default_args,
    description='10-Day Outlook Pipeline: GFS/ECMWF parallel load into dbt local/cloud transformation',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['meteorology', 'etl', 'dbt']
)
def dbt_ecmwf_gfs_data_pipeline():

    # ---------------- 1. Data Collection Layer (What) ----------------
    
    # Moved the pool assignment directly into the task decorator
    @task(task_id='download_gfs_data', pool='ecmwf_api_pool')
    def run_gfs_download(data_interval_end: datetime = None) -> str:
        """Downloads GFS data for the pipeline."""
        
        # SCOPE FIX: Import must happen at execution time inside the task
        from etl.meteorology import download_gfs_robust
        
        cycle = 0  # Standard daily run, 0z cycle
        step = 24  # 24h forecasting step
        offset_time = data_interval_end + timedelta(hours=-7, minutes=30)
        target_date = offset_time.replace(hour=0, minute=0, second=0, microsecond=0)

        success = download_gfs_robust(target_date, cycle, step)
        if not success:
            raise Exception("GFS download failed: Surface signal missing (Fake Cold)")
        return "GFS_READY"

    @task(task_id='download_ecmwf_data', pool='ecmwf_api_pool')
    def run_ecmwf_download(data_interval_end: datetime = None) -> str:
        """Downloads ECMWF data for the pipeline using a 6.5-hour Watermark."""
        
        # SCOPE FIX: Import must happen at execution time inside the task
        from etl.meteorology import download_ecmwf_unified
        
        cycle = 0
        step = 24
        
        # 1. Take the physical trigger time (e.g., Mar 16 07:00 UTC)
        # 2. Apply the -6.5 hour watermark (Mar 16 00:30 UTC)
        # 3. Floor it to midnight to get the exact cycle target (Mar 16 00:00 UTC)
        offset_time = data_interval_end + timedelta(hours=-7, minutes=30)
        target_date = offset_time.replace(hour=0, minute=0, second=0, microsecond=0)

        success = download_ecmwf_unified(
            target_date, cycle, step,
            target_models=['AIFS', 'IFS', 'EPS'], 
            task_type=['upper', 'surface', 'spread']
        )
        if not success:
            raise Exception("ECMWF download failed: Lacking upper-level vertical support")
        return "ECMWF_READY"


    # ---------------- 2. Market Translation Layer (So What) ----------------
    
    @task(task_id='dbt_run_postgres')
    def execute_dbt_run_pg(gfs_signal: str, ecmwf_signal: str):
        """Run dbt in Postgres container (Local Audit)"""
        print(f"Physical Audit Confirmed: {gfs_signal}, {ecmwf_signal}")
        
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        
        # SENIOR FIX: Robust Container Lookup List
        # This prevents the DAG from breaking if docker-compose changes the project prefix
        possible_containers = [
            'wx-alpha-pipeline-dbt-postgres-1', 
            'dbt-postgres', 
            'airflow-dbt-postgres-1'
        ]
        
        target_container = None
        for name in possible_containers:
            try:
                target_container = client.containers.get(name)
                break  # Exit loop once we find a matching container
            except docker.errors.NotFound:
                continue
                
        if not target_container:
            raise Exception(f"None of the configured dbt postgres containers were found: {possible_containers}")

        print(f"Executing dbt run inside container: {target_container.name}")
        exit_code, output = target_container.exec_run(
            cmd='bash -c "dbt run --profiles-dir . --target dev"',
            workdir='/usr/app/physical_meteor'
        )
        
        print(output.decode('utf-8'))
        if exit_code != 0:
            raise Exception(f"dbt pg run failed with exit code {exit_code}")

    @task(task_id='dbt_run_snowflake')
    def execute_dbt_run_sn(gfs_signal: str, ecmwf_signal: str):
        """Run dbt in Snowflake container (Cloud Production)"""
        print(f"Physical Audit Confirmed: {gfs_signal}, {ecmwf_signal}")
        
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        container_name = 'dbt-snowflake-runner'
        
        try:
            container = client.containers.get(container_name)
            print(f"Executing dbt run inside container: {container.name}")
            
            exit_code, output = container.exec_run(
                cmd='bash -c "dbt run --profiles-dir . --target prod"',
                workdir='/usr/app/physical_meteor'
            )
            
            print(output.decode('utf-8'))
            if exit_code != 0:
                raise Exception(f"dbt snowflake run failed with exit code {exit_code}")
                
        except docker.errors.NotFound:
            raise Exception(f"Container '{container_name}' not found. Is it running?")


    # ---------------- 3. Dynamic Propagation (Truth) ----------------
    
    # Physically call tasks to trigger execution and capture 'success signals'
    gfs_status = run_gfs_download()
    ecmwf_status = run_ecmwf_download()

    # Pass signals downstream to automatically form a 2x2 cross-dependency matrix.
    execute_dbt_run_pg(gfs_status, ecmwf_status)
    execute_dbt_run_sn(gfs_status, ecmwf_status)

# Physically instantiate the DAG
pipeline_instance = dbt_ecmwf_gfs_data_pipeline()