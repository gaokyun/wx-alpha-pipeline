# This maintenance DAG cleans up old metadata from the Airflow database.
# It runs directly inside an isolated container to bypass Airflow 3 database constraints.

import os
import pendulum
from airflow.sdk import dag, task
# from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk.bases.hook import BaseHook

default_args = {
    'owner': 'admin',
    'retries': 1,
}

# Dynamically fetch the connection details at parse time
def get_dynamic_conn_uri():
    try:
        # Resolves the connection named 'postgres_default' or your custom conn_id
        conn = BaseHook.get_connection("postgres_default")
        return conn.get_uri()
    except Exception:
        # Fallback to a safe empty string or default if not found
        return ""

@dag(
    dag_id='ops.metadata_db_cleanup',
    schedule='0 22 * * *',
    # Setting the timezone to America/New_York ensures automatic EDT/EST compliance
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    # start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['ops', 'maintenance', 'cleanup']
)
def cleanup_dag():

    # This bypasses the worker isolation rules by spinning up a clean CLI execution thread
    purge_metadata = DockerOperator(
        task_id="purge_airflow_db_history",
        image="apache/airflow:3.2.1",  # Match your active Airflow image version
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,           # Fixes the "bind source path does not exist" Docker-in-Docker error
        # Override the entrypoint to a raw bash shell execution loop
        entrypoint=[r"/bin/bash", "-s"],
        
        # Pass the clean routine directly as the command argument
        command=r"""<<'EOF'
            THRESHOLD_DATE=$(date -u -d '2 days ago' +'%Y-%m-%d %H:%M:%S')
            
            echo "🧹 Executing clean routine for timestamps older than: ${THRESHOLD_DATE} UTC"
            
            airflow db clean --clean-before-timestamp "${THRESHOLD_DATE}" --yes
            EOF
            """,
        environment={
            # Seamlessly injects the URI fetched from the Airflow connection manager
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": get_dynamic_conn_uri()
        },
        network_mode="wx-alpha-pipeline_default", # Match your docker-compose network name
    )

    @task(task_id="precreate_columnar_partitions")
    def precreate_partitions():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pendulum

        db_hook = PostgresHook(
            postgres_conn_id='postgres_default',
            database='PHYSICAL_METEOR_DB'
        )
        
        parent_tables = {
            "fct_gfs_surface": [0, 6, 12, 18],
            "fct_gfs_upper": [0, 6, 12, 18],
            "fct_ifs_surface": [0, 12],
            "fct_ifs_upper": [0, 12],
            "fct_ifs_spread": [0, 12],
            "fct_aifs_surface": [0, 6, 12, 18],
            "fct_aifs_upper": [0, 6, 12, 18],
            "fct_aifs_spread": [0, 12]
        }
        
        # Pre-create partitions for today, tomorrow, and day-after-tomorrow
        for day_offset in [0, 1, 2]:
            target_day = pendulum.now('UTC').add(days=day_offset)
            date_str = target_day.format('YYYY_MM_DD')
            start_date = target_day.start_of('day').format('YYYY-MM-DD')
            end_date = target_day.start_of('day').add(days=1).format('YYYY-MM-DD')
            
            for parent, cycles in parent_tables.items():
                for cycle in cycles:
                    cycle_str = f"c{cycle:02d}"
                    part_name = f"gold.{parent}_{cycle_str}_p{date_str}"
                    
                    # 1. Ensure Level-1 cycle parent exists
                    level1_sql = f"""
                    CREATE TABLE IF NOT EXISTS gold.{parent}_{cycle_str}
                    PARTITION OF gold.{parent}
                    FOR VALUES IN ({cycle})
                    PARTITION BY RANGE (cycle_date);
                    """
                    db_hook.run(level1_sql, autocommit=True)
                    
                    # 2. Create Level-2 daily partition using Columnar access
                    level2_sql = f"""
                    CREATE TABLE IF NOT EXISTS {part_name}
                    PARTITION OF gold.{parent}_{cycle_str}
                    FOR VALUES FROM ('{start_date}') TO ('{end_date}')
                    USING columnar;
                    """
                    db_hook.run(level2_sql, autocommit=True)

    @task(task_id="purge_gold_fact_partitions")
    def purge_gold_partitions():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pendulum
        import re
        
        db_hook = PostgresHook(
            postgres_conn_id='postgres_default',
            database='PHYSICAL_METEOR_DB'
        )
        
        # Retain 45 days of data
        retention_days = 45
        cutoff_date = pendulum.now('UTC').subtract(days=retention_days)
        
        # Query all physical tables in 'gold' schema
        sql = """
            SELECT c.relname AS partition_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'gold' AND c.relkind IN ('r', 'p');
        """
        partitions = db_hook.get_records(sql)
        
        for row in partitions:
            partition_name = row[0]
            # Match Level-2 partitions (convention: _pYYYY_MM_DD)
            match = re.search(r'_p(\d{4})_(\d{2})_(\d{2})$', partition_name)
            if match:
                year, month, day = map(int, match.groups())
                try:
                    partition_date = pendulum.datetime(year, month, day, tz='UTC')
                    if partition_date < cutoff_date:
                        drop_sql = f"DROP TABLE IF EXISTS gold.{partition_name} CASCADE;"
                        print(f"Purging old partition: {drop_sql}")
                        db_hook.run(drop_sql, autocommit=True)
                except Exception as e:
                    print(f"Error parsing date from partition {partition_name}: {e}")

    precreate_partitions_task = precreate_partitions()
    purge_gold_partitions_task = purge_gold_partitions()
    
    purge_metadata >> precreate_partitions_task >> purge_gold_partitions_task

cleanup_dag = cleanup_dag()