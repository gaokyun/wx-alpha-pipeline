# This DAG is responsible for cleaning up old metadata from the Airflow database.
# It runs once a day and deletes Task Instances, DagRuns, XComs, and Logs that are older than 2 days.
# This helps keep the Airflow metadata database performant and prevents it from growing indefinitely.
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import pendulum

default_args = {
    'owner': 'admin',
    'retries': 1,
}

@dag(
    dag_id='ops.metadata_db_cleanup',
    schedule='@daily',  # Runs every night at midnight
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['ops', 'maintenance', 'cleanup']
)
def cleanup_dag():
    
    # We use BashOperator to call the CLI directly
    # This cleans Task Instances, DagRuns, XComs, and Logs in the DB
    purge_metadata = BashOperator(
        task_id='purge_airflow_db_history',
        bash_command="""
        airflow db clean --clean-before-timestamp "$(date -d '2 days ago' +'%Y-%m-%d %H:%M:%S')" --yes
        """
    )

    purge_metadata

cleanup_dag_instance = cleanup_dag()

# docker exec -it <container_id> airflow db clean \
#     --clean-before-timestamp "$(date -d '2 days ago' +'%Y-%m-%d %H:%M:%S')" \
#     --dry-run   actual deletion:--yes

# du -sh /opt/airflow/logs

# If it's more than 500MB, purge them manually to match your 2-day DB policy:

# Bash
# find /opt/airflow/logs -type f -mtime +2 -delete

# airflow dags list-import-errors # find silently errors that might be causing metadata bloat.
