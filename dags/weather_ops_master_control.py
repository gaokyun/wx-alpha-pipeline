import pendulum
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException

# Define the logical hierarchy we established
MODELS = ['aifs', 'ifs', 'gfs']
EXTRACT_TYPES = ['upper', 'surface', 'spread']

default_args = {
    'owner': 'meteorologist',
    'start_date': pendulum.datetime(2026, 1, 1, tz="UTC"),
}

@dag(
    dag_id='weather_ops.master_control',
    default_args=default_args,
    schedule='0 0,6,12,18 * * *',  # Runs at the start of every major cycle
    catchup=False,
    tags=['control', 'weather_ops']
)
def master_control_pipeline():

    @task(task_id='gatekeeper_logic')
    def evaluate_execution_context():
        """
        Example Central Control Logic:
        Only proceed if it's a weekday or if a specific high-priority flag is set.
        """
        now = pendulum.now('UTC')
        print(f"Evaluating context for {now.to_datetime_string()}")
        
        # Logic: Skip Sunday runs if needed, or check an external API status
        if now.day_of_week == pendulum.SUNDAY:
            print("Sunday: Minimum maintenance mode triggered.")
            return "MINIMAL"
        return "FULL_RUN"

    # 1. Trigger AIFS Upper (High Priority)
    trigger_aifs_upper = TriggerDagRunOperator(
        task_id='trigger_aifs_upper',
        trigger_dag_id='weather_ops.extract.aifs.upper',
        wait_for_completion=False, # Fire and forget
        reset_dag_run=True,
    )

    # 2. Trigger GFS (Standard Priority)
    trigger_gfs = TriggerDagRunOperator(
        task_id='trigger_gfs_upper',
        trigger_dag_id='weather_ops.extract.gfs.upper',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # 3. Conditional Logic for "Spread" Data
    # In a real scenario, you might only want to trigger spreads if the 
    # 'gatekeeper_logic' returns 'FULL_RUN'
    trigger_ifs_spread = TriggerDagRunOperator(
        task_id='trigger_ifs_spread',
        trigger_dag_id='weather_ops.extract.ifs.spread',
        # This operator will only run if the gatekeeper allows it
        trigger_rule='all_success', 
    )

    # Define the Flow
    context = evaluate_execution_context()
    
    # Simple Orchestration Flow
    # We trigger the core models first, then conditional components
    context >> [trigger_aifs_upper, trigger_gfs] >> trigger_ifs_spread

# Instantiate the Super DAG
master_dag = master_control_pipeline()