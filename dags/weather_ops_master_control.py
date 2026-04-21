import pendulum
# --- AIRFLOW 2.x COMPATIBLE IMPORTS ---
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException

# Define the logical hierarchy
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
        # In Airflow 2, it's safer to use pendulum.now() inside the task 
        # to ensure execution-time awareness.
        now = pendulum.now('UTC')
        print(f"Evaluating context for {now.to_datetime_string()}")
        
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
    trigger_ifs_spread = TriggerDagRunOperator(
        task_id='trigger_ifs_spread',
        trigger_dag_id='weather_ops.extract.ifs.spread',
        trigger_rule='all_success', 
    )

    # Define the Flow
    # evaluate_execution_context() returns an XComArg that we use to set dependencies
    context_check = evaluate_execution_context()
    
    # Airflow 2 Bitshift Orchestration
    context_check >> [trigger_aifs_upper, trigger_gfs] >> trigger_ifs_spread

# Instantiate
master_dag = master_control_pipeline()