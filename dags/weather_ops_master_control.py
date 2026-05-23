import pendulum

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'meteorologist',
    'start_date': pendulum.datetime(2026, 1, 1, tz="UTC"),
    'retries': 0,
    'depends_on_past': False,
}

@dag(
    dag_id='weather_ops.master_control',
    default_args=default_args,
    schedule='0 0,6,12,18 * * *',
    catchup=False,
    tags=['control', 'weather_ops']
)
def master_control_pipeline():

    @task(task_id='gatekeeper_logic')
    def evaluate_execution_context():
        """
        Central control logic that decides whether to proceed with the pipeline.
        """
        now = pendulum.now('UTC')
        print(f"Evaluating context for {now.to_datetime_string()}")

        if now.day_of_week == pendulum.SUNDAY:
            # Sunday runs return "MINIMAL" context but are explicitly NOT skipped.
            # Downstream DAG runs are triggered normally.
            print("Sunday: Minimum maintenance mode triggered (not skipped).")
            return "MINIMAL"

        return "FULL_RUN"

    trigger_aifs_upper = TriggerDagRunOperator(
        task_id='trigger_aifs_upper',
        trigger_dag_id='weather_ops.extract.aifs.upper',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    trigger_gfs = TriggerDagRunOperator(
        task_id='trigger_gfs_upper',
        trigger_dag_id='weather_ops.extract.gfs.upper',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    trigger_ifs_spread = TriggerDagRunOperator(
        task_id='trigger_ifs_spread',
        trigger_dag_id='weather_ops.extract.ifs.spread',
        trigger_rule='all_success',
    )

    context_check = evaluate_execution_context()
    context_check >> [trigger_aifs_upper, trigger_gfs] >> trigger_ifs_spread

master_dag = master_control_pipeline()