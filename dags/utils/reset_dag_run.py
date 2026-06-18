from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState
from airflow.utils.session import create_session

dag_id = 'weather_ops.standardized_master_control'
run_id = 'scheduled__2026-06-07T08:00:00+00:00'

if __name__ == "__main__":
    with create_session() as session:
        dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id).first()
        if dag_run:
            print(f"Found DAG run: {dag_run.run_id} in state {dag_run.state}")
            dag_run.state = DagRunState.QUEUED
            
            # Reset the MySQL task and the downstream verification task
            tis = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id, 
                TaskInstance.run_id == run_id,
                TaskInstance.task_id.in_([
                    'unified_refreshes.refresh_gold_unified_mysql',
                    'verify_postgres_gold_marts'
                ])
            ).all()
            for ti in tis:
                print(f"Resetting task: {ti.task_id} from state {ti.state}")
                ti.state = None
            
            session.commit()
            print("Successfully reset DAG run and tasks!")
        else:
            print("DAG run not found!")
