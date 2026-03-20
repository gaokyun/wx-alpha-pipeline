import os
import pendulum
from airflow.sdk import dag, task
from deltalake import DeltaTable

# Re-use your central config
S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'amzn-s3-ykg-storage')
AWS_ACC_KEY = os.getenv('AWS_ACC_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = os.getenv('AWS_REGION')

storage_options = {
    "AWS_ACCESS_KEY_ID": AWS_ACC_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_REGION": AWS_REGION
}

# The tables you want to maintain
DELTA_TABLES = [
    f"s3://{S3_BUCKET}/weather_data/delta_lake/gfs_raw/",
    f"s3://{S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_upper/",
    f"s3://{S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_surface/"
]

@dag(
    dag_id='maintenance_delta_vacuum',
    schedule='@daily', # Run once a day at midnight
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['maintenance', 'delta-lake', 'cost-control']
)
def delta_maintenance_pipeline():

    @task(task_id='enforce_10_day_retention')
    def clean_delta_tables():
        # Calculate exactly 10 days ago
        cutoff_date = pendulum.now('UTC').subtract(days=10)
        
        # Format explicitly for Delta Lake SQL evaluation
        cutoff_string = cutoff_date.format('YYYY-MM-DD HH:mm:ss')
        
        # Use the `time` column to drop old model runs
        predicate = f"`time` < '{cutoff_string}'"
        
        for table_path in DELTA_TABLES:
            print(f"Starting maintenance for: {table_path}")
            
            try:
                dt = DeltaTable(table_path, storage_options=storage_options)
                
                # 1. Logical Delete
                print(f"  -> Deleting records older than 10 days: {predicate}")
                dt.delete(predicate)
                
                # 2. Physical Delete (VACUUM)
                print("  -> Vacuuming physical S3 objects...")
                dt.vacuum(retention_hours=0, enforce_retention_duration=False)
                
                print("  -> Maintenance complete.")
                
            except Exception as e:
                print(f"Failed to clean {table_path}: {e}")

    clean_delta_tables()

maintenance_dag = delta_maintenance_pipeline()