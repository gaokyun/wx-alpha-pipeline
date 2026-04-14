import os
import pendulum
# --- AIRFLOW 2.x COMPATIBLE IMPORTS ---
from airflow.decorators import dag, task
from deltalake import DeltaTable

# --- OCI CONFIGURATION (Pivoted from AWS) ---
OCI_BUCKET = os.getenv('OCI_OBJECT_STORAGE_BUCKET', 'oci-s3-ykg-storage')
OCI_ACC_KEY = os.getenv('OCI_OBJECT_STORAGE_ACCESS_KEY')
OCI_SECRET_KEY = os.getenv('OCI_OBJECT_STORAGE_SECRET_KEY')
OCI_REGION = os.getenv('OCI_REGION', 'us-ashburn-1')
OCI_ENDPOINT = os.getenv('OCI_ENDPOINT_URL')

# Mapping OCI to S3-compatible storage options
# Note: deltalake (via object_store) uses AWS keys for all S3-compatible storage
storage_options = {
    "AWS_ACCESS_KEY_ID": OCI_ACC_KEY,
    "AWS_SECRET_ACCESS_KEY": OCI_SECRET_KEY,
    "AWS_REGION": OCI_REGION,
    "AWS_ENDPOINT_URL": OCI_ENDPOINT,
    "AWS_S3_ADDRESSING_STYLE": "path"
}

# The tables you want to maintain in OCI
DELTA_TABLES = [
    f"s3://{OCI_BUCKET}/weather_data/delta_lake/gfs_raw/",
    f"s3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_upper/",
    f"s3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_ifs_surface/",
    f"s3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/ifs_spread/",
    f"s3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/",
    f"s3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/at_aifs_surface/",
    f"s3://{OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/aifs_spread/"
]

@dag(
    dag_id='maintenance_delta_vacuum_oci',
    schedule='@daily', # Run once a day at midnight
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=['maintenance', 'oci', 'delta-lake', 'cost-control']
)
def delta_maintenance_pipeline():

    @task(task_id='enforce_3_day_retention')
    def clean_delta_tables():
        # Calculate 3 days ago
        cutoff_date = pendulum.now('UTC').subtract(days=3)
        
        # Use the YYYY-MM-DD format to match your `forecast_date` partition column
        cutoff_string = cutoff_date.format('YYYY-MM-DD')
        
        # Optimized Predicate: Drop the whole partition!
        predicate = f"`forecast_date` < '{cutoff_string}'"
        
        for table_path in DELTA_TABLES:
            print(f"--- Starting OCI maintenance for: {table_path} ---")
            
            try:
                dt = DeltaTable(table_path, storage_options=storage_options)
                
                # 1. Logical Delete (Updates the Delta Log)
                print(f"  -> Executing logical delete for OCI partition: {predicate}")
                dt.delete(predicate)
                
                # 2. Physical Delete (VACUUM)
                print("  -> Vacuuming physical OCI objects...")

                # Explicitly turn off dry_run to actually delete the files
                # Note: retention_hours=24 means we delete anything logically removed 
                # for more than 24 hours.
                deleted_files = dt.vacuum(
                    retention_hours=24, 
                    enforce_retention_duration=False,
                    dry_run=False
                )                                                 
                                                                  
                num_deleted = len(deleted_files)
                
                if num_deleted > 0:
                    print(f"  -> SUCCESS: Physically deleted {num_deleted} files from OCI.")
                    for f in deleted_files[:3]:
                        print(f"     Example Deleted: {f}")
                else:
                    print("  -> No physical files required cleanup in OCI.")
                    
                print(f"--- Maintenance complete for {table_path} ---\n")
                
            except Exception as e:
                print(f"  -> ERROR: Failed to clean OCI table {table_path}: {str(e)}")

    clean_delta_tables()

# Instantiate the DAG
maintenance_dag = delta_maintenance_pipeline()