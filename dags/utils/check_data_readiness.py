import os
import boto3
import pendulum

def check_prior_day_data_readiness(**context):
    logical_date = context.get('logical_date') or context.get('execution_date')
    if not logical_date:
        # Fallback to now - 1 day if run outside of Airflow context
        local_date = pendulum.now('America/New_York')
    else:
        local_date = pendulum.instance(logical_date).in_timezone('America/New_York')
        
    yesterday = local_date.subtract(days=1).format('YYYY-MM-DD')
    
    oci_access_key = os.getenv('OCI_OBJECT_STORAGE_ACCESS_KEY') or os.getenv('OCI_ACCESS_KEY')
    oci_secret_key = os.getenv('OCI_OBJECT_STORAGE_SECRET_KEY') or os.getenv('OCI_SECRET_KEY')
    oci_endpoint = os.getenv('OCI_API_ENDPOINT') or 'https://idt2nq7cpbfu.compat.objectstorage.us-ashburn-1.oraclecloud.com'
    oci_bucket = os.getenv('OCI_OBJECT_STORAGE_BUCKET') or 'oci-s3-ykg-storage'
    
    if not oci_access_key or not oci_secret_key:
        print("Missing OCI credentials. Cannot check data readiness.")
        return False
        
    s3_oci = boto3.client(
        's3',
        aws_access_key_id=oci_access_key,
        aws_secret_access_key=oci_secret_key,
        endpoint_url=oci_endpoint
    )
    
    paths = [
        "gfs_raw/gfs_upper",
        "gfs_raw/gfs_surface",
        "ecmwf_raw/at_aifs_upper",
        "ecmwf_raw/at_aifs_surface",
        "ecmwf_raw/at_ifs_upper",
        "ecmwf_raw/at_ifs_surface",
        "ecmwf_raw/ifs_spread"
    ]
    
    missing = []
    print(f"Verifying OCI data readiness for date: {yesterday}")
    for path in paths:
        # Spread data and IFS models only exist for 00z and 12z cycles (cycles 0 and 12)
        target_cycles = [0, 12] if ("spread" in path or ("ifs" in path and "aifs" not in path)) else [0, 6, 12, 18]
        for cycle in target_cycles:
            prefix = f"weather_data/delta_lake/{path}/forecast_date={yesterday}/forecast_cycle={cycle}/"
            try:
                res = s3_oci.list_objects_v2(Bucket=oci_bucket, Prefix=prefix, MaxKeys=1)
                if 'Contents' not in res or len(res['Contents']) == 0:
                    missing.append(f"{path} (cycle {cycle})")
            except Exception as e:
                print(f"Error checking prefix {prefix}: {e}")
                return False
                
    if missing:
        print(f"Pending prior day's data ({yesterday}). Missing {len(missing)} partition(s):")
        for m in missing[:10]:
            print(f"  - {m}")
        if len(missing) > 10:
            print(f"  - and {len(missing) - 10} more...")
        return False
        
    print(f"All 4 cycles for all models for prior day ({yesterday}) exist in OCI S3 bucket! Proceeding with transform.")
    return True
