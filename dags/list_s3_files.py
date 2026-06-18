import os
import boto3
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path="/opt/airflow/physical_meteor/.env")
    
    # AWS S3
    aws_access_key = os.getenv('AWS_ACC_KEY')
    aws_secret_key = os.getenv('AWS_SECRET_KEY')
    aws_bucket = 'amzn-s3-ykg-storage'
    
    # OCI S3
    oci_access_key = os.getenv('OCI_OBJECT_STORAGE_ACCESS_KEY')
    oci_secret_key = os.getenv('OCI_OBJECT_STORAGE_SECRET_KEY')
    oci_endpoint = os.getenv('OCI_API_ENDPOINT')
    oci_bucket = 'oci-s3-ykg-storage'
    
    print("--- Checking AWS S3 ---")
    s3_aws = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    try:
        res = s3_aws.list_objects_v2(Bucket=aws_bucket, Prefix='weather_data/delta_lake/ecmwf_raw/at_aifs_upper/metadata/', MaxKeys=5)
        print("AWS at_aifs_upper metadata:")
        for obj in res.get('Contents', []):
            print(f"  - {obj['Key']}")
    except Exception as e:
        print(f"Failed to list AWS: {e}")
        
    print("\n--- Checking OCI Object Storage ---")
    s3_oci = boto3.client('s3', aws_access_key_id=oci_access_key, aws_secret_access_key=oci_secret_key, endpoint_url=oci_endpoint)
    prefixes = [
        'weather_data/delta_lake/gfs_raw/gfs_upper/metadata/',
        'weather_data/delta_lake/gfs_raw/gfs_surface/metadata/',
        'weather_data/delta_lake/ecmwf_raw/at_aifs_upper/metadata/',
        'weather_data/delta_lake/ecmwf_raw/at_aifs_surface/metadata/',
        'weather_data/delta_lake/ecmwf_raw/aifs_spread/metadata/',
        'weather_data/delta_lake/ecmwf_raw/at_ifs_upper/metadata/',
        'weather_data/delta_lake/ecmwf_raw/at_ifs_surface/metadata/',
        'weather_data/delta_lake/ecmwf_raw/ifs_spread/metadata/',
    ]
    for pref in prefixes:
        try:
            res = s3_oci.list_objects_v2(Bucket=oci_bucket, Prefix=pref, MaxKeys=3)
            print(f"OCI {pref}:")
            contents = res.get('Contents', [])
            if not contents:
                print("  ❌ No Iceberg metadata found")
            for obj in contents:
                print(f"  - {obj['Key']}")
        except Exception as e:
            print(f"Failed to list OCI {pref}: {e}")

if __name__ == "__main__":
    main()
