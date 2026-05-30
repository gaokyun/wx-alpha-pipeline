import os
import boto3
from dotenv import load_dotenv

def main():
    # Load environment variables if available
    load_dotenv()
    
    # Credentials from config or environment fallback
    access_key = os.getenv('OCI_OBJECT_STORAGE_ACCESS_KEY', '***REMOVED***')
    secret_key = os.getenv('OCI_OBJECT_STORAGE_SECRET_KEY', '***REMOVED***')
    endpoint_url = os.getenv('OCI_API_ENDPOINT', 'https://idt2nq7cpbfu.compat.objectstorage.us-ashburn-1.oraclecloud.com')
    bucket_name = 'oci-s3-ykg-storage'
    prefix = 'weather_data/delta_lake/ecmwf_raw/at_aifs_upper/'
    
    print("====================================================================")
    # Check Delta Lake log directory
    print(f"Connecting to OCI Object Storage endpoint: {endpoint_url}")
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url
    )
    
    # 1. Check Delta Log files
    delta_log_prefix = f"{prefix}_delta_log/"
    print(f"\n1. Scanning Delta log files under: s3://{bucket_name}/{delta_log_prefix}")
    try:
        delta_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=delta_log_prefix, MaxKeys=10)
        contents = delta_response.get('Contents', [])
        if contents:
            print(f"Found {len(contents)} Delta log objects (showing up to 5):")
            for obj in contents[:5]:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("  ❌ No Delta logs found! Check if the delta table path is correct.")
    except Exception as e:
        print(f"  ❌ Error scanning Delta logs: {e}")
        
    # 2. Check Iceberg Metadata files
    iceberg_prefix = f"{prefix}metadata/"
    print(f"\n2. Scanning Iceberg metadata files under: s3://{bucket_name}/{iceberg_prefix}")
    try:
        iceberg_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=iceberg_prefix)
        contents = iceberg_response.get('Contents', [])
        if contents:
            print(f"✅ Success! Found {len(contents)} Iceberg metadata objects:")
            for obj in contents:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("  ❌ No Iceberg metadata files found. Run the XTable container to sync the metadata.")
    except Exception as e:
        print(f"  ❌ Error scanning Iceberg metadata: {e}")
    print("====================================================================")

if __name__ == '__main__':
    main()
