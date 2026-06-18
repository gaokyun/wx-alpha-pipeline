import os
import boto3
import json
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path="/opt/airflow/physical_meteor/.env")
    oci_access_key = os.getenv('OCI_OBJECT_STORAGE_ACCESS_KEY')
    oci_secret_key = os.getenv('OCI_OBJECT_STORAGE_SECRET_KEY')
    oci_endpoint = os.getenv('OCI_API_ENDPOINT')
    oci_bucket = 'oci-s3-ykg-storage'
    
    s3_oci = boto3.client('s3', aws_access_key_id=oci_access_key, aws_secret_access_key=oci_secret_key, endpoint_url=oci_endpoint)
    
    try:
        res = s3_oci.get_object(Bucket=oci_bucket, Key='weather_data/delta_lake/ecmwf_raw/at_aifs_upper/metadata/v1.metadata.json')
        data = json.loads(res['Body'].read().decode('utf-8'))
        print("v1.metadata.json content (keys):", list(data.keys()))
        print("Schema in metadata:")
        print(json.dumps(data.get('schemas', data.get('schema', {})), indent=2)[:1000])
    except Exception as e:
        print(f"Failed to read metadata: {e}")

if __name__ == "__main__":
    main()
