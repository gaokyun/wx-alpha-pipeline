import os
import boto3
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path="/opt/airflow/physical_meteor/.env")
    oci_access_key = os.getenv('OCI_OBJECT_STORAGE_ACCESS_KEY')
    oci_secret_key = os.getenv('OCI_OBJECT_STORAGE_SECRET_KEY')
    oci_endpoint = os.getenv('OCI_API_ENDPOINT')
    oci_bucket = 'oci-s3-ykg-storage'
    
    s3_oci = boto3.client('s3', aws_access_key_id=oci_access_key, aws_secret_access_key=oci_secret_key, endpoint_url=oci_endpoint)
    
    # Check version-hint.text
    try:
        res = s3_oci.get_object(Bucket=oci_bucket, Key='weather_data/delta_lake/ecmwf_raw/at_aifs_upper/metadata/version-hint.text')
        ver = res['Body'].read().decode('utf-8').strip()
        print(f"version-hint.text content: {ver}")
    except Exception as e:
        print(f"Failed to read version-hint.text: {e}")

if __name__ == "__main__":
    main()
