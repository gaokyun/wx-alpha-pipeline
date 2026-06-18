import os
import duckdb
import pyarrow as pa
import pendulum
from dotenv import load_dotenv

def main():
    # Load env vars
    load_dotenv(dotenv_path='/opt/airflow/physical_meteor/.env')
    
    schema = pa.schema([
        ('valid_time', pa.timestamp('us', tz='UTC')),
        ('isobaricInhPa', pa.int32()),
        ('latitude', pa.float64()),
        ('longitude', pa.float64()),
        ('t', pa.float32()),
        ('lat_i', pa.int32()),
        ('lon_i', pa.int32()),
        ('forecast_reference_time', pa.timestamp('us', tz='UTC')),
        ('step_hours', pa.int32()),
        ('gh', pa.float32())
    ])

    now_ts = pendulum.now('UTC')
    data = {
        'valid_time': [now_ts],
        'isobaricInhPa': [500],
        'latitude': [0.0],
        'longitude': [0.0],
        't': [0.0],
        'lat_i': [0],
        'lon_i': [0],
        'forecast_reference_time': [now_ts],
        'step_hours': [0],
        'gh': [0.0]
    }
    table = pa.Table.from_pydict(data, schema=schema)

    con = duckdb.connect(database=':memory:')
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute("SET s3_region='us-ashburn-1';")
    
    access_key = os.getenv('OCI_OBJECT_STORAGE_ACCESS_KEY', '').strip('"')
    secret_key = os.getenv('OCI_OBJECT_STORAGE_SECRET_KEY', '').strip('"')
    endpoint = os.getenv('OCI_API_ENDPOINT', '').replace('https://', '').replace('http://', '')
    
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=true;")

    con.register('dummy_table', table)

    target_path = 's3://oci-s3-ykg-storage/weather_data/delta_lake/ecmwf_raw/aifs_spread/forecast_date=2026-05-24/forecast_cycle=0/dummy.parquet'
    con.execute(f"COPY (SELECT * FROM dummy_table) TO '{target_path}' (FORMAT PARQUET)")
    print('Successfully wrote dummy parquet using DuckDB httpfs!')

if __name__ == '__main__':
    main()
