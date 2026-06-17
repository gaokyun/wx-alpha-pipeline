import os
import oracledb
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv(dotenv_path="/opt/airflow/wx-alpha-pipeline/.env")
    if not os.getenv("ORACLE_USER"):
        load_dotenv(dotenv_path="/opt/airflow/.env")
        
    user = os.getenv("ORACLE_USER", "PHYSICAL_METEOR_RAW")
    password = os.getenv("ORACLE_PASSWORD")
    host = os.getenv("ORACLE_HOST", "adb.us-ashburn-1.oraclecloud.com")
    service = os.getenv("ORACLE_SERVICE", "g6fd1d6c71405c0_meteor0ykg0aidw_high.adb.oraclecloud.com")
    
    dsn = f"(description=(address=(protocol=tcps)(port=1522)(host={host}))(connect_data=(service_name={service}))(security=(ssl_server_dn_match=no)))"
    
    print(f"Connecting to ADW as {user}...")
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    
    # 1. AWS Table (Iceberg)
    aws_table = {
        "name": "ext_aws_at_aifs_upper",
        "credential": "AWS_STORAGE_CRED",
        "uri": "https://amzn-s3-ykg-storage.s3.amazonaws.com/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/metadata/latest.metadata.json",
        "format": '{"access_protocol":{"protocol_type":"iceberg"}}'
    }
    
    # 2. OCI Tables (Parquet)
    oci_tables = [
        {"name": "ext_gfs_upper", "path": "gfs_raw/gfs_upper"},
        {"name": "ext_gfs_surface", "path": "gfs_raw/gfs_surface"},
        {"name": "ext_at_aifs_upper", "path": "ecmwf_raw/at_aifs_upper"},
        {"name": "ext_at_aifs_surface", "path": "ecmwf_raw/at_aifs_surface"},
        {"name": "ext_aifs_spread", "path": "ecmwf_raw/aifs_spread"},
        {"name": "ext_at_ifs_upper", "path": "ecmwf_raw/at_ifs_upper"},
        {"name": "ext_at_ifs_surface", "path": "ecmwf_raw/at_ifs_surface"},
        {"name": "ext_ifs_spread", "path": "ecmwf_raw/ifs_spread"},
    ]
    
    # Recreate AWS table
    try:
        cursor.execute(f"DROP TABLE {aws_table['name']}")
        print(f"Dropped {aws_table['name']}")
    except Exception:
        pass
        
    print(f"Creating AWS external table {aws_table['name']}...")
    cursor.execute(f"""
        BEGIN
            DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
                table_name      => '{aws_table['name']}',
                credential_name => '{aws_table['credential']}',
                file_uri_list   => '{aws_table['uri']}',
                format          => '{aws_table['format']}'
            );
        END;
    """)
    print(f"Successfully created AWS external table {aws_table['name']}!")
    
    # Recreate OCI tables
    for tbl in oci_tables:
        try:
            cursor.execute(f"DROP TABLE {tbl['name']}")
            print(f"Dropped {tbl['name']}")
        except Exception:
            pass
            
        uri = f"https://objectstorage.us-ashburn-1.oraclecloud.com/n/idt2nq7cpbfu/b/oci-s3-ykg-storage/o/weather_data/delta_lake/{tbl['path']}/forecast_date=*/forecast_cycle=*/*.parquet"
        print(f"Creating OCI external table {tbl['name']}...")
        cursor.execute(f"""
            BEGIN
                DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
                    table_name      => '{tbl['name']}',
                    credential_name => 'OCI_STORAGE_CRED',
                    file_uri_list   => '{uri}',
                    format          => '{{"type":"parquet"}}'
                );
            END;
        """)
        print(f"Successfully created OCI external table {tbl['name']}!")
        
    conn.commit()
    cursor.close()
    conn.close()
    print("All external tables updated successfully!")

if __name__ == "__main__":
    main()
