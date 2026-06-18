import os
import oracledb
from dotenv import load_dotenv

def main():
    # In Airflow environment, .env variables might already be loaded or we can load them from /opt/airflow/wx-alpha-pipeline/.env
    # But let's first load them explicitly
    load_dotenv(dotenv_path="/opt/airflow/wx-alpha-pipeline/.env")
    if not os.getenv("ORACLE_USER"):
        load_dotenv(dotenv_path="/opt/airflow/.env")
    
    user = os.getenv("ORACLE_USER", "PHYSICAL_METEOR_RAW")
    password = os.getenv("ORACLE_PASSWORD")
    admin_password = os.getenv("ORACLE_ADMIN_PASSWORD")
    if not password:
        raise ValueError("ORACLE_PASSWORD environment variable is not set")
    host = os.getenv("ORACLE_HOST", "adb.us-ashburn-1.oraclecloud.com")
    port = int(os.getenv("ORACLE_PORT", "1521"))
    service = os.getenv("ORACLE_SERVICE", "g6fd1d6c71405c0_meteor0ykg0aidw_high.adb.oraclecloud.com")
    
    print(f"Connecting to Oracle ADW: {user}@{host}:{port}/{service}")
    
    try:
        # Try different credentials and DSNs
        cred_dsns = []
        if admin_password:
            cred_dsns.append(("admin", admin_password))
        cred_dsns.append(("admin", password))
        cred_dsns.append(("PHYSICAL_METEOR_RAW", password))
        if admin_password:
            cred_dsns.append(("PHYSICAL_METEOR_RAW", admin_password))
        
        connection = None
        for u, p in cred_dsns:
            dsn = f"(description=(address=(protocol=tcps)(port=1522)(host={host}))(connect_data=(service_name={service}))(security=(ssl_server_dn_match=no)))"
            try:
                print(f"Trying user={u} with password={p[:4]}...")
                connection = oracledb.connect(
                    user=u,
                    password=p,
                    dsn=dsn
                )
                print(f"SUCCESS: Connected as {u}!")
                user = u
                password = p
                break
            except Exception as dsn_e:
                print(f"Failed for user={u}: {dsn_e}")
                
        if not connection:
            raise Exception("All credential and connection attempts failed")


        print("Unlocking PHYSICAL_METEOR_RAW...")
        cursor = connection.cursor()
        try:
            cursor.execute(f'ALTER USER PHYSICAL_METEOR_RAW IDENTIFIED BY "{password}" ACCOUNT UNLOCK')
            print("Successfully unlocked PHYSICAL_METEOR_RAW!")
        except Exception as ue:
            print(f"Failed to unlock: {ue}")
            
        # Re-check credentials
        try:
            cursor.execute("SELECT credential_name, username FROM user_credentials")
            creds = cursor.fetchall()
            print(f"Admin Existing credentials: {creds}")
        except Exception as ce:
            print(f"Could not query user_credentials: {ce}")
            
        # Test connecting as PHYSICAL_METEOR_RAW
        print("Testing connection as PHYSICAL_METEOR_RAW...")
        try:
            conn_raw = oracledb.connect(
                user="PHYSICAL_METEOR_RAW",
                password=password,
                dsn=f"(description=(address=(protocol=tcps)(port=1522)(host={host}))(connect_data=(service_name={service}))(security=(ssl_server_dn_match=no)))"
            )
            print("Successfully connected as PHYSICAL_METEOR_RAW!")
            c_raw = conn_raw.cursor()
            c_raw.execute("SELECT sysdate, user FROM dual")
            print(f"Query as PHYSICAL_METEOR_RAW: {c_raw.fetchone()}")
            # Query columns of METEOR_GFS_UPPER
            try:
                c_raw.execute("SELECT column_name, data_type FROM user_tab_cols WHERE table_name = 'METEOR_GFS_UPPER'")
                cols = c_raw.fetchall()
                print(f"Columns of METEOR_GFS_UPPER: {cols}")
            except Exception as col_e:
                print(f"Failed to query columns: {col_e}")

            # Drop and create OCI credential with native Auth Token
            try:
                try:
                    c_raw.execute("BEGIN DBMS_CLOUD.DROP_CREDENTIAL('OCI_STORAGE_CRED'); END;")
                    print("Dropped existing OCI_STORAGE_CRED")
                except Exception:
                    pass
                oci_auth_token = os.getenv("OCI_AUTH_TOKEN")
                if not oci_auth_token:
                    raise ValueError("OCI_AUTH_TOKEN environment variable is not set")
                c_raw.execute(f"""
                    BEGIN
                        DBMS_CLOUD.CREATE_CREDENTIAL(
                            credential_name => 'OCI_STORAGE_CRED',
                            username        => 'gaokyun@gmail.com',
                            password        => '{oci_auth_token}'
                        );
                    END;
                """)
                print("Successfully created native OCI_STORAGE_CRED!")
            except Exception as e:
                print(f"Failed to create OCI_STORAGE_CRED: {e}")

            # Test LIST_OBJECTS with OCI native URL
            try:
                print("Testing LIST_OBJECTS on OCI native URL...")
                c_raw.execute("""
                    SELECT object_name FROM table(
                        DBMS_CLOUD.LIST_OBJECTS(
                            credential_name => 'OCI_STORAGE_CRED',
                            location_uri    => 'https://objectstorage.us-ashburn-1.oraclecloud.com/n/idt2nq7cpbfu/b/oci-s3-ykg-storage/o/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/metadata/'
                        )
                    )
                """)
                objs = c_raw.fetchall()
                print(f"LIST_OBJECTS result ({len(objs)} objects):")
                for obj in objs[:3]:
                    print(f"  - {obj[0]}")
            except Exception as le:
                print(f"LIST_OBJECTS failed: {le}")

            # Drop and create AWS credential
            try:
                try:
                    c_raw.execute("BEGIN DBMS_CLOUD.DROP_CREDENTIAL('AWS_STORAGE_CRED'); END;")
                    print("Dropped existing AWS_STORAGE_CRED")
                except Exception:
                    pass
                aws_acc_key = os.getenv("AWS_ACC_KEY")
                aws_secret_key = os.getenv("AWS_SECRET_KEY")
                if not aws_acc_key or not aws_secret_key:
                    raise ValueError("AWS credentials environment variables are not set")
                c_raw.execute(f"""
                    BEGIN
                        DBMS_CLOUD.CREATE_CREDENTIAL(
                            credential_name => 'AWS_STORAGE_CRED',
                            username        => '{aws_acc_key}',
                            password        => '{aws_secret_key}'
                        );
                    END;
                """)
                print("Successfully created AWS_STORAGE_CRED!")
            except Exception as e:
                print(f"Failed to create AWS_STORAGE_CRED: {e}")

            # Test LIST_OBJECTS with AWS S3 URL
            try:
                print("Testing LIST_OBJECTS on AWS S3 URL...")
                c_raw.execute("""
                    SELECT object_name FROM table(
                        DBMS_CLOUD.LIST_OBJECTS(
                            credential_name => 'AWS_STORAGE_CRED',
                            location_uri    => 'https://amzn-s3-ykg-storage.s3.amazonaws.com/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/metadata/'
                        )
                    )
                """)
                objs = c_raw.fetchall()
                print(f"AWS LIST_OBJECTS result ({len(objs)} objects):")
                for obj in objs[:3]:
                    print(f"  - {obj[0]}")
            except Exception as le:
                print(f"AWS LIST_OBJECTS failed: {le}")

            # Query raw credentials
            try:
                c_raw.execute("SELECT credential_name, username FROM user_credentials")
                creds_raw = c_raw.fetchall()
                print(f"PHYSICAL_METEOR_RAW credentials: {creds_raw}")
            except Exception as ce_raw:
                print(f"Could not query user_credentials for PHYSICAL_METEOR_RAW: {ce_raw}")
                
            # Drop and create AWS external table using Iceberg format
            try:
                try:
                    c_raw.execute("DROP TABLE ext_at_aifs_upper")
                    print("Dropped existing ext_at_aifs_upper")
                except Exception:
                    pass
                
                # Note: Direct Parquet external table creation on OCI native HTTPS URL
                c_raw.execute("""
                    BEGIN
                        DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
                            table_name      => 'ext_at_aifs_upper',
                            credential_name => 'OCI_STORAGE_CRED',
                            file_uri_list   => 'https://objectstorage.us-ashburn-1.oraclecloud.com/n/idt2nq7cpbfu/b/oci-s3-ykg-storage/o/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/forecast_date=*/forecast_cycle=*/*.parquet',
                            format          => '{"type":"parquet"}'
                        );
                    END;
                """)
                print("Successfully created direct Parquet external table ext_at_aifs_upper!")
                
                # Test query external table
                c_raw.execute("SELECT count(*) FROM ext_at_aifs_upper")
                cnt = c_raw.fetchone()[0]
                print(f"Count of rows in ext_at_aifs_upper: {cnt}")
                
                # Query column names
                c_raw.execute("SELECT column_name, data_type FROM user_tab_cols WHERE table_name = 'EXT_AT_AIFS_UPPER'")
                cols = c_raw.fetchall()
                print(f"Columns of EXT_AT_AIFS_UPPER: {cols}")
                
            except Exception as e:
                print(f"Failed to create/query external table: {e}")

        except Exception as re:
            print(f"Failed to connect as PHYSICAL_METEOR_RAW: {re}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

