import os
import oracledb
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path="/opt/airflow/wx-alpha-pipeline/.env")
    if not os.getenv("ORACLE_USER"):
        load_dotenv(dotenv_path="/opt/airflow/.env")
        
    host = os.getenv("ORACLE_HOST", "adb.us-ashburn-1.oraclecloud.com")
    service = os.getenv("ORACLE_SERVICE", "g6fd1d6c71405c0_meteor0ykg0aidw_high.adb.oraclecloud.com")
    dsn = f"(description=(address=(protocol=tcps)(port=1522)(host={host}))(connect_data=(service_name={service}))(security=(ssl_server_dn_match=no)))"
    
    admin_password = os.getenv("ORACLE_ADMIN_PASSWORD")
    oracle_password = os.getenv("ORACLE_PASSWORD")
    if not oracle_password:
        raise ValueError("ORACLE_PASSWORD environment variable is not set")
    
    # Connection passwords to try for ADMIN
    passwords = []
    if admin_password:
        passwords.append(admin_password)
    passwords.append(oracle_password)
    
    connection = None
    for pwd in passwords:
        try:
            print(f"Connecting to ADW as ADMIN...")
            connection = oracledb.connect(user="admin", password=pwd, dsn=dsn)
            print("Successfully connected as admin!")
            break
        except Exception as e:
            print(f"Admin connection failed: {e}")
            
    if not connection:
        raise Exception("Failed to connect to ADW as ADMIN.")
        
    cursor = connection.cursor()
    
    # 1. Create target schemas
    schemas = ["PHYSICAL_METEOR_STG", "PHYSICAL_METEOR_GOLD"]
    for schema in schemas:
        try:
            print(f"Creating user/schema {schema}...")
            cursor.execute(f"CREATE USER {schema} IDENTIFIED BY \"{oracle_password}\"")
        except oracledb.DatabaseError as e:
            error_obj, = e.args
            if error_obj.code == 1920: # User already exists
                print(f"Schema {schema} already exists.")
            else:
                raise
                
        # Grant basic quotas & capabilities
        cursor.execute(f"GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SYNONYM TO {schema}")
        cursor.execute(f"GRANT SELECT ANY TABLE TO {schema}")
        cursor.execute(f"ALTER USER {schema} QUOTA UNLIMITED ON DATA")
        try:
            cursor.execute(f"GRANT READ, WRITE ON DIRECTORY DATA_PUMP_DIR TO {schema}")
            print(f"Granted READ, WRITE ON DIRECTORY DATA_PUMP_DIR TO {schema}")
        except Exception as e:
            print(f"Warning: Failed to grant directory privileges to {schema}: {e}")
        
    # 2. Grant system permissions to runner (PHYSICAL_METEOR_RAW)
    print("Granting system privileges to PHYSICAL_METEOR_RAW...")
    privileges = [
        "CREATE ANY TABLE", "CREATE ANY VIEW", "DROP ANY TABLE", "DROP ANY VIEW",
        "ALTER ANY TABLE", "SELECT ANY TABLE", "INSERT ANY TABLE", "UPDATE ANY TABLE", "DELETE ANY TABLE"
    ]
    for priv in privileges:
        cursor.execute(f"GRANT {priv} TO PHYSICAL_METEOR_RAW")
        
    try:
        cursor.execute("GRANT READ, WRITE ON DIRECTORY DATA_PUMP_DIR TO PHYSICAL_METEOR_RAW")
        print("Granted READ, WRITE ON DIRECTORY DATA_PUMP_DIR TO PHYSICAL_METEOR_RAW")
    except Exception as e:
        print(f"Warning: Failed to grant directory privileges to PHYSICAL_METEOR_RAW: {e}")
        
    connection.commit()
    cursor.close()
    connection.close()
    print("ADW Multi-schema bootstrapping complete!")

if __name__ == "__main__":
    main()
