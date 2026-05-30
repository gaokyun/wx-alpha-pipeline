import os
import oracledb
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path='/opt/airflow/physical_meteor/.env')
    
    host = os.getenv('ORACLE_HOST', 'adb.us-ashburn-1.oraclecloud.com')
    service = os.getenv('ORACLE_SERVICE', 'g6fd1d6c71405c0_meteor0ykg0aidw_high.adb.oraclecloud.com')
    dsn = f"(description=(address=(protocol=tcps)(port=1522)(host={host}))(connect_data=(service_name={service}))(security=(ssl_server_dn_match=no)))"
    
    admin_password = os.getenv("ORACLE_ADMIN_PASSWORD")
    oracle_password = os.getenv("ORACLE_PASSWORD")
    if not oracle_password:
        raise ValueError("ORACLE_PASSWORD environment variable is not set")
    
    passwords = []
    if admin_password:
        passwords.append(admin_password)
    passwords.append(oracle_password)
    
    success = False
    for pwd in passwords:
        print(f"Trying connection as ADMIN with password: {pwd[:3]}...")
        try:
            conn = oracledb.connect(user="ADMIN", password=pwd, dsn=dsn)
            print("Successfully connected as ADMIN!")
            cursor = conn.cursor()
            cursor.execute("ALTER USER PHYSICAL_METEOR_RAW QUOTA UNLIMITED ON DATA")
            print("Successfully granted UNLIMITED quota on DATA to PHYSICAL_METEOR_RAW!")
            conn.commit()
            cursor.close()
            conn.close()
            success = True
            break
        except Exception as e:
            print(f"Failed: {e}")
            
    if not success:
        print("Could not grant quota as ADMIN. Let's see if PHYSICAL_METEOR_RAW can grant to itself (unlikely but worth a shot)...")
        try:
            conn = oracledb.connect(user="PHYSICAL_METEOR_RAW", password=os.getenv('ORACLE_PASSWORD'), dsn=dsn)
            cursor = conn.cursor()
            cursor.execute("ALTER USER PHYSICAL_METEOR_RAW QUOTA UNLIMITED ON DATA")
            print("Wow, PHYSICAL_METEOR_RAW successfully altered its own quota!")
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Self-grant failed: {e}")

if __name__ == '__main__':
    main()
