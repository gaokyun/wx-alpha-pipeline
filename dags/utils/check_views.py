import os
import oracledb
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path="/opt/airflow/.env")
    
    user = os.getenv("ORACLE_USER", "PHYSICAL_METEOR_RAW")
    password = os.getenv("ORACLE_PASSWORD")
    if not password:
        raise ValueError("ORACLE_PASSWORD environment variable is not set")
    host = os.getenv("ORACLE_HOST", "adb.us-ashburn-1.oraclecloud.com")
    service = os.getenv("ORACLE_SERVICE", "g6fd1d6c71405c0_meteor0ykg0aidw_high.adb.oraclecloud.com")
    
    dsn = f"(description=(address=(protocol=tcps)(port=1522)(host={host}))(connect_data=(service_name={service}))(security=(ssl_server_dn_match=no)))"
    
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    
    print("\n--- Listing all views in PHYSICAL_METEOR_RAW schema ---")
    cursor.execute("""
    SELECT view_name 
    FROM user_views
    ORDER BY view_name
    """)
    for row in cursor.fetchall():
        print(f"View: {row[0]}")
        
    print("\n--- Listing all tables in PHYSICAL_METEOR_RAW schema ---")
    cursor.execute("""
    SELECT table_name
    FROM user_tables
    ORDER BY table_name
    """)
    for row in cursor.fetchall():
        print(f"Table: {row[0]}")
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
