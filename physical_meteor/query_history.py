import os
import oracledb
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path="/opt/airflow/.env")
    if not os.getenv("ORACLE_USER"):
        load_dotenv(dotenv_path="/opt/airflow/wx-alpha-pipeline/.env")
        
    user = os.getenv("ORACLE_USER", "PHYSICAL_METEOR_RAW")
    password = os.getenv("ORACLE_PASSWORD")
    host = os.getenv("ORACLE_HOST", "adb.us-ashburn-1.oraclecloud.com")
    service = os.getenv("ORACLE_SERVICE", "g6fd1d6c71405c0_meteor0ykg0aidw_medium.adb.oraclecloud.com")
    
    dsn = f"(description=(address=(protocol=tcps)(port=1522)(host={host}))(connect_data=(service_name={service}))(security=(ssl_server_dn_match=no)))"
    
    print("Connecting to ADW...")
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    
    try:
        print("\n--- SQL EXECUTION HISTORY FOR SESSION ---")
        cursor.execute("""
            SELECT sql_text, sql_id, elapsed_time/1000000 elapsed_sec, cpu_time/1000000 cpu_sec, executions
            FROM v$sql
            WHERE parsing_schema_name = 'PHYSICAL_METEOR_RAW'
              AND last_active_time >= SYSDATE - 1/24
            ORDER BY last_active_time DESC
        """)
        for r in cursor.fetchall():
            print(f"SQL: {r[0][:150]}...\n  ID: {r[1]}, Elapsed: {r[2]}s, CPU: {r[3]}s, Execs: {r[4]}")
            
    except Exception as e:
        print(f"Query failed: {e}")
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
