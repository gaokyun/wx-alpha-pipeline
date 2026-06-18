import os
import oracledb
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path="/opt/airflow/wx-alpha-pipeline/.env")
    if not os.getenv("ORACLE_USER"):
        load_dotenv(dotenv_path="/opt/airflow/.env")
        
    user = os.getenv("ORACLE_USER", "PHYSICAL_METEOR_RAW")
    password = os.getenv("ORACLE_PASSWORD")
    host = os.getenv("ORACLE_HOST", "adb.us-ashburn-1.oraclecloud.com")
    service = os.getenv("ORACLE_SERVICE", "g6fd1d6c71405c0_meteor0ykg0aidw_high.adb.oraclecloud.com")
    
    dsn = f"(description=(address=(protocol=tcps)(port=1522)(host={host}))(connect_data=(service_name={service}))(security=(ssl_server_dn_match=no)))"
    
    print("Connecting to ADW...")
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    
    print("\n--- ACTIVE SESSIONS ---")
    sql_sessions = """
    SELECT 
        sid, 
        serial#, 
        username, 
        status, 
        osuser, 
        machine, 
        program, 
        event, 
        seconds_in_wait,
        sql_id
    FROM v$session 
    WHERE username IN ('PHYSICAL_METEOR_RAW', 'PHYSICAL_METEOR_GOLD', 'ADMIN')
    """
    try:
        cursor.execute(sql_sessions)
        rows = cursor.fetchall()
        print(f"Found {len(rows)} active sessions:")
        for r in rows:
            print(f"  SID: {r[0]}, Serial: {r[1]}, User: {r[2]}, Status: {r[3]}, OS: {r[4]}, Machine: {r[5]}, Program: {r[6]}\n    Event: {r[7]}, WaitSeconds: {r[8]}, SQL_ID: {r[9]}")
            if r[9]:
                cursor.execute(f"SELECT sql_fulltext FROM v$sql WHERE sql_id = '{r[9]}'")
                sql_txt = cursor.fetchone()
                if sql_txt:
                    print(f"    SQL TEXT: {sql_txt[0].read() if hasattr(sql_txt[0], 'read') else sql_txt[0]}")
    except Exception as e:
        print(f"Failed to query sessions: {e}")
        
    print("\n--- LOCKED OBJECTS ---")
    sql_locks = """
    SELECT 
        l.oracle_username, 
        o.object_name, 
        o.object_type, 
        l.locked_mode, 
        s.sid, 
        s.serial#,
        s.program
    FROM v$locked_object l
    JOIN dba_objects o ON l.object_id = o.object_id
    JOIN v$session s ON l.session_id = s.sid
    """
    try:
        cursor.execute(sql_locks)
        rows = cursor.fetchall()
        print(f"Found {len(rows)} locked objects:")
        for r in rows:
            print(f"  User: {r[0]}, Object: {r[1]} ({r[2]}), Mode: {r[3]}, SID: {r[4]}, Serial: {r[5]}, Program: {r[6]}")
    except Exception as e:
        print(f"Failed to query locks: {e}")
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
