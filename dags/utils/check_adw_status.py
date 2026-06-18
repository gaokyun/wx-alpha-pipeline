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
        print("Error: ORACLE_PASSWORD is not set!")
        return

    connection = None
    for pwd in [admin_password, oracle_password]:
        if not pwd:
            continue
        try:
            print("Connecting to ADW as admin...")
            connection = oracledb.connect(user="admin", password=pwd, dsn=dsn)
            print("Successfully connected as admin!")
            break
        except Exception as e:
            print(f"Admin connection failed: {e}")

    if not connection:
        print("Falling back to PHYSICAL_METEOR_RAW...")
        try:
            connection = oracledb.connect(user="PHYSICAL_METEOR_RAW", password=oracle_password, dsn=dsn)
            print("Successfully connected!")
        except Exception as e:
            print(f"Connection failed: {e}")
            return

    cursor = connection.cursor()

    # 1. Query tablespaces and file sizes
    try:
        print("\nChecking tablespace files and quotas...")
        cursor.execute("""
            SELECT tablespace_name, file_name, bytes/1024/1024, maxbytes/1024/1024, autoextensible 
            FROM dba_data_files
        """)
        for df in cursor.fetchall():
            print(f"DataFile tablespace={df[0]}, size={df[2]:.1f}MB, max_size={df[3]:.1f}MB, autoext={df[4]}")
            
        cursor.execute("""
            SELECT tablespace_name, file_name, bytes/1024/1024, maxbytes/1024/1024, autoextensible 
            FROM dba_temp_files
        """)
        for tf in cursor.fetchall():
            print(f"TempFile tablespace={tf[0]}, size={tf[2]:.1f}MB, max_size={tf[3]:.1f}MB, autoext={tf[4]}")
    except Exception as e:
        print(f"Could not query DBA files: {e}")


    # 2. Check active user sessions/queries (if we have privileges)
    try:
        print("\nChecking active user sessions/queries...")
        cursor.execute("""
            SELECT s.sid, s.serial#, s.status, s.username, s.program, s.sql_id, q.sql_fulltext
            FROM v$session s
            LEFT JOIN v$sql q ON s.sql_id = q.sql_id
            WHERE s.status = 'ACTIVE' AND s.username IS NOT NULL
        """)
        sessions = cursor.fetchall()
        print(f"Active sessions count: {len(sessions)}")
        for sess in sessions:
            sql_text = sess[6]
            if sql_text:
                sql_text = sql_text.read()
            print(f"Session SID={sess[0]}, Serial={sess[1]}, Program={sess[4]}, SQL_ID={sess[5]}")
            print(f"SQL Text:\n{sql_text}\n")
    except Exception as e:
        print(f"Could not query v$session/v$sql: {e}")

    # 3. Check for any locks
    try:
        print("\nChecking locks...")
        cursor.execute("""
            SELECT 
                (SELECT username FROM v$session WHERE sid=a.sid) locker,
                a.sid,
                (SELECT name FROM v$bgprocess WHERE paddr=s.paddr) bgproc,
                a.type,
                lmode,
                request,
                id1,
                id2
            FROM v$lock a, v$session s
            WHERE a.sid = s.sid
        """)
        locks = cursor.fetchall()
        print(f"Locks count: {len(locks)}")
        for lock in locks[:20]: # print up to 20 locks
            print(lock)
    except Exception as e:
        print(f"Could not query locks: {e}")

    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
