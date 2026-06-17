import os
import oracledb

def main():
    user = os.getenv("ORACLE_USER", "PHYSICAL_METEOR_RAW")
    password = os.getenv("ORACLE_PASSWORD")
    host = os.getenv("ORACLE_HOST", "adb.us-ashburn-1.oraclecloud.com")
    service = os.getenv("ORACLE_SERVICE", "g6fd1d6c71405c0_meteor0ykg0aidw_high.adb.oraclecloud.com")
    
    dsn = f"(description=(address=(protocol=tcps)(port=1522)(host={host}))(connect_data=(service_name={service}))(security=(ssl_server_dn_match=no)))"
    
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    
    for table in ["EXT_GFS_SURFACE", "EXT_GFS_UPPER"]:
        print(f"\n--- Columns in {table} ---")
        cursor.execute(f"SELECT column_name, data_type FROM user_tab_columns WHERE table_name = '{table}'")
        for row in cursor.fetchall():
            print(f"{row[0]}: {row[1]}")
            
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
