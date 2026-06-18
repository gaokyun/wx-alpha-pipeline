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
        print("\n--- TABLES INFO ---")
        cursor.execute("""
            SELECT owner, table_name, num_rows, blocks, partitioned
            FROM all_tables
            WHERE table_name IN ('FCT_IFS_SURFACE', 'FCT_IFS_UPPER', 'FCT_GFS_SURFACE', 'FCT_GFS_UPPER')
        """)
        for r in cursor.fetchall():
            print(f"Table: {r}")
            
        cursor.execute("""
            SELECT table_name, partition_name, high_value
            FROM all_tab_partitions
            WHERE table_name IN ('FCT_IFS_SURFACE', 'FCT_IFS_UPPER')
            ORDER BY table_name, partition_name
        """)
        for r in cursor.fetchall():
            print(f"Partition: {r}")

        print("\n--- INDEXES ---")
        cursor.execute("""
            SELECT index_name, table_name, uniqueness, status
            FROM all_indexes
            WHERE table_name IN ('FCT_IFS_SURFACE', 'FCT_IFS_UPPER')
              AND owner = 'PHYSICAL_METEOR_GOLD'
        """)
        for r in cursor.fetchall():
            print(f"Index: {r}")

        cursor.execute("""
            SELECT index_name, column_name, column_position
            FROM all_ind_columns
            WHERE table_name IN ('FCT_IFS_SURFACE', 'FCT_IFS_UPPER')
              AND index_owner = 'PHYSICAL_METEOR_GOLD'
            ORDER BY index_name, column_position
        """)
        for r in cursor.fetchall():
            print(f"Index Column: {r}")

    except Exception as e:
        print(f"Query failed: {e}")
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
