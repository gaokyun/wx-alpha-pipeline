import os
import mysql.connector
from dotenv import load_dotenv

def main():
    load_dotenv(dotenv_path="/opt/airflow/wx-alpha-pipeline/.env")
    if not os.getenv("MYSQL_HOST"):
        load_dotenv(dotenv_path="/opt/airflow/.env")
        
    host = os.getenv("MYSQL_HOST", os.getenv("OCI_MYSQL_PRIVATE_IP", "10.0.1.58"))
    user = os.getenv("MYSQL_USER", "admin")
    password = os.getenv("ORACLE_PASSWORD")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    
    print(f"Connecting to MySQL at {host}:{port} as {user}...")
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        port=port,
        database="PHYSICAL_METEOR_GOLD"
    )
    cursor = conn.cursor()
    
    targets = [
        "fct_spread_forecast",
        "fct_surface_forecast",
        "fct_upper_forecast",
        "fct_spread_forecast__dbt_tmp",
        "fct_surface_forecast__dbt_tmp",
        "fct_upper_forecast__dbt_tmp"
    ]
    
    for target in targets:
        print(f"Dropping: {target}")
        try:
            cursor.execute(f"DROP VIEW IF EXISTS `{target}`")
            print(f"  Successfully dropped view (if existed)")
        except Exception as e:
            print(f"  Failed to drop view: {e}")
            
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{target}`")
            print(f"  Successfully dropped table (if existed)")
        except Exception as e:
            print(f"  Failed to drop table: {e}")
        
    conn.commit()
    cursor.close()
    conn.close()
    print("Cleanup complete!")

if __name__ == "__main__":
    main()
