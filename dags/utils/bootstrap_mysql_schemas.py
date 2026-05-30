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
    if not password:
        raise ValueError("ORACLE_PASSWORD environment variable is not set")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    
    print(f"Connecting to MySQL at {host}:{port} as {user}...")
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        port=port
    )
    cursor = conn.cursor()
    
    print("Creating target database PHYSICAL_METEOR_GOLD...")
    cursor.execute("CREATE DATABASE IF NOT EXISTS PHYSICAL_METEOR_GOLD")
    
    conn.commit()
    cursor.close()
    conn.close()
    print("MySQL database bootstrapping complete!")

if __name__ == "__main__":
    main()
