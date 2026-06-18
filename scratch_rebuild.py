import os
import sys
import psycopg2

def get_connection():
    host = "postgres"
    try:
        conn = psycopg2.connect(
            host=host,
            database="PHYSICAL_METEOR_DB",
            user="airflow",
            password="airflow",
            port=5432
        )
        return conn
    except Exception as e:
        return psycopg2.connect(
            host="localhost",
            database="PHYSICAL_METEOR_DB",
            user="airflow",
            password="airflow",
            port=5432
        )

def rebuild_invalid_indexes():
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    # Increase maintenance_work_mem so Postgres uses RAM instead of writing pgsql_tmp files to disk
    try:
        cur.execute("SET maintenance_work_mem = '3GB';")
        print("Set maintenance_work_mem to 3GB successfully.")
    except Exception as e:
        print(f"Failed to set maintenance_work_mem: {e}")
        
    invalid_indexes = [
        {
            "table": "gold.fct_gfs_upper",
            "index_name": "idx_fct_gfs_upper_uk",
            "cols": "cycle_date, cycle_hour, forecast_step_hours, lat_i, lon_i, pressure_level_hpa"
        },
        {
            "table": "gold.fct_aifs_upper",
            "index_name": "idx_fct_aifs_upper_uk",
            "cols": "cycle_date, cycle_hour, forecast_step_hours, lat_i, lon_i, pressure_level_hpa"
        }
    ]
    
    for item in invalid_indexes:
        table = item["table"]
        idx_name = item["index_name"]
        cols = item["cols"]
        schema, name = table.split('.')
        
        print(f"\n--- Rebuilding index {idx_name} on {table} ---")
        
        # Drop concurrently if exists
        print(f"Dropping index concurrently: {idx_name}")
        cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {schema}.{idx_name};")
        
        # Recreate concurrently
        sql = f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {table} ({cols});"
        print(f"Running: {sql}")
        try:
            cur.execute(sql)
            print(f"✅ Successfully rebuilt unique index {idx_name} on {table}.")
        except Exception as e:
            print(f"❌ Failed to build unique index {idx_name} on {table}: {e}")
            
    cur.close()
    conn.close()

if __name__ == "__main__":
    rebuild_invalid_indexes()
