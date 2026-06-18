import duckdb

def main():
    db_path = "/opt/airflow/data/mysql/local_catalog.duckdb"
    print(f"Connecting to DuckDB at {db_path}...")
    con = duckdb.connect(db_path)
    
    print("\n--- Listing all databases/catalogs ---")
    res = con.execute("PRAGMA show_databases").fetchall()
    for row in res:
        print(f"Database: {row}")
        
    print("\n--- Listing all tables/views in all schemas ---")
    res = con.execute("""
        SELECT database_name, schema_name, table_name, table_type 
        FROM duckdb_tables()
    """).fetchall()
    for row in res:
        print(f"Table/View: {row}")
        
    # Drop any conflicting local_catalog tables/views
    targets = [
        "fct_spread_forecast",
        "fct_surface_forecast",
        "fct_upper_forecast",
        "fct_spread_forecast__dbt_tmp",
        "fct_surface_forecast__dbt_tmp",
        "fct_upper_forecast__dbt_tmp"
    ]
    
    for target in targets:
        for schema in ["PHYSICAL_METEOR_GOLD", "main", "silver"]:
            try:
                con.execute(f"DROP VIEW IF EXISTS local_catalog.{schema}.{target}")
                con.execute(f"DROP TABLE IF EXISTS local_catalog.{schema}.{target}")
                print(f"Dropped local_catalog.{schema}.{target} if existed.")
            except Exception as e:
                print(f"Could not drop local_catalog.{schema}.{target}: {e}")
                
    con.close()
    print("DuckDB inspection and cleanup complete!")

if __name__ == "__main__":
    main()
