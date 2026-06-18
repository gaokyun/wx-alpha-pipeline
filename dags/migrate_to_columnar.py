import os
import psycopg2
from datetime import datetime, timedelta

def get_connection():
    return psycopg2.connect(
        host="postgres",
        database="PHYSICAL_METEOR_DB",
        user=os.getenv("POSTGRES_USERNAME", "airflow"),
        password=os.getenv("POSTGRES_PASS"),
        port=5432
    )

def main():
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    # 1. Backup all views and materialized views in the gold schema
    print("Backing up view and materialized view definitions...")
    cur.execute("""
        SELECT 'view' as type, viewname as name, definition 
        FROM pg_views 
        WHERE schemaname = 'gold'
        UNION ALL
        SELECT 'matview' as type, matviewname as name, definition 
        FROM pg_matviews 
        WHERE schemaname = 'gold';
    """)
    views = cur.fetchall()
    
    # 2. Drop views and materialized views to allow table modifications
    print("Dropping dependent views and materialized views...")
    for v_type, v_name, _ in views:
        if v_type == 'view':
            print(f"Dropping view: gold.{v_name}")
            cur.execute(f"DROP VIEW IF EXISTS gold.{v_name} CASCADE;")
        elif v_type == 'matview':
            print(f"Dropping materialized view: gold.{v_name}")
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS gold.{v_name} CASCADE;")

    # List of 8 fact tables
    tables = [
        "fct_ifs_spread",
        "fct_ifs_surface",
        "fct_ifs_upper",
        "fct_gfs_surface",
        "fct_gfs_upper",
        "fct_aifs_spread",
        "fct_aifs_surface",
        "fct_aifs_upper"
    ]

    for table in tables:
        print(f"\n=========================================")
        print(f"Migrating table: gold.{table}")
        print(f"=========================================")
        
        # Check if table is already partitioned (and thus already migrated)
        cur.execute(f"""
            SELECT c.relkind 
            FROM pg_class c 
            JOIN pg_namespace n ON n.oid = c.relnamespace 
            WHERE n.nspname = 'gold' AND c.relname = '{table}';
        """)
        res = cur.fetchone()
        if res and res[0] == 'p':
            print(f"Table gold.{table} is already partitioned! Skipping.")
            continue
            
        # A. Fetch columns
        cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'gold' AND table_name = '{table}'
            ORDER BY ordinal_position;
        """)
        columns = cur.fetchall()
        if not columns:
            print(f"Table gold.{table} not found or has no columns! Skipping.")
            continue
            
        col_defs = []
        for col_name, data_type in columns:
            col_defs.append(f"{col_name} {data_type}")
        columns_sql = ",\n    ".join(col_defs)
        
        # B. Fetch distinct cycles and dates
        print("Fetching distinct cycle_hour and cycle_date from old table...")
        cur.execute(f"SELECT DISTINCT cycle_hour, cycle_date FROM gold.{table} ORDER BY cycle_hour, cycle_date;")
        distinct_cycles = cur.fetchall()
        print(f"Found {len(distinct_cycles)} distinct cycle-date combinations.")
        
        # C. Rename old table
        print(f"Renaming gold.{table} to gold.{table}_old...")
        cur.execute(f"ALTER TABLE gold.{table} RENAME TO {table}_old;")
        
        # D. Create new parent table
        print(f"Creating new partitioned parent table gold.{table}...")
        create_parent_sql = f"""
        CREATE TABLE gold.{table} (
            {columns_sql}
        ) PARTITION BY LIST (cycle_hour);
        """
        cur.execute(create_parent_sql)
        
        # E. Determine all distinct cycle hours and pre-create level-1 partitions
        hours = sorted(list(set(cycle[0] for cycle in distinct_cycles)))
        if not hours:
            if "spread" in table or "ifs" in table:
                hours = [0, 12]
            else:
                hours = [0, 6, 12, 18]
                
        for hour in hours:
            print(f"Creating Level-1 partition for cycle_hour = {hour}...")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS gold.{table}_c{hour:02d}
                PARTITION OF gold.{table}
                FOR VALUES IN ({hour})
                PARTITION BY RANGE (cycle_date);
            """)
            
        # F. Determine all dates to precreate Level-2 partitions
        all_dates_by_hour = {}
        for hour in hours:
            all_dates_by_hour[hour] = set()
            
        for hour, dt in distinct_cycles:
            if hour in all_dates_by_hour:
                all_dates_by_hour[hour].add(dt)
                
        # Add today and next 3 days
        today = datetime.utcnow().date()
        for i in range(4):
            future_date = today + timedelta(days=i)
            for hour in hours:
                all_dates_by_hour[hour].add(future_date)
                
        for hour in hours:
            sorted_dates = sorted(list(all_dates_by_hour[hour]))
            for dt in sorted_dates:
                dt_str = dt.strftime("%Y_%m_%d")
                start_str = dt.strftime("%Y-%m-%d")
                end_str = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
                
                part_name = f"gold.{table}_c{hour:02d}_p{dt_str}"
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {part_name}
                    PARTITION OF gold.{table}_c{hour:02d}
                    FOR VALUES FROM ('{start_str}') TO ('{end_str}')
                    USING columnar;
                """)
                
        # G. Copy data from old table (this auto-routes and compresses it 10x!)
        print(f"Copying data from gold.{table}_old to gold.{table}...")
        cur.execute(f"INSERT INTO gold.{table} SELECT * FROM gold.{table}_old;")
        
        # H. Drop old table to reclaim space immediately
        print(f"Dropping gold.{table}_old and reclaiming space...")
        cur.execute(f"DROP TABLE gold.{table}_old;")
        print(f"Reclaimed space for gold.{table} successfully.")

    # 3. Restore standard views
    print("\nRestoring views...")
    for v_type, v_name, definition in views:
        if v_type == 'view':
            print(f"Creating view: gold.{v_name}")
            cur.execute(f"CREATE OR REPLACE VIEW gold.{v_name} AS {definition};")

    # 4. Restore materialized views
    print("\nRestoring materialized views...")
    for v_type, v_name, definition in views:
        if v_type == 'matview':
            print(f"Creating materialized view: gold.{v_name}")
            cur.execute(f"CREATE MATERIALIZED VIEW gold.{v_name} AS {definition};")
            
    print("\nAll views and materialized views restored successfully.")
    
    cur.close()
    conn.close()
    print("\nMigration to Columnar Storage completed successfully!")

if __name__ == "__main__":
    main()
