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

    # We want to ensure partitions exist from 5 days ago to 10 days in the future
    start_date = datetime.utcnow().date() - timedelta(days=5)
    dates_to_ensure = [start_date + timedelta(days=i) for i in range(16)]

    for table in tables:
        print(f"Ensuring partitions for gold.{table}...")
        
        hours = [0, 6, 12, 18]
        if "spread" in table or table.startswith("fct_ifs_"):
            hours = [0, 12]

        for hour in hours:
            # Ensure Level-1 partition exists
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS gold.{table}_c{hour:02d}
                PARTITION OF gold.{table}
                FOR VALUES IN ({hour})
                PARTITION BY RANGE (cycle_date);
            """)

            # Ensure Level-2 partitions exist for all target dates
            for dt in dates_to_ensure:
                dt_str = dt.strftime("%Y_%m_%d")
                start_str = dt.strftime("%Y-%m-%d")
                end_str = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
                
                part_name = f"gold.{table}_c{hour:02d}_p{dt_str}"
                
                try:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {part_name}
                        PARTITION OF gold.{table}_c{hour:02d}
                        FOR VALUES FROM ('{start_str}') TO ('{end_str}')
                        USING columnar;
                    """)
                except Exception as e:
                    print(f"Skipped {part_name}: {e}")

    print("Successfully pre-created all necessary partitions!")

if __name__ == "__main__":
    main()
