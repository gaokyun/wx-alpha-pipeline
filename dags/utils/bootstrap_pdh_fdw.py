import os
import psycopg2
import duckdb
from dotenv import load_dotenv

TYPE_MAPPING = {
    "BOOLEAN": "bool",
    "BIGINT": "int8",
    "HUGEINT": "numeric",
    "INTEGER": "int4",
    "SMALLINT": "int2",
    "TINYINT": "int2",
    "FLOAT": "float4",
    "DOUBLE": "float8",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMPTZ": "timestamptz",
    "VARCHAR": "text",
    "BLOB": "bytea",
    "UUID": "uuid",
    "JSON": "jsonb"
}

def map_type(duck_type):
    duck_type = duck_type.upper()
    if "WITH TIME ZONE" in duck_type:
        return "timestamptz"
    if "TIMESTAMP" in duck_type:
        return "timestamp"
    if duck_type.endswith("[]"):
        base_type = duck_type[:-2]
        return map_type(base_type) + "[]"
    if duck_type.startswith("DECIMAL"):
        return duck_type.lower().replace("decimal", "numeric")
    return TYPE_MAPPING.get(duck_type, "text")

def bootstrap_fdw():
    # Load environment variables
    project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    load_dotenv(dotenv_path=os.path.join(project_dir, ".env"))
    load_dotenv(dotenv_path="/opt/airflow/wx-alpha-pipeline/.env")
    load_dotenv(dotenv_path="/opt/airflow/.env")

    # PostgreSQL configuration
    host = os.getenv("POSTGRES_JH_HOST", "postgres")
    user = os.getenv("POSTGRES_USERNAME", "airflow")
    password = os.getenv("POSTGRES_PASS")
    database = os.getenv("POSTGRES_DB", "PHYSICAL_METEOR_DB")
    port = int(os.getenv("POSTGRES_PORT", "5432"))

    # S3/OCI credentials
    s3_key = os.getenv("OCI_OBJECT_STORAGE_ACCESS_KEY")
    s3_secret = os.getenv("OCI_OBJECT_STORAGE_SECRET_KEY")
    s3_region = os.getenv("OCI_OBJECT_STORAGE_REGION", "us-ashburn-1")
    s3_endpoint = os.getenv("OCI_API_ENDPOINT", "idt2nq7cpbfu.compat.objectstorage.us-ashburn-1.oraclecloud.com")
    
    # Clean endpoint format
    s3_endpoint = s3_endpoint.replace("https://", "").replace("http://", "")

    # 1. Connect to local memory DuckDB to get schemas from S3
    print("Initializing local DuckDB to scan S3 schema...")
    duck_conn = duckdb.connect()
    duck_conn.execute("INSTALL httpfs; LOAD httpfs;")
    duck_conn.execute("INSTALL delta; LOAD delta;")
    duck_conn.execute(f"""
      CREATE SECRET (
        TYPE S3,
        KEY_ID '{s3_key}',
        SECRET '{s3_secret}',
        REGION '{s3_region}',
        ENDPOINT '{s3_endpoint}',
        URL_STYLE 'path'
      );
    """)

    tables = {
        "gfs_upper": "s3://oci-s3-ykg-storage/weather_data/delta_lake/gfs_raw/gfs_upper/",
        "gfs_surface": "s3://oci-s3-ykg-storage/weather_data/delta_lake/gfs_raw/gfs_surface/",
        "at_ifs_upper": "s3://oci-s3-ykg-storage/weather_data/delta_lake/ecmwf_raw/at_ifs_upper/",
        "at_ifs_surface": "s3://oci-s3-ykg-storage/weather_data/delta_lake/ecmwf_raw/at_ifs_surface/",
        "ifs_spread": "s3://oci-s3-ykg-storage/weather_data/delta_lake/ecmwf_raw/ifs_spread/",
        "at_aifs_upper": "s3://oci-s3-ykg-storage/weather_data/delta_lake/ecmwf_raw/at_aifs_upper/",
        "at_aifs_surface": "s3://oci-s3-ykg-storage/weather_data/delta_lake/ecmwf_raw/at_aifs_surface/",
        "aifs_spread": "s3://oci-s3-ykg-storage/weather_data/delta_lake/ecmwf_raw/aifs_spread/"
    }

    schema_definitions = {}
    for tbl_name, s3_path in tables.items():
        print(f"Describing columns for table '{tbl_name}'...")
        try:
            res = duck_conn.execute(f"DESCRIBE SELECT * FROM delta_scan('{s3_path}');").fetchall()
            cols = []
            for row in res:
                col_name = row[0].lower()
                duck_type = row[1]
                pg_type = map_type(duck_type)
                cols.append(f'"{col_name}" {pg_type}')
            schema_definitions[tbl_name] = ", ".join(cols)
        except Exception as e:
            print(f"Warning: Failed to describe '{tbl_name}' from S3: {e}")
            raise e

    # 2. Connect to PostgreSQL to apply DDL
    print(f"Connecting to PostgreSQL at {host}:{port} ({database}) as user '{user}'...")
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        print("Enabling duckdb_fdw extension...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS duckdb_fdw;")

        print("Ensuring target schema 'raw' exists...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        print("Recreating DuckDB foreign server (duckdb_srv)...")
        cursor.execute("DROP SERVER IF EXISTS duckdb_srv CASCADE;")
        
        server_sql = f"""
        CREATE SERVER duckdb_srv
        FOREIGN DATA WRAPPER duckdb_fdw
        OPTIONS (
          database ':memory:',
          s3_region '{s3_region}',
          s3_endpoint '{s3_endpoint}',
          s3_use_ssl 'true'
        );
        """
        cursor.execute(server_sql)

        print("Creating User Mapping for 'airflow'...")
        mapping_sql = f"""
        CREATE USER MAPPING FOR airflow SERVER duckdb_srv;
        """
        cursor.execute(mapping_sql)

        print("Installing & loading DuckDB extensions...")
        cursor.execute("SELECT duckdb_execute('duckdb_srv', 'INSTALL httpfs; LOAD httpfs;');")
        cursor.execute("SELECT duckdb_execute('duckdb_srv', 'INSTALL delta; LOAD delta;');")

        print("Configuring persistent OCI S3 secret in DuckDB session...")
        secret_sql = f"""
        SELECT duckdb_execute('duckdb_srv', $$
          CREATE OR REPLACE PERSISTENT SECRET oci_secret (
            TYPE S3,
            KEY_ID '{s3_key}',
            SECRET '{s3_secret}',
            REGION '{s3_region}',
            ENDPOINT '{s3_endpoint}',
            URL_STYLE 'path'
          );
        $$);
        """
        cursor.execute(secret_sql)

        for tbl_name, s3_path in tables.items():
            print(f"Setting up foreign table in Postgres for '{tbl_name}'...")
            
            # Create Postgres foreign table using the described schema columns
            col_definitions = schema_definitions[tbl_name]
            cursor.execute(f"DROP FOREIGN TABLE IF EXISTS raw.{tbl_name} CASCADE;")
            
            create_ft_sql = f"""
            CREATE FOREIGN TABLE raw.{tbl_name} (
              {col_definitions}
            )
            SERVER duckdb_srv
            OPTIONS (table '(SELECT * FROM delta_scan(''{s3_path}'')) AS "{tbl_name}"');
            """
            cursor.execute(create_ft_sql)
            print(f"  -> Created foreign table raw.{tbl_name}")

        print("Postgres-DuckDB Hybrid FDW bootstrapping completed successfully!")

    except Exception as e:
        print(f"Error during bootstrapping: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    bootstrap_fdw()
