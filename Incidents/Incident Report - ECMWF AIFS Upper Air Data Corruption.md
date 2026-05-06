Incident Report: ECMWF AIFS Upper Air Data CorruptionDate: 2026-05-01Category: Incident Report & Prevention SOPStatus: Resolved & DocumentedSystems Impacted: OCI Delta Lake, DuckDB Gold Layer, Airflow Transformation DAGs1. Executive SummaryDuring the ingestion of the ECMWF AIFS "Upper" meteorological dataset, the pipeline experienced a series of fatal DuckDB crashes. The root cause was twofold:Data Quality: Impossible coordinate values (e.g., -538,099) in the source GRIB files.Infrastructure Corruption: Truncated Parquet files on OCI resulting in negative file offsets, which triggered an "Information loss on integer cast" error in DuckDB's httpfs extension.2. Technical Root Cause AnalysisA. The "Ghost Coordinate" CrashSymptom: Fatal Error: Information loss on integer cast: value -332482 outside of target range.Mechanism: The pipeline attempted to cast raw coordinates to unsigned integers for indexing. High-magnitude negative values (garbage data from the source) exceeded the uint64 bounds, invalidating the DuckDB session.B. The "Poisoned Footer" (Parquet Truncation)Symptom: Invalidated database ... Original error: value -1751548.Mechanism: Parquet files store metadata at the end (the footer). If an upload to OCI is interrupted, the file is truncated. When DuckDB attempts to calculate the metadata offset (File Size - Metadata Length), a truncated file results in a negative offset. DuckDB’s attempt to cast this negative address to a memory pointer causes a fatal crash.3. Best Practices & Advanced ResolutionLayer 1: Staging (Data Quality Guard)To prevent the "Ghost Coordinate" crash, every staging model must implement a Geography Guard. This filters out impossible meteorological values before they hit any casting or indexing logic.-- Pattern for stg_ecmwf_aifs_surface/upper
WITH raw_data AS (
    SELECT * FROM {{ source('ecmwf_raw', 'at_aifs_upper') }}
    -- Filter out garbage coordinates/null masks
    WHERE latitude BETWEEN -90 AND 90
      AND longitude BETWEEN -180 AND 360
)
SELECT ... FROM raw_data
Layer 2: Gold Layer (Integer-Based Indexing)To ensure stable unique_key joins in DuckDB, coordinates must be shifted and scaled into a Fixed-Point Integer Index.Formula: lat_i = (latitude + 90) * 100 | lon_i = (longitude + 360) * 100Layer 3: Advanced Recovery (Transaction Management)Instead of deleting the entire _delta_log and folder, use these surgical methods:Delta Time Travel (Rollback):If a partition upload fails, use Delta's versioning to revert to the last known good state.-- Identify the last good version
SELECT * FROM delta_history('s3://bucket/at_aifs_upper/');
-- Restore to version before the crash
RESTORE TABLE delta_scan('s3://bucket/at_aifs_upper/') TO VERSION AS OF <last_good_version>;
Atomic Partition Overwrite:Configure the writer to use partition_overwrite mode. This ensures that the new data is fully uploaded and validated before the old/corrupted file is unlinked in the _delta_log.Surgical File Vacuuming:If a specific file is identified as corrupted (via glob), use VACUUM with a short retention period (e.g., 0 hours) after restoring to physically remove orphaned "poisoned" files.4. Monitoring & Architectural ResilienceCheckpointing: Enable frequent Delta checkpoints (every 10 commits) to minimize log file overhead.File Splitting Strategy: Use a ROW_GROUP_SIZE (e.g., 100,000) to ensure data is split into multiple Parquet files, localizing potential corruption.Post-Write Verification: Implement a "Smoke Test" in the Airflow task immediately following the write to catch truncated footers.5. Summary Checklist for Future Cycles[x] Filter: Are latitude and longitude range-checked in Staging?[x] Cast: Is lat_i and lon_i used in the Gold Layer unique_key?[x] Split: Is the Parquet writer configured to produce multiple files per partition?[x] Validate: Does the DAG include a check for "Invalidated Database" errors post-extraction?6. Applied Code Improvements (Python Extractors)The extraction scripts have been hardened to include file splitting and an immediate post-write integrity sensor.def _verify_delta_integrity(delta_table_oci_path, date_obj, cycle):
    """Integrity Sensor: Verifies the Parquet footer of newly written partitions."""
    import duckdb
    import pendulum
    
    part_date = pendulum.instance(date_obj).format("YYYY-MM-DD")
    logger.info(f"🔍 Running Integrity Sensor on {delta_table_oci_path} for {part_date} {cycle}z")
    
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL delta; LOAD delta;")
        con.execute(f"SET s3_access_key_id='{weather_config.OCI_ACCESS_KEY}';")
        con.execute(f"SET s3_secret_access_key='{weather_config.OCI_SECRET_KEY}';")
        con.execute(f"SET s3_endpoint='{weather_config.OCI_ENDPOINT.replace('https://', '')}';")
        con.execute("SET s3_url_style='path';")
        con.execute(f"SET s3_region='{weather_config.OCI_REGION}';")
        
        # This will trigger the -1751548 Fatal Error immediately if a file is truncated
        test_query = f"""
            SELECT COUNT(*) FROM delta_scan('{delta_table_oci_path}') 
            WHERE forecast_date = '{part_date}' AND forecast_cycle = {cycle}
        """
        con.execute(test_query).fetchone()
        logger.info("✅ Integrity check passed. Parquet footers are healthy.")
    except Exception as e:
        logger.error(f"❌ INTEGRITY SENSOR FAILED: Parquet corruption detected post-write. {e}")
        raise RuntimeError("Data corruption detected post-write. Failing task for safe retry/restore.") from e
    finally:
        con.close()

def download_gfs_robust(date_obj, cycle, steps, task_type='upper'):
    """GFS Downloader (GRIB2 -> Xarray -> PyArrow -> Delta Lake on OCI Object Storage)"""
    # ... [Existing Extraction Logic] ...
    
    if arrow_tables and master_pk_cols:
        try:
            master_table = pa.concat_tables(unified_tables)
            delta_table_oci_path = f"s3://{weather_config.OCI_BUCKET}/weather_data/delta_lake/gfs_raw/{task_name}/"
            
            storage_options = {
                "AWS_ACCESS_KEY_ID": weather_config.OCI_ACCESS_KEY,
                "AWS_SECRET_ACCESS_KEY": weather_config.OCI_SECRET_KEY,
                "AWS_REGION": weather_config.OCI_REGION,
                "AWS_ENDPOINT_URL": weather_config.OCI_ENDPOINT,
                "AWS_S3_ADDRESSING_STYLE": "path"
            }
            
            # IMPROVEMENT: File Splitting & Atomic Overwrite
            write_options = {
                "max_rows_per_file": 100000,
                "mode": "overwrite"
            }
            
            upsert_weather_data(master_table, master_pk_cols, delta_table_oci_path, storage_options, **write_options)
            
            # POST-WRITE VERIFICATION
            _verify_delta_integrity(delta_table_oci_path, date_obj, cycle)
            
            build_duckdb_silver_layer(task_name, delta_table_oci_path)
            
        except Exception as e:
            logger.error(f"❌ Batch Upsert Failed: {e}")
            all_success = False
            
    return all_success
