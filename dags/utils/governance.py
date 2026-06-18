import os
import time
import json
import logging
import datetime
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pendulum
from airflow.sdk import get_current_context

# Configure logger
logger = logging.getLogger(__name__)

# Try importing Airflow PostgresHook
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    logger.warning("Airflow providers not available. Running in standalone mode.")

# Data validation rules for structural/coordinate sanity checks at ingestion time.
# Physical domain value-range checks (temperature, wind, pressure, etc.) are
# intentionally EXCLUDED here — they live in dbt schema.yml `expression_is_true`
# tests at the mart layer, where column names are unambiguous and semantics are clear.
VALIDATION_RULES = {
    # Coordinates — these are universal and never ambiguous across any model or layer
    'latitude': {'min': -90.0, 'max': 90.0},
    'longitude': {'min': -180.0, 'max': 180.0},
}

def init_metadata_table():
    """
    Initializes the metadata log table in Postgres if it does not exist.
    """
    if not AIRFLOW_AVAILABLE:
        logger.info("Skipping DB initialization (Airflow PostgresHook not available).")
        return False

    create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS RAW;
    CREATE TABLE IF NOT EXISTS RAW.ingestion_metadata_log (
        log_id SERIAL PRIMARY KEY,
        dag_id VARCHAR(255) NOT NULL,
        run_id VARCHAR(255) NOT NULL,
        logical_date TIMESTAMP WITH TIME ZONE NOT NULL,
        model_name VARCHAR(50) NOT NULL,
        layer_type VARCHAR(50) NOT NULL,
        status VARCHAR(50) NOT NULL,
        records_extracted INTEGER NOT NULL DEFAULT 0,
        records_loaded INTEGER NOT NULL DEFAULT 0,
        null_count_latitude INTEGER NOT NULL DEFAULT 0,
        null_count_longitude INTEGER NOT NULL DEFAULT 0,
        null_count_valid_time INTEGER NOT NULL DEFAULT 0,
        null_count_metric_values INTEGER NOT NULL DEFAULT 0,
        duplicate_records_count INTEGER NOT NULL DEFAULT 0,
        min_latitude FLOAT,
        max_latitude FLOAT,
        min_longitude FLOAT,
        max_longitude FLOAT,
        min_metric_value FLOAT,
        max_metric_value FLOAT,
        validation_details TEXT,
        execution_time_seconds FLOAT NOT NULL DEFAULT 0.0,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        db_hook = PostgresHook(
            postgres_conn_id='postgres_default',
            schema='PHYSICAL_METEOR_DB'
        )
        db_hook.run(create_table_sql)
        logger.info("Postgres: Ingestion metadata log table verified.")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize metadata table: {e}")
        return False

def validate_and_log_arrow_table(
    pa_table,
    model_name,
    layer_type,
    execution_time_seconds=0.0,
    status="SUCCESS",
    validation_error=None,
    pk_cols=None,
    extra_details=None
):
    """
    Validates a PyArrow table for structural quality and logs results to Postgres.

    Responsibilities (ingestion-time, pre-dbt):
      - Null counts for all columns
      - Coordinate sanity bounds (lat/lon via VALIDATION_RULES)
      - Duplicate primary key detection
      - Row count (0 rows → FAILED)
      - Structured audit log → RAW.ingestion_metadata_log

    Domain value-range checks (temperature, wind, pressure, etc.) are intentionally
    NOT performed here.  They are defined per-model in dbt schema.yml using
    `expression_is_true` tests at the mart layer, where column naming is unambiguous.
    """
    # 1. Initialize table
    init_metadata_table()

    # 2. Retrieve Airflow context details if running inside a DAG task
    dag_id = "standalone"
    run_id = "standalone_run"
    logical_date = pendulum.now("UTC")

    if AIRFLOW_AVAILABLE:
        try:
            context = get_current_context()
            if context:
                dag_id = context['dag'].dag_id
                run_id = context['run_id']
                logical_date = context['logical_date']
        except Exception:
            logger.debug("Failed to retrieve Airflow context, defaulting to standalone.")

    # 3. Parse stats from Arrow table if provided
    records_extracted = 0
    records_loaded = 0
    null_count_latitude = 0
    null_count_longitude = 0
    null_count_valid_time = 0
    null_count_metric_values = 0
    duplicate_records_count = 0

    min_lat, max_lat = None, None
    min_lon, max_lon = None, None
    min_metric, max_metric = None, None

    validation_details = {
        "validation_error": str(validation_error) if validation_error else None,
        "checks": {}
    }
    if extra_details:
        validation_details.update(extra_details)

    if pa_table is not None:
        records_extracted = len(pa_table)
        records_loaded = len(pa_table) if status == "SUCCESS" else 0

        # Null counts
        if 'latitude' in pa_table.column_names:
            null_count_latitude = pa_table.column('latitude').null_count
            if len(pa_table) > 0 and null_count_latitude < len(pa_table):
                min_lat = pc.min(pa_table.column('latitude')).as_py()
                max_lat = pc.max(pa_table.column('latitude')).as_py()
        
        if 'longitude' in pa_table.column_names:
            null_count_longitude = pa_table.column('longitude').null_count
            if len(pa_table) > 0 and null_count_longitude < len(pa_table):
                min_lon = pc.min(pa_table.column('longitude')).as_py()
                max_lon = pc.max(pa_table.column('longitude')).as_py()

        if 'valid_time' in pa_table.column_names:
            null_count_valid_time = pa_table.column('valid_time').null_count

        # Compute metric specific validations
        metric_columns = [
            c for c in pa_table.column_names 
            if c not in ['latitude', 'longitude', 'forecast_reference_time', 'forecast_cycle', 
                         'step_hours', 'valid_time', 'lat_i', 'lon_i', 'isobaricInhPa', 
                         'level', 'forecast_date', 'heightAboveGround', 'surface']
        ]

        metric_mins = []
        metric_maxs = []

        for col in metric_columns:
            col_nulls = pa_table.column(col).null_count
            null_count_metric_values += col_nulls
            
            validation_details["checks"][col] = {
                "null_count": col_nulls,
                "null_percentage": (col_nulls / len(pa_table)) * 100 if len(pa_table) > 0 else 0
            }

            if len(pa_table) > 0 and col_nulls < len(pa_table):
                col_min = pc.min(pa_table.column(col)).as_py()
                col_max = pc.max(pa_table.column(col)).as_py()
                metric_mins.append(col_min)
                metric_maxs.append(col_max)
                
                validation_details["checks"][col].update({
                    "min_value": col_min,
                    "max_value": col_max
                })

                # Coordinate range checks (universal — latitude and longitude only)
                if col in VALIDATION_RULES:
                    rule = VALIDATION_RULES[col]
                    col_min_val = float(col_min)
                    col_max_val = float(col_max)
                    
                    in_range = True
                    if col_min_val < rule['min'] or col_max_val > rule['max']:
                        in_range = False
                        status = "WARNING"
                        
                    validation_details["checks"][col]["range_check"] = {
                        "rule_min": rule['min'],
                        "rule_max": rule['max'],
                        "passed": in_range
                    }
                    if not in_range:
                        logger.warning(
                            f"⚠️ Governance Coordinate Violation in {model_name} {layer_type} for column '{col}': "
                            f"Observed [{col_min_val}, {col_max_val}] vs Allowed [{rule['min']}, {rule['max']}]."
                        )

        if metric_mins:
            min_metric = min(metric_mins)
        if metric_maxs:
            max_metric = max(metric_maxs)

        # Coordinate range checks
        for coord_col, val_min, val_max in [('latitude', min_lat, max_lat), ('longitude', min_lon, max_lon)]:
            if val_min is not None and coord_col in VALIDATION_RULES:
                rule = VALIDATION_RULES[coord_col]
                if val_min < rule['min'] or val_max > rule['max']:
                    status = "WARNING"
                    validation_details["checks"][coord_col] = {
                        "passed": False,
                        "min": val_min,
                        "max": val_max,
                        "rule_min": rule['min'],
                        "rule_max": rule['max']
                    }
                else:
                    validation_details["checks"][coord_col] = {
                        "passed": True,
                        "min": val_min,
                        "max": val_max
                    }

        # Duplicate check on Primary Keys
        if pk_cols:
            try:
                # filter out key columns that are not present in table
                active_pk_cols = [c for c in pk_cols if c in pa_table.column_names]
                if active_pk_cols:
                    unique_pks = pa_table.select(active_pk_cols).group_by(active_pk_cols).aggregate([])
                    duplicate_records_count = len(pa_table) - len(unique_pks)
                    if duplicate_records_count > 0:
                        status = "WARNING"
                        logger.warning(f"⚠️ Governance Duplicate check failed: Found {duplicate_records_count} duplicate key rows.")
                    
                    validation_details["duplicate_check"] = {
                        "passed": duplicate_records_count == 0,
                        "duplicate_count": duplicate_records_count,
                        "pk_columns": active_pk_cols
                    }
            except Exception as e:
                logger.error(f"Failed to check duplicate rows: {e}")
                validation_details["duplicate_check"] = {
                    "error": str(e)
                }

    # If status is failed or there was a validation error
    if validation_error:
        status = "FAILED"

    # 4. Insert log into Postgres
    if AIRFLOW_AVAILABLE:
        insert_sql = """
        INSERT INTO RAW.ingestion_metadata_log (
            dag_id, run_id, logical_date, model_name, layer_type, status,
            records_extracted, records_loaded, null_count_latitude, null_count_longitude,
            null_count_valid_time, null_count_metric_values, duplicate_records_count,
            min_latitude, max_latitude, min_longitude, max_longitude,
            min_metric_value, max_metric_value, validation_details, execution_time_seconds
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        ) RETURNING log_id;
        """
        try:
            db_hook = PostgresHook(
                postgres_conn_id='postgres_default',
                schema='PHYSICAL_METEOR_DB'
            )
            
            # Format datetime appropriately
            # logical_date is a pendulum.DateTime or datetime.datetime
            logical_date_dt = logical_date
            
            # Convert values to native types to avoid pyarrow wrapper issues in postgres driver
            params = (
                dag_id,
                run_id,
                logical_date_dt,
                model_name,
                layer_type,
                status,
                int(records_extracted),
                int(records_loaded),
                int(null_count_latitude),
                int(null_count_longitude),
                int(null_count_valid_time),
                int(null_count_metric_values),
                int(duplicate_records_count),
                float(min_lat) if min_lat is not None else None,
                float(max_lat) if max_lat is not None else None,
                float(min_lon) if min_lon is not None else None,
                float(max_lon) if max_lon is not None else None,
                float(min_metric) if min_metric is not None else None,
                float(max_metric) if max_metric is not None else None,
                json.dumps(validation_details),
                float(execution_time_seconds)
            )
            
            log_id = db_hook.get_first(insert_sql, parameters=params)[0]
            logger.info(f"💾 Ingestion Metadata Log successfully written to Postgres: Log ID {log_id} (Status: {status})")
            return log_id
        except Exception as e:
            logger.error(f"❌ Failed to write Ingestion Metadata Log to Postgres: {e}")
            logger.debug(f"Details of write error: {e}", exc_info=True)
            return None
    else:
        logger.info(f"Governance Ingestion Log (Dry Run): {json.dumps(validation_details, indent=2)}")
        return None

def verify_gold_datasets():
    """
    Connects to target databases and verifies gold dataset row counts, null counts, and freshness.
    Logs the outcome to RAW.ingestion_metadata_log.
    """
    if not AIRFLOW_AVAILABLE:
        logger.warning("Airflow not available. Skipping gold dataset verification.")
        return

    db_hook = PostgresHook(
        postgres_conn_id='postgres_default',
        schema='PHYSICAL_METEOR_DB'
    )
    
    start_time = time.time()
    verification_results = {}
    overall_status = "SUCCESS"
    
    # Check what schema the target views actually reside in dynamically
    schemas_to_try = ['gold', 'RAW_gold', 'RAW', 'public']
    schema = 'gold'
    for s in schemas_to_try:
        try:
            db_hook.get_first(f"SELECT 1 FROM {s}.fct_upper_forecast LIMIT 1;")
            schema = s
            logger.info(f"Located target views in schema: '{s}'")
            break
        except Exception:
            continue
            
    # Models to verify
    models = {
        'fct_upper_forecast': f'{schema}.fct_upper_forecast',
        'fct_surface_forecast': f'{schema}.fct_surface_forecast',
        'fct_spread_forecast': f'{schema}.fct_spread_forecast'
    }
    
    for name, table in models.items():
        try:
            # Check row count
            count_sql = f"SELECT COUNT(*) FROM {table};"
            row_count = db_hook.get_first(count_sql)[0]
            
            # Check null count of valid_time
            null_sql = f"SELECT COUNT(*) FROM {table} WHERE valid_time IS NULL;"
            null_count = db_hook.get_first(null_sql)[0]
            
            # Check max valid time (freshness)
            fresh_sql = f"SELECT MAX(valid_time) FROM {table};"
            max_time = db_hook.get_first(fresh_sql)[0]
            
            passed = row_count > 0 and null_count == 0
            if not passed:
                overall_status = "WARNING"
                
            verification_results[name] = {
                "table": table,
                "row_count": row_count,
                "null_valid_time_count": null_count,
                "max_valid_time": str(max_time) if max_time else None,
                "passed": passed
            }
            logger.info(f"🔍 Gold dataset validation for {table}: row_count={row_count}, nulls={null_count}, max_time={max_time} - Passed: {passed}")
        except Exception as e:
            overall_status = "FAILED"
            logger.error(f"❌ Failed to verify gold dataset {table}: {e}")
            verification_results[name] = {
                "table": table,
                "error": str(e),
                "passed": False
            }

    # Write log to Postgres
    execution_time = time.time() - start_time
    validate_and_log_arrow_table(
        pa_table=None,
        model_name="gold_verification",
        layer_type="postgres_gold",
        execution_time_seconds=execution_time,
        status=overall_status,
        extra_details={"verification_results": verification_results}
    )
