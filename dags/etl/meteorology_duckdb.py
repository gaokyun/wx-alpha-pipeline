import os
import time
import logging
import requests
import subprocess
import xarray as xr
import numpy as np
import cfgrib
import pendulum
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import duckdb
from ecmwf.opendata import Client
import warnings
from dbt.cli.main import dbtRunner, dbtRunnerResult

# Silence Dask & Airflow 3 SDK redirection noise
warnings.filterwarnings("ignore", category=DeprecationWarning, module="dask.tokenize")
warnings.filterwarnings("ignore", message=".*attribute is deprecated.*")
logging.getLogger("py.warnings").setLevel(logging.ERROR)

# Configure logger
logger = logging.getLogger(__name__)

# ==============================================================================
# CONFIGURATION & REGISTRY
# ==============================================================================
class Config:
    BASE_DIR = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    
    # --- OCI Object Storage (The Raw Data Lake) ---
    OCI_BUCKET = os.getenv('OCI_OBJECT_STORAGE_BUCKET', 'oci-s3-ykg-storage')
    OCI_ACCESS_KEY = os.environ.get('OCI_OBJECT_STORAGE_ACCESS_KEY')
    OCI_SECRET_KEY = os.environ.get('OCI_OBJECT_STORAGE_SECRET_KEY')
    OCI_REGION = os.environ.get('OCI_REGION', 'us-ashburn-1')
    OCI_ENDPOINT = os.environ.get('OCI_ENDPOINT_URL')
    OCI_NAMESPACE = os.environ.get('OCI_OBJECT_STORAGE_NAMESPACE')

    # --- Local DuckDB Data Warehouse (Silver & Gold) ---
    DUCKDB_PATH = os.path.join(BASE_DIR, 'data', 'weather_warehouse.duckdb')

    REGION_US = {
        'top': 90.0,
        'bottom': -10.0,
        'left': -180.0,
        'right': 180.0
    }

weather_config = Config()

METEO_REGISTRY = {
    "upper": {
        "gh": {"gfs": ":HGT:500 mb:", "ecmwf": ["gh", "z"], "levelist": [500]},
        "t": {"gfs": ":TMP:850 mb:", "ecmwf": "t", "levelist": [850]},
        "u": {"gfs": ":UGRD:250 mb:", "ecmwf": "u", "levelist": [250]},
        "v": {"gfs": ":VGRD:250 mb:", "ecmwf": "v", "levelist": [250]},
    },
    "surface": {
        "2t": {"gfs": ":TMP:2 m above ground:", "ecmwf": "2t"},
        "2d": {"gfs": ":DPT:2 m above ground:", "ecmwf": "2d"},
        "msl": {"gfs": ":PRMSL:mean sea level:", "ecmwf": "msl"},
        "tp": {"gfs": ":APCP:surface:", "ecmwf": "tp"}
    }
}

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================
def _download_s3_range(url, ranges, target_path):
    """S3 Byte-Range Download Helper Function for GFS (Leaves NOAA AWS intact)"""
    try:
        with open(target_path, 'wb') as f_out:
            for start, end in ranges:
                headers = {"Range": f"bytes={start}-{end}"}
                for attempt in range(2): 
                    try:
                        with requests.get(url, headers=headers, timeout=20, stream=True) as r:
                            if r.status_code == 206:
                                f_out.write(r.content)
                                break 
                            else:
                                if attempt == 1: raise ConnectionError(f"S3 Status {r.status_code}")
                    except Exception:
                        if attempt == 1: raise
                        time.sleep(1)
        return True
    except Exception as e:
        logger.warning(f"S3 Range Download Failed: {e}")
        return False

def _apply_xarray_canonicalization(ds):
    """
    Applies native chunking and spatial normalization.
    """
    logger.info("Executing Xarray-Native Canonicalization...")
    
    chunk_dict = {"latitude": 200, "longitude": 200}
    if "step" in ds.dims: chunk_dict["step"] = 1
    if "isobaricInhPa" in ds.dims: chunk_dict["isobaricInhPa"] = 1
    ds = ds.chunk(chunk_dict, name_prefix="wx_chunk")
    
    if 'longitude' in ds.coords:
        ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
        ds = ds.sortby('longitude')
        
    if 'latitude' in ds.coords:
        if float(ds.latitude[0]) > float(ds.latitude[-1]):
            ds = ds.sortby('latitude')
        ds = ds.sel(latitude=slice(weather_config.REGION_US['bottom'], weather_config.REGION_US['top']))
        
    if not ds.data_vars:
        raise ValueError("Empty dataset after spatial crop.")
        
    return ds

def extract_to_arrow(ds, target_var):
    """
    Memory-safe Xarray → Arrow conversion (no pandas).
    Uses chunk-aware stacking and avoids dataset materialization.
    """
    if target_var and target_var in ds:
        ds = ds.dropna(dim="latitude", how="all")
        ds = ds.dropna(dim="longitude", how="all")

    spatial_dims = ["latitude", "longitude"]
    non_spatial_dims = [d for d in ds.dims if d not in spatial_dims]
    stack_dims = non_spatial_dims + spatial_dims

    ds = ds.transpose(*stack_dims)
    stacked = ds.stack(points=stack_dims).reset_index("points")

    if target_var and target_var in stacked:
        stacked = stacked.dropna("points", subset=[target_var])

    coord_arrays = {}
    for coord in stacked.coords:
        if coord == "points": continue
        val = stacked.coords[coord].data
        if hasattr(val, "compute"): val = val.compute()
        val = np.asarray(val)
        if val.ndim == 0: val = np.repeat(val, stacked.sizes["points"])
        coord_arrays[coord] = val

    data_arrays = {}
    for var in stacked.data_vars:
        arr = stacked[var].data
        if hasattr(arr, "compute"): arr = arr.compute()
        data_arrays[var] = np.asarray(arr).astype(np.float32)

    data_dict = {**coord_arrays, **data_arrays}

    lat_arr = data_dict["latitude"].astype(np.float32)
    lon_arr = data_dict["longitude"].astype(np.float32)
    data_dict["lat_i"] = np.round(lat_arr * 1000).astype(np.int32)
    data_dict["lon_i"] = np.round(lon_arr * 1000).astype(np.int32)

    if "time" in data_dict:
        frt = data_dict["time"].astype("datetime64[us]")
        del data_dict["time"]
    else:
        frt = data_dict["forecast_reference_time"].astype("datetime64[us]")

    data_dict["forecast_reference_time"] = frt

    if "step" in data_dict:
        step = data_dict["step"].astype("timedelta64[us]")
        data_dict["valid_time"] = frt + step
        data_dict["step_hours"] = step.astype("timedelta64[h]").astype(np.int32) 
        del data_dict["step"]
    else:
        data_dict["valid_time"] = frt

    data_dict["forecast_date"] = frt.astype("datetime64[D]")
    hours_since_epoch = frt.astype("datetime64[h]").astype(np.int64)
    data_dict["forecast_cycle"] = (hours_since_epoch % 24).astype(np.int32)

    table = pa.Table.from_pydict(data_dict)

    table = table.set_column(
        table.schema.get_field_index("forecast_reference_time"),
        "forecast_reference_time",
        pc.cast(table["forecast_reference_time"], pa.timestamp("us", tz="UTC")),
    )
    table = table.set_column(
        table.schema.get_field_index("valid_time"),
        "valid_time",
        pc.cast(table["valid_time"], pa.timestamp("us", tz="UTC")),
    )
    
    table = table.set_column(
        table.schema.get_field_index("forecast_date"),
        "forecast_date",
        pc.cast(pc.cast(table["forecast_date"], pa.date32()), pa.string()),
    )

    pk_cols = ["forecast_date", "forecast_cycle", "step_hours", "lat_i", "lon_i"]

    if "isobaricInhPa" in table.column_names:
        table = table.set_column(table.schema.get_field_index("isobaricInhPa"), "isobaricInhPa", pc.cast(table["isobaricInhPa"], pa.int32()))
        pk_cols.append("isobaricInhPa")

    if "level" in table.column_names:
        table = table.set_column(table.schema.get_field_index("level"), "level", pc.cast(table["level"], pa.int32()))
        pk_cols.append("level")

    if table.num_rows > 0:
        dtype_map = [(col, table[col].to_numpy().dtype) for col in pk_cols]
        structured = np.empty(table.num_rows, dtype=dtype_map)
        for col in pk_cols:
            structured[col] = table[col].to_numpy()
        _, idx = np.unique(structured, return_index=True)
        table = table.take(np.sort(idx))

    return table, pk_cols

# ==============================================================================
# RAW LAYER: DELTA LAKE ON OCI OBJECT STORAGE
# ==============================================================================

def upsert_weather_data(pa_table, pk_cols, delta_table_path, storage_options):
    """
    Idempotent Delta Lake ingestion using Dynamic Partition Overwrite.
    """
    partition_cols = ["forecast_date", "forecast_cycle"]
    
    try:
        dt = DeltaTable(delta_table_path, storage_options=storage_options)
        table_exists = True
    except Exception as e:
        if any(k in str(e).lower() for k in ["not a delta table", "not found", "no files"]):
            table_exists = False
        else:
            raise e

    if not table_exists:
        logger.warning(f"⚠️ Delta table not initialized. Creating vertically partitioned table safely...")
        write_deltalake(
            delta_table_path, 
            pa_table, 
            mode="append",  
            partition_by=partition_cols,
            storage_options=storage_options,
            configuration={
                "delta.logRetentionDuration": "interval 3 days",
                "delta.checkpointInterval": "5"                  
            }
        )
        return

    schema_obj = dt.schema() if callable(dt.schema) else dt.schema
    
    target_pa_schema = None
    if hasattr(schema_obj, "to_pyarrow"):
        target_pa_schema = schema_obj.to_pyarrow()
    elif hasattr(schema_obj, "to_arrow"):
        target_pa_schema = schema_obj.to_arrow()

    if target_pa_schema:
        target_cols = target_pa_schema.names
        
        for field in target_pa_schema:
            col_name = field.name
            target_type = field.type
            
            if not isinstance(target_type, pa.DataType):
                continue
            
            if col_name in pa_table.column_names:
                source_type = pa_table.schema.field(col_name).type
                if source_type != target_type:
                    pa_table = pa_table.set_column(
                        pa_table.schema.get_field_index(col_name),
                        col_name,
                        pc.cast(pa_table[col_name], target_type)
                    )
                    
        source_cols = pa_table.column_names
        if set(target_cols) == set(source_cols) and target_cols != source_cols:
            pa_table = pa_table.select(target_cols)
        elif set(target_cols) != set(source_cols):
            raise ValueError("Schema mismatch detected. Halting to prevent data corruption.")

    unique_dates = pa_table["forecast_date"].unique().to_pylist()
    unique_cycles = pa_table["forecast_cycle"].unique().to_pylist()

    date_list = ",".join([f"'{d}'" for d in unique_dates])
    cycle_list = ",".join([str(c) for c in unique_cycles])
    
    delete_predicate = f"forecast_date IN ({date_list}) AND CAST(forecast_cycle AS BIGINT) IN ({cycle_list})"

    logger.info(f"♻️ Executing Explicit Partition Wipe: {delete_predicate}")
    
    try:
        dt.delete(delete_predicate)
    except Exception as e:
        logger.warning(f"⚠️ Partition wipe skipped/failed (it might already be empty): {e}")

    logger.info(f"✅ Appending fresh batch to Delta Lake...")
    write_deltalake(
        delta_table_path,
        pa_table,
        mode="append",
        partition_by=partition_cols,
        storage_options=storage_options
    )
    
    try:
        logger.info(f"🧹 Vacuuming old, tombstoned files from Object Storage...")
        dt_clean = DeltaTable(delta_table_path, storage_options=storage_options)
        deleted_files = dt_clean.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
        logger.info(f"✨ Storage clean! Physically deleted: {len(deleted_files)} files.")
    except Exception as e:
        logger.warning(f"⚠️ Vacuum failed: {e}")

# ==============================================================================
# DUCKDB WAREHOUSE ENGINE (SILVER & GOLD)
# ==============================================================================

def get_duckdb_connection(max_retries=5, delay=5):
    """
    Initializes local DuckDB, loads OCI (S3-Compat) extensions, and returns connection.
    """
    os.makedirs(os.path.dirname(weather_config.DUCKDB_PATH), exist_ok=True)
    
    for attempt in range(max_retries):
        try:
            con = duckdb.connect(database=weather_config.DUCKDB_PATH)
            
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute("INSTALL aws; LOAD aws;")
            con.execute("INSTALL delta; LOAD delta;")
            
            # Secure OCI Credentials Loading (S3 Compatibility API)
            if weather_config.OCI_ACCESS_KEY and weather_config.OCI_SECRET_KEY:
                # Sanitize endpoint for DuckDB (no https://)
                clean_ep = weather_config.OCI_ENDPOINT.replace("https://", "").replace("http://", "").strip("/")
                
                con.execute(f"""
                    CREATE OR REPLACE SECRET oci_secret (
                        TYPE S3,
                        KEY_ID '{weather_config.OCI_ACCESS_KEY}',
                        SECRET '{weather_config.OCI_SECRET_KEY}',
                        REGION '{weather_config.OCI_REGION}',
                        ENDPOINT '{clean_ep}',
                        URL_STYLE 'path',
                        USE_SSL 'true'
                    );
                """)
                # Force global settings to ensure delta_scan uses the OCI path
                con.execute(f"SET s3_endpoint = '{clean_ep}';")
                con.execute("SET s3_url_style = 'path';")
                con.execute(f"SET s3_region = '{weather_config.OCI_REGION}';")
            
            return con
            
        except duckdb.IOException as e:
            if "lock on file" in str(e).lower() and attempt < max_retries - 1:
                logger.warning(f"⚠️ DuckDB locked. Retrying in {delay}s... ({attempt+1}/{max_retries})")
                time.sleep(delay)
            else:
                raise e

def build_duckdb_silver_layer(dataset_name: str, delta_oci_path: str):
    """
    Creates/Updates the Silver layer inside the local DuckDB database.
    """
    con = get_duckdb_connection()
    logger.info(f"🦆 DuckDB: Rebuilding Silver Layer for {dataset_name} from OCI Delta Lake...")
    
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        sql = f"""
            CREATE OR REPLACE TABLE silver.{dataset_name} AS 
            SELECT * FROM delta_scan('{delta_oci_path}')
            WHERE forecast_date >= CAST((CURRENT_DATE - INTERVAL 7 DAYS) AS VARCHAR);
        """
        con.execute(sql)
        logger.info(f"✅ Silver Layer Updated: silver.{dataset_name} stored locally.")
        
    except Exception as e:
        logger.error(f"❌ DuckDB Silver Build Failed: {e}")
    finally:
        con.close()

# ==============================================================================
# DATA EXTRACTORS
# ==============================================================================

def download_gfs_robust(date_obj, cycle, steps):
    """GFS Downloader (GRIB2 -> Xarray -> PyArrow -> Delta Lake on OCI Object Storage)"""
    
    if not isinstance(steps, (list, tuple)):
        steps = [steps]
        
    date_str = pendulum.instance(date_obj).format("YYYYMMDD")
    cycle_str = f"{cycle:02d}"
    
    temp_dir = os.path.join(weather_config.BASE_DIR, "Data", "Temp_GFS_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    
    arrow_tables = []
    master_pk_cols = None
    all_success = True
    
    for step in steps:
        temp_filename = f"TEMP_gfs_{date_str}_{cycle_str}z_{step}h.grib2"
        temp_path = os.path.join(temp_dir, temp_filename)

        download_success = False
        # Note: We still download the PUBLIC RAW data from NOAA's AWS S3
        s3_base = f"https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{date_str}/{cycle_str}/atmos/gfs.t{cycle_str}z.pgrb2.0p25.f{str(step).zfill(3)}"
        
        try:
            r_idx = requests.get(s3_base + ".idx", timeout=10)
            if r_idx.status_code == 200:
                lines = r_idx.text.splitlines()
                target_vars = [v["gfs"] for k, v in METEO_REGISTRY["upper"].items()] + \
                              [v["gfs"] for k, v in METEO_REGISTRY["surface"].items()]
                
                fcst_marker = f"{int(step)} hour fcst" if step > 0 else "anl"
                
                ranges = []
                for key in target_vars:
                    for i, line in enumerate(lines):
                        if key in line and fcst_marker in line:
                            parts = line.split(':')
                            start = int(parts[1])
                            end = int(lines[i+1].split(':')[1])-1 if i+1 < len(lines) else ""
                            ranges.append((start, end))
                            continue
                            
                if len(ranges) > 0: 
                    if _download_s3_range(s3_base, ranges, temp_path):
                        download_success = True
        except Exception:
            pass

        if not download_success:
            nomads_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
            params = {
                'file': f'gfs.t{cycle_str}z.pgrb2.0p25.f{str(step).zfill(3)}',
                'subregion': 'on',
                'toplat': weather_config.REGION_US['top'],
                'bottomlat': weather_config.REGION_US['bottom'],
                'leftlon': weather_config.REGION_US['left'],
                'rightlon': weather_config.REGION_US['right'],
                'dir': f'/gfs.{date_str}/{cycle_str}/atmos'
            }
            
            for level_type, vars_dict in METEO_REGISTRY.items():
                for var_key, var_info in vars_dict.items():
                    gfs_str = var_info['gfs'] 
                    parts = [p for p in gfs_str.split(':') if p]
                    if len(parts) >= 2:
                        var_name = parts[0]
                        level_name = parts[1].replace(' ', '_')
                        params[f'var_{var_name}'] = 'on'
                        if any(k in level_name.lower() for k in ['surface', 'mean_sea_level', 'above_ground']):
                            params['lev_surface'] = 'on'
                            params[f'lev_{level_name}'] = 'on' 
                        else:
                            params[f'lev_{level_name}'] = 'on'

            try:
                r = requests.get(nomads_url, params=params, timeout=60)
                if r.status_code == 200:
                    with open(temp_path, 'wb') as f:
                        f.write(r.content)
                    download_success = True
            except Exception:
                pass

        if download_success and os.path.exists(temp_path):
            ds = None
            try:
                datasets = cfgrib.open_datasets(temp_path, backend_kwargs={'indexpath': '', 'filter_by_keys': {'typeOfLevel': 'isobaricInhPa'}})
                if not datasets: raise ValueError("No valid grids found in GRIB file.")
                    
                clean_datasets = []
                for ds_part in datasets:
                    if 'isobaricInhPa' in ds_part.coords and ds_part['isobaricInhPa'].ndim == 0:
                        ds_part = ds_part.expand_dims('isobaricInhPa')
                    clean_datasets.append(ds_part)
                
                ds = xr.merge(clean_datasets) if len(clean_datasets) > 1 else clean_datasets[0]
                ds = _apply_xarray_canonicalization(ds)
                target_var = 'gh' if 'gh' in ds.data_vars else ('z' if 'z' in ds.data_vars else None)
                pa_table, pk_cols = extract_to_arrow(ds, target_var)
                arrow_tables.append(pa_table)
                master_pk_cols = pk_cols
            except Exception as e:
                logger.error(f"❌ [GFS ETL Error] {e}"); all_success = False
            finally:
                if ds is not None: ds.close()
                if os.path.exists(temp_path): os.remove(temp_path)
        else:
            all_success = False

    if arrow_tables and master_pk_cols:
        try:
            logger.info(f"📦 Batching GFS steps into OCI Delta transaction...")
            master_table = pa.concat_tables(arrow_tables)
            delta_table_oci_path = f"s3://{weather_config.OCI_BUCKET}/weather_data/delta_lake/gfs_raw/"
            
            # Storage options configured for OCI S3-Compatibility
            storage_options = {
                "AWS_ACCESS_KEY_ID": weather_config.OCI_ACCESS_KEY,
                "AWS_SECRET_ACCESS_KEY": weather_config.OCI_SECRET_KEY,
                "AWS_REGION": weather_config.OCI_REGION,
                "AWS_ENDPOINT_URL": weather_config.OCI_ENDPOINT,
                "AWS_S3_ADDRESSING_STYLE": "path"
            }
            upsert_weather_data(master_table, master_pk_cols, delta_table_oci_path, storage_options)
            build_duckdb_silver_layer("gfs_raw", delta_table_oci_path)
        except Exception as e:
            logger.error(f"❌ Batch Upsert Failed: {e}"); all_success = False
            
    return all_success

def download_ecmwf_unified(date_obj, cycle, steps, target_model='aifs', task_type='upper'): 
    """ECMWF Unified Downloader (GRIB2 -> Xarray -> PyArrow -> Delta Lake on OCI Object Storage)"""
    
    client = Client("ecmwf", beta=False)
    date_str = pendulum.instance(date_obj).format("YYYYMMDD")
    cycle_str = f"{cycle:02d}z"
    if not isinstance(steps, (list, tuple)): steps = [steps]
        
    temp_dir = os.path.join(weather_config.BASE_DIR, "Data", "Temp_Global_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    
    model_type = target_model.upper()
    arrow_tables, master_pk_cols, task_name, all_success = [], None, None, True

    for step in steps:
        task_dict = None
        if model_type == 'AIFS':
            common_params = {"class": "od", "stream": "oper", "type": "fc", "model": "aifs-single", "step": step}
            file_prefix = "at_aifs"
        elif model_type == 'IFS':
            common_params = {"class": "od", "stream": "oper", "type": "fc", "levtype": "pl", "step": step}
            file_prefix = "at_ifs"   
        else: return False

        if task_type == 'spread':
            task_dict = {"name": f"{model_type.lower()}_spread", "temp_name": f"TEMP_{model_type}_{step}h.grib2", 
                         "params": {"class": "od", "stream": "enfo", "type": "es", "levtype": "pl", "levelist": [500, 850], "param": ['z', 't'], "step": step}}
        elif task_type == 'upper':
            short_names = []
            for p in [v["ecmwf"] for k, v in METEO_REGISTRY["upper"].items()]:
                short_names.extend(p) if isinstance(p, list) else short_names.append(p)
            task_dict = {"name": f"{file_prefix}_upper", "temp_name": f"TEMP_{file_prefix}_upper_{step}h.grib2",
                         "params": {**common_params, "levtype": "pl", "levelist": [850, 500, 250], "param": list(set(short_names))}}
        elif task_type == 'surface':
            task_dict = {"name": f"{file_prefix}_surface", "temp_name": f"TEMP_{file_prefix}_surf_{step}h.grib2",
                         "params": {**common_params, "levtype": "sfc", "param": [v["ecmwf"] for k, v in METEO_REGISTRY["surface"].items()]}}

        if task_dict:
            task_name, temp_path, download_ok = task_dict['name'], os.path.join(temp_dir, task_dict['temp_name']), False
            for attempt in range(3):
                try:
                    client.retrieve({"date": date_str, "time": f"{cycle:02d}", "target": temp_path, **task_dict['params']})
                    if os.path.exists(temp_path) and os.path.getsize(temp_path) > 1024: download_ok = True; break
                except Exception: time.sleep(2 ** (attempt + 1))

            if not download_ok: all_success = False; continue

            ds = None
            try:
                datasets = cfgrib.open_datasets(temp_path, backend_kwargs={'indexpath': ''})
                clean_datasets = []
                for ds_part in datasets:
                    if 'isobaricInhPa' in ds_part.coords and ds_part['isobaricInhPa'].ndim == 0:
                        ds_part = ds_part.expand_dims('isobaricInhPa')
                    clean_datasets.append(ds_part)
                ds = xr.merge(clean_datasets) if len(clean_datasets) > 1 else clean_datasets[0]
                ds = _apply_xarray_canonicalization(ds)
                target_var = 'z' if 'z' in ds.data_vars else ('msl' if 'msl' in ds.data_vars else None)
                pa_table, pk_cols = extract_to_arrow(ds, target_var)
                arrow_tables.append(pa_table); master_pk_cols = pk_cols
            except Exception: all_success = False
            finally:
                if ds: ds.close()
                if os.path.exists(temp_path): os.remove(temp_path)
        
    if arrow_tables and master_pk_cols and task_name:
        try:
            master_table = pa.concat_tables(arrow_tables)
            delta_table_oci_path = f"s3://{weather_config.OCI_BUCKET}/weather_data/delta_lake/ecmwf_raw/{task_name}/"
            storage_options = {
                "AWS_ACCESS_KEY_ID": weather_config.OCI_ACCESS_KEY,
                "AWS_SECRET_ACCESS_KEY": weather_config.OCI_SECRET_KEY,
                "AWS_REGION": weather_config.OCI_REGION,
                "AWS_ENDPOINT_URL": weather_config.OCI_ENDPOINT,
                "AWS_S3_ADDRESSING_STYLE": "path"
            }
            upsert_weather_data(master_table, master_pk_cols, delta_table_oci_path, storage_options)
            build_duckdb_silver_layer(task_name, delta_table_oci_path)
        except Exception: all_success = False
            
    return all_success

# ==============================================================================
# DOWNSTREAM PROCESSING FUNCTIONS (Local Compute Analytics / Legacy)
# ==============================================================================

def process_z500(ds, tag="Z500"):
    if ds is None: return None
    logger.info(f"[PROCESS][{tag}] Start processing...")

    var_name = next((v for v in ['z', 'gh', 'hgt'] if v in ds), None)
    if not var_name: return None
    
    try:
        da_raw = ds[var_name]
        if 'isobaricInhPa' in da_raw.dims: z_raw = da_raw.sel(isobaricInhPa=500)
        elif 'level' in da_raw.dims: z_raw = da_raw.sel(level=500)
        else: z_raw = da_raw
    except: return None

    z_val = ensure_2d(z_raw, tag=f"{tag}_Step3")
    try:
        mean_val = np.nanmean(z_val.values)
        if np.isnan(mean_val): return None
        if mean_val > 40000: return z_val / 9.80665 / 10.0
        elif mean_val > 4000: return z_val / 10.0
        else: return z_val
    except: return None

def get_val_safe(da, lon, lat):
    if da is None: return 0.0
    try: return float(da.sel(longitude=lon, latitude=lat, method='nearest'))
    except KeyError:
        try:
            lon_query = lon + 360 if lon < 0 else (lon - 360 if lon > 180 else lon)
            return float(da.sel(longitude=lon_query, latitude=lat, method='nearest'))
        except: return 0.0
    except: return 0.0

def ensure_2d(da, tag="Unknown"):
    if da is None: return None
    try: da = da.squeeze()
    except: pass
    while da.ndim > 2:
        dims = list(da.dims)
        target_dim = next((d for d in dims if 'lat' not in str(d).lower() and 'lon' not in str(d).lower()), dims[0])
        da = da.isel({target_dim: -1})
    return da

def force_2d(da):
    try: return da.squeeze()
    except: return da

def load_data(file_path):
    pass

# ==============================================================================
# DBT ORCHESTRATION VIA DUCKDB (Pivoted to OCI)
# ==============================================================================
def run_dbt_duckdb(command: str, select_path: str = None):
    """
    Standardized runner using dbt's native Python API.
    Bypasses subprocess and docker exec completely.
    """    
    try:
        # 1. Inject OCI credentials securely into the current Python environment.
        # Your profiles.yml will automatically pick these up via {{ env_var(...) }}
        os.environ["OCI_ACCESS_KEY"] = weather_config.OCI_ACCESS_KEY
        os.environ["OCI_SECRET_KEY"] = weather_config.OCI_SECRET_KEY
        os.environ["OCI_REGION"] = weather_config.OCI_REGION
        os.environ["OCI_ENDPOINT_URL"] = weather_config.OCI_ENDPOINT
        os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
        
        # 2. Build the argument list (exactly as you would type it in the CLI)
        dbt_cli_args = [
            command,
            "--project-dir", "/opt/airflow/physical_meteor", # Absolute path to dbt_project.yml
            "--profiles-dir", "/opt/airflow/physical_meteor", # Absolute path to profiles.yml
            "--target", "dev_duckdb"
        ]
        
        if select_path:
            dbt_cli_args.extend(["--select", select_path])
            
        logger.info(f"Executing dbt natively via Python API: dbt {' '.join(dbt_cli_args)}")
        
        # 3. Initialize and invoke the dbt runner
        dbt = dbtRunner()
        result: dbtRunnerResult = dbt.invoke(dbt_cli_args)
        
        # 4. Check the results programmatically
        if result.success:
            logger.info("✅ dbt completed successfully.")
            # You can even loop through result.result to get stats per model
        else:
            # If success is False, we check if it was a compilation error, runtime error, etc.
            logger.error("❌ DBT Error. The models failed to execute.")
            if result.exception:
                logger.error(f"Exception details: {result.exception}")
            raise Exception("dbt DuckDB command failed.")
            
    except Exception as e:
        logger.error(f"❌ General Error during dbt execution: {e}")
        raise