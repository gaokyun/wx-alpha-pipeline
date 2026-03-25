import os
import time
import logging
import requests
import xarray as xr
import numpy as np
import cfgrib
import pendulum
import docker
import pyarrow as pa
import pyarrow.compute as pc
from ecmwf.opendata import Client
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

# Configure logger
logger = logging.getLogger(__name__)

# ==============================================================================
# CONFIGURATION & REGISTRY
# ==============================================================================
class Config:
    BASE_DIR = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    S3_BUCKET = os.environ.get('AWS_S3_BUCKET', 'amzn-s3-ykg-storage')
    AWS_ACC_KEY = os.environ.get('AWS_ACC_KEY')
    AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
    AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

    REGION_US = {
        'top': 90.0,
        'bottom': -10.0,
        'left': -180.0,
        'right': 180.0
    }

weather_config = Config()

METEO_REGISTRY = {
    "upper": {
        "gh": {"gfs": ":HGT:500 mb:", "ecmwf": "gh", "levelist": [500]},
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
    """S3 Byte-Range Download Helper Function for GFS"""
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
    
    # 1. Advanced Chunking (Preventing stack explosions)
    chunk_dict = {"latitude": 200, "longitude": 200}
    if "step" in ds.dims: chunk_dict["step"] = 1
    if "isobaricInhPa" in ds.dims: chunk_dict["isobaricInhPa"] = 1
    ds = ds.chunk(chunk_dict, name_prefix="wx_chunk")
    
    # 2. Robust Longitude Normalization
    if 'longitude' in ds.coords:
        ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
        ds = ds.sortby('longitude')
        
    # 3. Spatial Crop via Registry Config
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

    # --- deterministic dimension ordering ---
    spatial_dims = ["latitude", "longitude"]
    non_spatial_dims = [d for d in ds.dims if d not in spatial_dims]
    stack_dims = non_spatial_dims + spatial_dims

    # chunk-safe stacking
    ds = ds.transpose(*stack_dims)
    stacked = ds.stack(points=stack_dims)
    stacked = stacked.reset_index("points")

    if target_var and target_var in stacked:
        stacked = stacked.dropna("points", subset=[target_var])

    # --- coordinate extraction ---
    coord_arrays = {}

    for coord in stacked.coords:
        if coord == "points":
            continue

        val = stacked.coords[coord].data

        # Resolve dask scalars safely
        if hasattr(val, "compute"):
            val = val.compute()

        val = np.asarray(val)

        if val.ndim == 0:
            val = np.repeat(val, stacked.sizes["points"])

        coord_arrays[coord] = val

        data_arrays = {}

        for var in stacked.data_vars:

            arr = stacked[var].data

            if hasattr(arr, "compute"):
                arr = arr.compute()

            data_arrays[var] = np.asarray(arr).astype(np.float32)

    data_dict = {**coord_arrays, **data_arrays}

    # --- spatial index compression ---
    lat_arr = data_dict["latitude"].astype(np.float32)
    lon_arr = data_dict["longitude"].astype(np.float32)

    _, lat_idx = np.unique(lat_arr, return_inverse=True)
    _, lon_idx = np.unique(lon_arr, return_inverse=True)

    data_dict["lat_i"] = lat_idx.astype(np.int32)
    data_dict["lon_i"] = lon_idx.astype(np.int32)

    # --- time normalization ---
    if "time" in data_dict:
        frt = data_dict["time"].astype("datetime64[ms]")
        del data_dict["time"]
    else:
        frt = data_dict["forecast_reference_time"].astype("datetime64[ms]")

    data_dict["forecast_reference_time"] = frt

    if "step" in data_dict:
        step = data_dict["step"].astype("timedelta64[ms]")
        data_dict["valid_time"] = frt + step
        data_dict["step_hours"] = step.astype("timedelta64[h]").astype(np.int16)
        del data_dict["step"]
    else:
        data_dict["valid_time"] = frt

    data_dict["forecast_date"] = frt.astype("datetime64[D]")

    hours_since_epoch = frt.astype("datetime64[h]").astype(np.int64)
    data_dict["forecast_cycle"] = (hours_since_epoch % 24).astype(np.int16)

    # --- Arrow conversion ---
    table = pa.Table.from_pydict(data_dict)

    # --- timezone enforcement ---
    table = table.set_column(
        table.schema.get_field_index("forecast_reference_time"),
        "forecast_reference_time",
        pc.cast(table["forecast_reference_time"], pa.timestamp("ms", tz="UTC")),
    )

    table = table.set_column(
        table.schema.get_field_index("valid_time"),
        "valid_time",
        pc.cast(table["valid_time"], pa.timestamp("ms", tz="UTC")),
    )

    table = table.set_column(
        table.schema.get_field_index("forecast_date"),
        "forecast_date",
        pc.cast(table["forecast_date"], pa.date32()),
    )

    # --- primary key construction ---
    pk_cols = [
        "forecast_date",
        "forecast_cycle",
        "forecast_reference_time",
        "valid_time",
        "lat_i",
        "lon_i",
    ]

    if "isobaricInhPa" in table.column_names:
        pk_cols.append("isobaricInhPa")

    if "level" in table.column_names:
        pk_cols.append("level")

    # --- deduplication ---
    if table.num_rows > 0:

        dtype_map = [
            (col, table[col].to_numpy().dtype)
            for col in pk_cols
        ]

        structured = np.empty(table.num_rows, dtype=dtype_map)

        for col in pk_cols:
            structured[col] = table[col].to_numpy()

        _, idx = np.unique(structured, return_index=True)

        table = table.take(np.sort(idx))

    return table, pk_cols

def upsert_weather_data(pa_table, pk_cols, delta_table_s3_path, storage_options):
    """
    Idempotent Delta Lake merge using strict integer grids and pyarrow tables.
    Prunes partitions vertically and horizontally directly in the predicate.
    """
    predicate = " AND ".join([f"s.{col} = t.{col}" for col in pk_cols])
    
    partition_cols = ["forecast_date", "forecast_cycle"]
    if "isobaricInhPa" in pa_table.column_names:
        partition_cols.append("isobaricInhPa")
    elif "level" in pa_table.column_names:
        partition_cols.append("level")
    
    try:
        dt = DeltaTable(delta_table_s3_path, storage_options=storage_options)

        # --- VERSION-PROOF SCHEMA ALIGNER ---
        schema_obj = dt.schema() if callable(dt.schema) else dt.schema
        
        if hasattr(schema_obj, "to_pyarrow"):
            target_cols = schema_obj.to_pyarrow().names
        elif hasattr(schema_obj, "to_arrow"):
            target_cols = schema_obj.to_arrow().names
        elif hasattr(schema_obj, "fields"):
            target_cols = [f.name for f in schema_obj.fields]
        else:
            target_cols = [getattr(f, "name", str(f)) for f in schema_obj] 
            
        source_cols = pa_table.column_names

        if set(target_cols) == set(source_cols) and target_cols != source_cols:
            logger.info("🔁 Reordering source Arrow table columns to match Delta target schema order")
            pa_table = pa_table.select(target_cols)
        elif set(target_cols) != set(source_cols):
            missing_fields = set(target_cols) - set(source_cols)
            extra_fields = set(source_cols) - set(target_cols)
            logger.error(f"❌ Source fields differ from Delta target fields (missing={missing_fields}, extra={extra_fields})")
            raise ValueError("Schema mismatch detected. Halting to prevent data corruption.")

        # --- SIMPLIFIED MERGE EXECUTION ---
        (
            dt.merge(
                source=pa_table,
                predicate=predicate,
                source_alias="s",
                target_alias="t"
            )
            .when_not_matched_insert_all()
            .when_matched_update_all() 
            .execute()
        )
        logger.info(f"✅ Arrow-Native Upsert successful (Partition Pruned) via keys: {pk_cols}")
        
    except Exception as e:
        error_str = str(e).lower()
        
        # 1. If the table is missing entirely
        if any(k in error_str for k in ["not a delta table", "not found", "no table found", "no files in log segment"]):
            logger.warning(f"⚠️ Delta table not initialized. Creating vertically partitioned table safely...")
            write_deltalake(
                delta_table_s3_path, 
                pa_table, 
                mode="append",  
                schema_mode="merge", 
                partition_by=partition_cols,
                storage_options=storage_options
            )
            
        # 2. If the schema order is scrambled
        elif any(k in error_str for k in ["field names are not matching", "target schema", "schema mismatch", "does not exist in schema", "lat_i"]):
            logger.warning(f"⚠️ Delta table schema mismatch detected ({e}). Forcing schema overwrite...")
            write_deltalake(
                delta_table_s3_path,
                pa_table,
                mode="overwrite",
                schema_mode="overwrite",
                partition_by=partition_cols,
                storage_options=storage_options
            )
            
        # 3. True failures
        else:
            logger.error(f"❌ Upsert failed: {e}")
            raise e
                
# ==============================================================================
# DATA EXTRACTORS (UPDATED FOR MEMORY BATCHING)
# ==============================================================================

def download_gfs_robust(date_obj, cycle, steps):
    """GFS Downloader (GRIB2 -> Xarray -> PyArrow -> Delta Lake)"""
    
    # Ensure steps is an iterable list for batching
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
                        if 'surface' in level_name.lower() or 'mean_sea_level' in level_name.lower() or 'above_ground' in level_name.lower():
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
                if not datasets:
                    raise ValueError("No valid grids found in GRIB file.")
                    
                clean_datasets = []
                for ds_part in datasets:
                    if 'isobaricInhPa' in ds_part.coords and ds_part['isobaricInhPa'].ndim == 0:
                        ds_part = ds_part.expand_dims('isobaricInhPa')
                    clean_datasets.append(ds_part)
                
                ds = xr.merge(clean_datasets) if len(clean_datasets) > 1 else clean_datasets[0]
                
                ds = _apply_xarray_canonicalization(ds)
                target_var = 'gh' if 'gh' in ds.data_vars else ('z' if 'z' in ds.data_vars else None)
                
                pa_table, pk_cols = extract_to_arrow(ds, target_var)
                
                # STORE IN RAM INSTEAD OF WRITING
                arrow_tables.append(pa_table)
                master_pk_cols = pk_cols
                
            except Exception as e:
                logger.error(f"❌ [GFS ETL Error at step {step}] {e}")
                all_success = False
                
            finally:
                if ds is not None: ds.close()
                if os.path.exists(temp_path):
                    try: os.remove(temp_path)
                    except OSError: pass
        else:
            logger.warning(f"⚠️ GFS download failed for step {step}")
            all_success = False

    # ---------------------------------------------------------
    # BATCH WRITE TO DELTA LAKE (Executes exactly once)
    # ---------------------------------------------------------
    if arrow_tables and master_pk_cols:
        try:
            logger.info(f"📦 Batching {len(arrow_tables)} GFS steps into a single Delta transaction...")
            master_table = pa.concat_tables(arrow_tables)
            
            delta_table_s3_path = f"s3://{weather_config.S3_BUCKET}/weather_data/delta_lake/gfs_raw/"
            storage_options = {
                "AWS_ACCESS_KEY_ID": weather_config.AWS_ACC_KEY,
                "AWS_SECRET_ACCESS_KEY": weather_config.AWS_SECRET_KEY,
                "AWS_REGION": weather_config.AWS_REGION
            }
            
            upsert_weather_data(master_table, master_pk_cols, delta_table_s3_path, storage_options)
        except Exception as e:
            logger.error(f"❌ Batch Upsert Failed: {e}")
            all_success = False
            
    return all_success


def download_ecmwf_unified(date_obj, cycle, steps, target_model='aifs', task_type='upper'): 
    """ECMWF Unified Downloader (GRIB2 -> Xarray -> PyArrow -> Delta Lake)"""
    
    client = Client("ecmwf", beta=False)
    date_str = pendulum.instance(date_obj).format("YYYYMMDD")
    cycle_str = f"{cycle:02d}z"
    
    # Ensure steps is an iterable list for batching
    if not isinstance(steps, (list, tuple)):
        steps = [steps]
        
    temp_dir = os.path.join(weather_config.BASE_DIR, "Data", "Temp_Global_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    
    model_type = target_model.upper()
    
    arrow_tables = []
    master_pk_cols = None
    task_name = None
    all_success = True

    for step in steps:
        task_dict = None

        if model_type == 'AIFS':
            common_params = {"class": "od", "stream": "oper", "type": "fc", "model": "aifs-single", "step": step}
            file_prefix = "at_aifs"
        elif model_type == 'IFS':
            common_params = {"class": "od", "stream": "oper", "type": "fc", "levtype": "pl", "step": step}
            file_prefix = "at_ifs"   
        else:
            logger.error(f"❌ Unknown target_model: {target_model}")
            return False

        if task_type == 'spread':
            if model_type == 'AIFS':
                task_dict = {
                    "name": "aifs_spread",
                    "temp_name": f"TEMP_GLOBAL_aifs_eps_{date_str}_{cycle_str}_{step}h.grib2",
                    "params": {"class": "od", "stream": "enfo", "type": "es", "levtype": "pl", 
                               "levelist": [500, 850], "param": ['z', 't'], "step": step}
                }
            elif model_type == 'IFS':
                task_dict = {
                    "name": "ifs_spread",
                    "temp_name": f"TEMP_GLOBAL_ifs_eps_{date_str}_{cycle_str}_{step}h.grib2",
                    "params": {"class": "od", "stream": "enfo", "type": "es", "levtype": "pl", 
                               "levelist": [500, 850], "param": ['z', 't'], "step": step}
                }
                
        elif task_type == 'upper':
            short_names_upper = [v["ecmwf"] for k, v in METEO_REGISTRY["upper"].items()]
            task_dict = {
                "name": f"{file_prefix}_upper",
                "temp_name": f"TEMP_GLOBAL_{file_prefix}_upper_{date_str}_{cycle_str}_{step}h.grib2",
                "params": {**common_params, "levtype": "pl", "levelist": [850, 500, 250], "param": short_names_upper}
            }
            
        elif task_type == 'surface':
            short_names_surf = [v["ecmwf"] for k, v in METEO_REGISTRY["surface"].items()]
            task_dict = {
                "name": f"{file_prefix}_surface",
                "temp_name": f"TEMP_GLOBAL_{file_prefix}_surface_{date_str}_{cycle_str}_{step}h.grib2",
                "params": {**common_params, "levtype": "sfc", "param": short_names_surf}
            }
            
        else:
            logger.error(f"❌ Unknown task_type: {task_type}")
            return False

        if task_dict is not None:
            task_name = task_dict['name'] # Save for the batch write
            temp_path = os.path.join(temp_dir, task_dict['temp_name'])
            download_ok = False
            
            for attempt in range(3):
                try:
                    client.retrieve({"date": date_str, "time": f"{cycle:02d}", "target": temp_path, **task_dict['params']})
                    if os.path.exists(temp_path) and os.path.getsize(temp_path) > 1024:
                        download_ok = True
                        break
                except Exception as e:
                    logger.warning(f"⚠️ Network error on attempt {attempt+1}: {e}")
                    time.sleep(2 ** (attempt + 1))
                    if os.path.exists(temp_path):
                        try: os.remove(temp_path)
                        except: pass
            
            if not download_ok:
                logger.error(f"❌ Final failure after 3 attempts: {task_dict['temp_name']}")
                all_success = False
                continue

            ds = None
            try:
                datasets = cfgrib.open_datasets(temp_path, backend_kwargs={'indexpath': ''})
                if not datasets:
                    raise ValueError("No valid grids found in ECMWF GRIB file.")
                    
                clean_datasets = []
                for ds_part in datasets:
                    if 'isobaricInhPa' in ds_part.coords and ds_part['isobaricInhPa'].ndim == 0:
                        ds_part = ds_part.expand_dims('isobaricInhPa')
                    clean_datasets.append(ds_part)
                
                ds = xr.merge(clean_datasets) if len(clean_datasets) > 1 else clean_datasets[0]
                
                ds = _apply_xarray_canonicalization(ds)
                target_var = 'z' if 'z' in ds.data_vars else ('msl' if 'msl' in ds.data_vars else None)
                
                pa_table, pk_cols = extract_to_arrow(ds, target_var)
                
                # STORE IN RAM INSTEAD OF WRITING
                arrow_tables.append(pa_table)
                master_pk_cols = pk_cols
                
            except Exception as e:
                logger.warning(f"⚠️ ECMWF ETL Error for {task_dict['name']} at step {step}: {e}")
                all_success = False
                
            finally:
                if ds is not None: ds.close()
                if os.path.exists(temp_path):
                    try: os.remove(temp_path)
                    except OSError: pass
        
    # ---------------------------------------------------------
    # BATCH WRITE TO DELTA LAKE (Executes exactly once)
    # ---------------------------------------------------------
    if arrow_tables and master_pk_cols and task_name:
        try:
            logger.info(f"📦 Batching {len(arrow_tables)} ECMWF steps into a single Delta transaction...")
            master_table = pa.concat_tables(arrow_tables)
            
            delta_table_s3_path = f"s3://{weather_config.S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/{task_name}/"
            storage_options = {
                "AWS_ACCESS_KEY_ID": weather_config.AWS_ACC_KEY,
                "AWS_SECRET_ACCESS_KEY": weather_config.AWS_SECRET_KEY,
                "AWS_REGION": weather_config.AWS_REGION
            }
            
            upsert_weather_data(master_table, master_pk_cols, delta_table_s3_path, storage_options)
        except Exception as e:
            logger.error(f"❌ Batch Upsert Failed: {e}")
            all_success = False
            
    return all_success

# ==============================================================================
# DOWNSTREAM PROCESSING FUNCTIONS (Local Compute Analytics / Legacy)
# ==============================================================================

def process_z500(ds, tag="Z500"):
    if ds is None: return None
    logger.info(f"[PROCESS][{tag}] Start processing...")

    var_name = next((v for v in ['z', 'gh', 'hgt'] if v in ds), None)
    logger.info(f"-> Variable Name Identified: {var_name}")
    if not var_name: 
        logger.error(f"-> Error: Variable not found! Existing variables: {list(ds.data_vars)}")
        return None
    
    try:
        da_raw = ds[var_name]
        logger.info(f"-> Raw DataArray dimensions: {da_raw.dims}, Shape: {da_raw.shape}")
        
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
# DBT ORCHESTRATION VIA DOCKER
# ==============================================================================
def run_dbt_command(command: str, select_path: str = None):
    """
    Standardized runner to execute dbt commands in the 'dbt-snowflake-runner' container.
    """
    client = docker.from_env()
    try:
        container = client.containers.get('dbt-snowflake-runner')
        
        full_cmd = f"dbt {command} --profiles-dir . --target prod"
        if select_path:
            full_cmd += f" --select {select_path}"
            
        print(f"Executing: {full_cmd}")
        
        exit_code, (stdout, stderr) = container.exec_run(
            cmd=f'bash -c "{full_cmd}"',
            workdir='/usr/app/physical_meteor',
            demux=True
        )
        
        if stdout:
            print(stdout.decode('utf-8'))
        if stderr:
            print(f"STDERR: {stderr.decode('utf-8')}")
            
        if exit_code != 0:
            raise Exception(f"dbt command failed: {full_cmd}")
            
    except docker.errors.NotFound:
        raise Exception("Container 'dbt-snowflake-runner' not found.")