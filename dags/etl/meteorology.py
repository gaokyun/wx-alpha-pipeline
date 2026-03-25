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
    ds = ds.chunk(chunk_dict)
    
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
    100% Pandas-Free memory conversion.
    Stacks non-spatial profiles first, filters NaNs, applies Arrow schemas,
    and returns a PyArrow table ready for Delta Lake.
    """
    # 1. Optimize Stacking Order (Non-Spatial -> Spatial)
    non_spatial_dims = [d for d in ds.dims if d not in ["latitude", "longitude"]]
    if non_spatial_dims:
        ds = ds.stack(profile=non_spatial_dims)
        ds_stacked = ds.stack(points=["latitude", "longitude", "profile"])
    else:
        ds_stacked = ds.stack(points=["latitude", "longitude"])
    
    # 2. Native NaN filtering
    if target_var and target_var in ds_stacked.data_vars:
        ds_stacked = ds_stacked.dropna(dim="points", subset=[target_var])
        
    # 3. Extract raw numpy arrays
    data_dict = {
        **{str(k): v.values for k, v in ds_stacked.coords.items() if str(k) not in ["points", "profile"]},
        **{str(k): v.values for k, v in ds_stacked.data_vars.items()}
    }
    
    # 4. Pure Numpy Pre-computation
    lat_arr = data_dict['latitude'].astype(np.float32)
    lon_arr = data_dict['longitude'].astype(np.float32)
    data_dict['lat_i'] = (lat_arr * 10000).astype(np.int32)
    data_dict['lon_i'] = (lon_arr * 10000).astype(np.int32)
    data_dict['latitude'] = lat_arr
    data_dict['longitude'] = lon_arr

    # Temporal Math via Numpy Datetime64
    if 'time' in data_dict:
        frt_arr = data_dict['time'].astype('datetime64[ms]')
        data_dict['forecast_reference_time'] = frt_arr
        del data_dict['time']
    elif 'forecast_reference_time' in data_dict:
        frt_arr = data_dict['forecast_reference_time'].astype('datetime64[ms]')
        data_dict['forecast_reference_time'] = frt_arr

    if 'step' in data_dict:
        step_arr = data_dict['step'].astype('timedelta64[ms]')
        data_dict['valid_time'] = frt_arr + step_arr
        data_dict['step_hours'] = data_dict['step'].astype('timedelta64[h]').astype(np.int16)
        del data_dict['step']
    elif 'valid_time' in data_dict:
        data_dict['valid_time'] = data_dict['valid_time'].astype('datetime64[ms]')
    else:
        data_dict['valid_time'] = frt_arr

    # Extract Partition Columns natively (Numpy & PyArrow Compute)
    data_dict['forecast_date'] = frt_arr.astype('datetime64[D]')
    time_pa_array = pa.array(frt_arr, type=pa.timestamp('ms'))
    data_dict['forecast_cycle'] = pc.hour(time_pa_array).to_numpy(c_contiguous=True).astype(np.int32)

    # 5. Build PyArrow Table
    table = pa.Table.from_pydict(data_dict)
    
    # 6. Apply Timezone casting (Reducing ns to ms to save parquet space)
    for t_col in ['forecast_reference_time', 'valid_time']:
        idx = table.schema.get_field_index(t_col)
        table = table.set_column(idx, t_col, pc.cast(table[t_col], pa.timestamp('ms', tz='UTC')))
        
    date_idx = table.schema.get_field_index('forecast_date')
    table = table.set_column(date_idx, 'forecast_date', pc.cast(table['forecast_date'], pa.date32()))

    # 7. Define Immutable Schema & Primary Keys
    pk_cols = ['forecast_date', 'forecast_cycle', 'forecast_reference_time', 'valid_time', 'lat_i', 'lon_i']
    fields = [
        pa.field("forecast_date", pa.date32()),
        pa.field("forecast_cycle", pa.int32()),
        pa.field("forecast_reference_time", pa.timestamp('ms', tz='UTC')),
        pa.field("valid_time", pa.timestamp('ms', tz='UTC')),
        pa.field("latitude", pa.float32()),
        pa.field("longitude", pa.float32()),
        pa.field("lat_i", pa.int32()),
        pa.field("lon_i", pa.int32()),
    ]
    
    if 'step_hours' in table.column_names:
        fields.append(pa.field("step_hours", pa.int16()))
    if 'isobaricInhPa' in table.column_names:
        pk_cols.append('isobaricInhPa')
        fields.append(pa.field("isobaricInhPa", pa.float32()))
        table = table.set_column(table.schema.get_field_index('isobaricInhPa'), 'isobaricInhPa', pc.cast(table['isobaricInhPa'], pa.float32()))
    elif 'level' in table.column_names:
        pk_cols.append('level')
        fields.append(pa.field("level", pa.float32()))
        table = table.set_column(table.schema.get_field_index('level'), 'level', pc.cast(table['level'], pa.float32()))
        
    for var in ds.data_vars:
        fields.append(pa.field(str(var), pa.float32()))
        table = table.set_column(table.schema.get_field_index(str(var)), str(var), pc.cast(table[str(var)], pa.float32()))

    # 8. Pure Numpy Deduplication (Zero Pandas)
    if table.num_rows > 0:
        dtype_list = [(col, table[col].to_numpy().dtype) for col in pk_cols]
        struct_arr = np.empty(table.num_rows, dtype=dtype_list)
        for col in pk_cols:
            struct_arr[col] = table[col].to_numpy()
            
        _, unique_indices = np.unique(struct_arr, return_index=True)
        # Filter table natively via Arrow zero-copy take
        table = table.take(np.sort(unique_indices))

    # Lock Schema
    schema = pa.schema(fields)
    table = table.cast(schema)
    
    return table, pk_cols


def upsert_weather_data(pa_table, pk_cols, delta_table_s3_path, storage_options):
    """
    Idempotent Delta Lake merge using strict integer grids and pyarrow tables.
    Prunes partitions vertically and horizontally directly in the predicate.
    """
    # Incorporate partition pruning directly into the merge predicate for exponential speedup
    predicate = " AND ".join([f"s.{col} = t.{col}" for col in pk_cols])
    
    # Elite-Tier Vertical & Horizontal Partitioning
    partition_cols = ["forecast_date", "forecast_cycle"]
    if "isobaricInhPa" in pa_table.column_names:
        partition_cols.append("isobaricInhPa")
    elif "level" in pa_table.column_names:
        partition_cols.append("level")
    
    # Build dynamic update condition to prevent rewriting identical parquet blocks
    data_vars = [f.name for f in pa_table.schema if f.name not in pk_cols and f.name not in ['latitude', 'longitude', 'step_hours']]
    update_condition = " OR ".join([f"s.{v} != t.{v}" for v in data_vars]) if data_vars else None
    
    try:
        dt = DeltaTable(delta_table_s3_path, storage_options=storage_options)
        
        if not dt.metadata().partition_columns:
             raise RuntimeError(f"Delta table lacks partitioning. Manual wipe required: {delta_table_s3_path}")
        
        merge_op = (
            dt.merge(
                source=pa_table,
                predicate=predicate,
                source_alias="s",
                target_alias="t"
            )
            .when_not_matched_insert_all() 
        )
        
        # Apply conditional rewrite logic
        if update_condition:
            merge_op = merge_op.when_matched_update_all(condition=update_condition)
        else:
            merge_op = merge_op.when_matched_update_all()
            
        merge_op.execute()
        logger.info(f"✅ Arrow-Native Upsert successful (Partition Pruned) via keys: {pk_cols}")
        
    except Exception as e:
        error_str = str(e).lower()
        if any(k in error_str for k in ["not a delta table", "not found", "no table found", "no files in log segment"]):
            logger.warning(f"⚠️ Delta table not initialized. Creating vertically partitioned table...")
            write_deltalake(
                delta_table_s3_path, 
                pa_table, 
                mode="append", 
                schema_mode="merge",
                partition_by=partition_cols,
                storage_options=storage_options
            )
        else:
            logger.error(f"❌ Upsert failed: {e}")
            raise e

# ==============================================================================
# DATA EXTRACTORS
# ==============================================================================

def download_gfs_robust(date_obj, cycle, step):
    """GFS Downloader (GRIB2 -> Xarray -> PyArrow -> Delta Lake)"""
    
    date_str = pendulum.instance(date_obj).format("YYYYMMDD")
    cycle_str = f"{cycle:02d}"
    
    temp_dir = os.path.join(weather_config.BASE_DIR, "Data", "Temp_GFS_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
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

            delta_table_s3_path = f"s3://{weather_config.S3_BUCKET}/weather_data/delta_lake/gfs_raw/"
            storage_options = {
                "AWS_ACCESS_KEY_ID": weather_config.AWS_ACC_KEY,
                "AWS_SECRET_ACCESS_KEY": weather_config.AWS_SECRET_KEY,
                "AWS_REGION": weather_config.AWS_REGION
            }
            
            upsert_weather_data(pa_table, pk_cols, delta_table_s3_path, storage_options)
            return True
                
        except Exception as e:
            logger.error(f"❌ [GFS ETL Error] {e}")
            return False
            
        finally:
            if ds is not None: ds.close()
            if os.path.exists(temp_path):
                try: os.remove(temp_path)
                except OSError: pass
                
    return False


def download_ecmwf_unified(date_obj, cycle, step, target_model='aifs', task_type='upper'): 
    """ECMWF Unified Downloader (GRIB2 -> Xarray -> PyArrow -> Delta Lake)"""
    
    client = Client("ecmwf", beta=False)
    date_str = pendulum.instance(date_obj).format("YYYYMMDD")
    cycle_str = f"{cycle:02d}z"
    
    temp_dir = os.path.join(weather_config.BASE_DIR, "Data", "Temp_Global_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    
    model_type = target_model.upper()
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

    all_success = True
    
    if task_dict is not None:
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
            return False

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
            
            delta_table_s3_path = f"s3://{weather_config.S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/{task_dict['name']}/"
            storage_options = {
                "AWS_ACCESS_KEY_ID": weather_config.AWS_ACC_KEY,
                "AWS_SECRET_ACCESS_KEY": weather_config.AWS_SECRET_KEY,
                "AWS_REGION": weather_config.AWS_REGION
            }
            
            upsert_weather_data(pa_table, pk_cols, delta_table_s3_path, storage_options)
            
        except Exception as e:
            logger.warning(f"⚠️ ECMWF ETL Error for {task_dict['name']}: {e}")
            all_success = False
            
        finally:
            if ds is not None: ds.close()
            if os.path.exists(temp_path):
                try: os.remove(temp_path)
                except OSError: pass
            
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