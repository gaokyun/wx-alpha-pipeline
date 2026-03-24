import os
import time
import logging
import requests
import xarray as xr
import numpy as np
import cfgrib
import pendulum
import boto3
import docker
import pyarrow as pa
from botocore.exceptions import ClientError
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

# Consolidates variables across GFS and ECMWF models
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
def crop_to_nh_safe(ds):
    """
    Northern Hemisphere Full Panorama Cropper
    """
    try:
        if 'longitude' in ds.coords:
            ds.coords['longitude'] = (ds.coords['longitude'] + 180) % 360 - 180
            ds = ds.sortby('longitude')
        
        ds = ds.sortby('latitude')

        if 'latitude' in ds.coords:
            ds_nh = ds.sel(latitude=slice(-10, 90))
        else:
            logger.warning("Warning: No 'latitude' coord found, returning full ds.")
            return ds

        if ds_nh.latitude.size == 0 or ds_nh.longitude.size == 0:
            logger.error("Error: Crop resulted in empty dataset. Check source dimensions.")
            return None

        return ds_nh

    except Exception as e:
        logger.error(f"Crop Error: {e}")
        return None    

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

def upsert_weather_data(df, delta_table_s3_path, storage_options):
    """
    Idempotent write operation for weather data. 
    Prevents duplicates during Airflow retries or backfills.
    """
    pa_table = pa.Table.from_pandas(df)
    
    try:
        dt = DeltaTable(delta_table_s3_path, storage_options=storage_options)
        
        (
            dt.merge(
                source=pa_table,
                predicate="s.time = t.time AND s.step = t.step AND s.latitude = t.latitude AND s.longitude = t.longitude",
                source_alias="s",
                target_alias="t"
            )
            .when_not_matched_insert_all() 
            .when_matched_update_all()     
            .execute()
        )
        logger.info(f"✅ Idempotent Upsert successful for {delta_table_s3_path}")
        
    except Exception as e:
        if "not a delta table" in str(e).lower() or "not found" in str(e).lower() or "no table found" in str(e).lower():
            logger.warning(f"⚠️ Delta table not found at {delta_table_s3_path}. Initializing new table...")
            write_deltalake(
                delta_table_s3_path, 
                df, 
                mode="append", 
                schema_mode="merge",
                storage_options=storage_options
            )
        else:
            logger.error(f"❌ Upsert failed for {delta_table_s3_path}: {e}")
            raise e


# ==============================================================================
# DATA EXTRACTORS
# ==============================================================================

def download_gfs_robust(date_obj, cycle, step):
    """GFS Downloader (GRIB2 -> Delta Lake)"""
    
    date_str = pendulum.instance(date_obj).format("YYYYMMDD")
    cycle_str = f"{cycle:02d}"
    
    temp_dir = os.path.join(weather_config.BASE_DIR, "Data", "Temp_GFS_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    temp_filename = f"TEMP_gfs_{date_str}_{cycle_str}z_{step}h.grib2"
    temp_path = os.path.join(temp_dir, temp_filename)

    download_success = False

    s3_base = f"https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{date_str}/{cycle_str}/atmos/gfs.t{cycle_str}z.pgrb2.0p25.f{str(step).zfill(3)}"
    
    # 1. Try S3 Byte Range Extraction
    try:
        r_idx = requests.get(s3_base + ".idx", timeout=10)
        if r_idx.status_code == 200:
            lines = r_idx.text.splitlines()
            
            target_vars = [v["gfs"] for k, v in METEO_REGISTRY["upper"].items()] + \
                          [v["gfs"] for k, v in METEO_REGISTRY["surface"].items()]
            
            ranges = []
            for key in target_vars:
                for i, line in enumerate(lines):
                    if key in line:
                        parts = line.split(':')
                        start = int(parts[1])
                        end = int(lines[i+1].split(':')[1])-1 if i+1 < len(lines) else ""
                        ranges.append((start, end))
                        break
            if len(ranges) == len(target_vars):
                if _download_s3_range(s3_base, ranges, temp_path):
                    download_success = True
    except Exception:
        pass

    # 2. Fallback to NOMADS Perl Filter
    if not download_success:
        nomads_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
        params = {
            'file': f'gfs.t{cycle_str}z.pgrb2.0p25.f{str(step).zfill(3)}',
            'lev_850_mb': 'on', 'var_TMP': 'on', 
            'lev_500_mb': 'on', 'var_HGT': 'on',
            'var_ABSV': 'on',
            'subregion': 'on',
            'toplat': weather_config.REGION_US['top'],
            'bottomlat': weather_config.REGION_US['bottom'],
            'leftlon': weather_config.REGION_US['left'],
            'rightlon': weather_config.REGION_US['right'],
            'dir': f'/gfs.{date_str}/{cycle_str}/atmos'
        }
        try:
            r = requests.get(nomads_url, params=params, timeout=60)
            if r.status_code == 200:
                with open(temp_path, 'wb') as f:
                    f.write(r.content)
                download_success = True
        except Exception:
            pass

    # 3. Transform & Load into Delta Lake
    if download_success and os.path.exists(temp_path):
        try:
            datasets = cfgrib.open_datasets(
                temp_path, 
                backend_kwargs={'indexpath': '', 'filter_by_keys': {'typeOfLevel': 'isobaricInhPa'}}
            )
            
            clean_datasets = []
            for ds_part in datasets:
                if 'isobaricInhPa' in ds_part.coords:
                    if ds_part['isobaricInhPa'].ndim == 0:
                        ds_part = ds_part.expand_dims('isobaricInhPa')
                clean_datasets.append(ds_part)
            
            ds = xr.merge(clean_datasets) if len(clean_datasets) > 1 else clean_datasets[0]
            ds_nh = crop_to_nh_safe(ds)
            
            if ds_nh is not None:
                logger.info("Flattening GFS grid to Pandas DataFrame...")
                df = ds_nh.to_dataframe().reset_index()

                target_var = 'gh' if 'gh' in df.columns else ('z' if 'z' in df.columns else None)
                if target_var:
                    df = df.dropna(subset=[target_var])
                
                df.columns = [str(c) for c in df.columns]
                
                for col in df.select_dtypes(include=['timedelta64[ns]', 'timedelta64']).columns:
                    df[col] = df[col].dt.total_seconds() / 3600.0

                delta_table_s3_path = f"s3://{weather_config.S3_BUCKET}/weather_data/delta_lake/gfs_raw/"
                
                storage_options = {
                    "AWS_ACCESS_KEY_ID": weather_config.AWS_ACC_KEY,
                    "AWS_SECRET_ACCESS_KEY": weather_config.AWS_SECRET_KEY,
                    "AWS_REGION": weather_config.AWS_REGION
                }
                
                # Execute Idempotent Upsert
                upsert_weather_data(df, delta_table_s3_path, storage_options)
                
                ds.close()
                ds_nh.close()
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return True
            else:
                ds.close()
                return False
        except Exception as e:
            logger.error(f"❌ [GFS ETL Error] {e}")
            if os.path.exists(temp_path): os.remove(temp_path)
            return False
    return False


def download_ecmwf_unified(date_obj, cycle, step, target_model='aifs', task_type='upper'): 
    """ECMWF Unified Downloader (GRIB2 -> Delta Lake)"""
    
    client = Client("ecmwf", beta=False)
    date_str = pendulum.instance(date_obj).format("YYYYMMDD")
    cycle_str = f"{cycle:02d}z"
    
    temp_dir = os.path.join("/opt/airflow", "Data", "Temp_Global_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    
    model_type = target_model.upper()
    task_dict = None

    # 1. Base parameters setup
    if model_type == 'AIFS':
        common_params = {"class": "od", "stream": "oper", "type": "fc", "model": "aifs-single", "step": step}
        file_prefix = "at_aifs"
    elif model_type == 'IFS':
        common_params = {"class": "od", "stream": "oper", "type": "fc", "levtype": "pl", "step": step}
        file_prefix = "at_ifs"   
    else:
        logger.error(f"❌ Unknown target_model: {target_model}")
        return False

    # 2. Specific Task Dictionary Construction
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
    
    # 3. Execution Block
    if task_dict is not None:
        temp_path = os.path.join(temp_dir, task_dict['temp_name'])
        download_ok = False
        
        for attempt in range(3):
            try:
                client.retrieve({"date": date_str, "time": cycle, "target": temp_path, **task_dict['params']})
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

        try:
            ds = xr.open_dataset(temp_path, engine='cfgrib', backend_kwargs={'indexpath': ''})
            ds_nh = crop_to_nh_safe(ds)
            
            if ds_nh is not None:
                logger.info(f"Flattening {task_dict['name']} grid to Pandas DataFrame...")
                df = ds_nh.to_dataframe().reset_index()

                target_var = 'z' if 'z' in df.columns else ('msl' if 'msl' in df.columns else None)
                if target_var and target_var in df.columns:
                    df = df.dropna(subset=[target_var])
                
                df.columns = [str(c) for c in df.columns]
                
                # Convert timedelta/Duration columns (like 'step') into Float Hours
                for col in df.select_dtypes(include=['timedelta64[ns]', 'timedelta64']).columns:
                    df[col] = df[col].dt.total_seconds() / 3600.0
                
                delta_table_s3_path = f"s3://{weather_config.S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/{task_dict['name']}/"
                
                storage_options = {
                    "AWS_ACCESS_KEY_ID": weather_config.AWS_ACC_KEY,
                    "AWS_SECRET_ACCESS_KEY": weather_config.AWS_SECRET_KEY,
                    "AWS_REGION": weather_config.AWS_REGION
                }
                
                # Execute Idempotent Upsert
                upsert_weather_data(df, delta_table_s3_path, storage_options)
                
                ds.close()
                ds_nh.close()
                os.remove(temp_path)
            else:
                logger.error(f"❌ Crop failed (Empty result): {task_dict['temp_name']}")
                all_success = False
                ds.close()
                
        except Exception as e:
            logger.warning(f"⚠️ ECMWF ETL Error for {task_dict['name']}: {e}")
            all_success = False
            if os.path.exists(temp_path): 
                try: os.remove(temp_path)
                except: pass
    
    # Final Cleanup
    try:
        for f in os.listdir(temp_dir):
            if f.startswith("TEMP_GLOBAL"): os.remove(os.path.join(temp_dir, f))
    except: pass
            
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
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    try:
        container = client.containers.get('dbt-snowflake-runner')
        
        full_cmd = f"dbt {command} --profiles-dir . --target prod"
        if select_path:
            full_cmd += f" --select {select_path}"
            
        print(f"Executing: {full_cmd}")
        
        exit_code, output = container.exec_run(
            cmd=f'bash -c "{full_cmd}"',
            workdir='/usr/app/physical_meteor'
        )
        
        print(output.decode('utf-8'))
        if exit_code != 0:
            raise Exception(f"dbt command failed: {full_cmd}")
            
    except docker.errors.NotFound:
        raise Exception("Container 'dbt-snowflake-runner' not found. Ensure it is part of your docker-compose.")