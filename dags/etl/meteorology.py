import os
import time
import requests
import xarray as xr
import numpy as np
import cfgrib
import pendulum
import logging
from ecmwf.opendata import Client
import boto3
from botocore.exceptions import ClientError
from deltalake.writer import write_deltalake

# Configure logger
logger = logging.getLogger(__name__)

class Config:
    BASE_DIR = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    S3_BUCKET = os.environ.get('AWS_S3_BUCKET')
    AWS_ACC_KEY = os.environ.get('AWS_ACC_KEY')
    AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
    AWS_REGION = os.environ.get('AWS_REGION')

    REGION_US = {
        'top': 90.0,
        'bottom': -10.0,
        'left': -180.0,
        'right': 180.0
    }
weather_config = Config()

# --- METEOROLOGICAL PARAMETER MAPPING ---
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

def upload_to_s3_and_cleanup(local_file_path, s3_prefix):
    """
    Physical Delivery: Pushes the local NetCDF file to S3 dynamically and destroys the local copy.
    (Note: Kept for legacy fallback, but bypassed by Delta Lake logic)
    """
    s3_client = boto3.client('s3',
                             aws_access_key_id=weather_config.AWS_ACC_KEY,
                             aws_secret_access_key=weather_config.AWS_SECRET_KEY,
                             region_name=weather_config.AWS_REGION)
    
    file_name = os.path.basename(local_file_path)

    AWS_ACC_KEY = weather_config.AWS_ACC_KEY
    AWS_SECRET_KEY = weather_config.AWS_SECRET_KEY
    AWS_REGION = weather_config.AWS_REGION
    S3_BUCKET = weather_config.S3_BUCKET

    logger.info(f"Config Initialized: S3_BUCKET={S3_BUCKET},AWS_REGION={AWS_REGION}, AWS_ACC_KEY={'***' if AWS_ACC_KEY else 'NOT SET'}, AWS_SECRET_KEY={'**HIDDEN**' if AWS_SECRET_KEY else 'NOT SET'}")

    target_bucket = weather_config.S3_BUCKET
    logger.info(f"******* Uploading {file_name} to s3://{target_bucket}/{s3_prefix}...")

    if not target_bucket or target_bucket is None:
        logger.error("🛑 CRITICAL: AWS_S3_BUCKET environment variable is missing!")
        return False

    s3_key = f"{s3_prefix}/{file_name}"
    
    try:
        s3_client.upload_file(local_file_path, target_bucket, s3_key)
        logger.info(f"✅ S3 Upload Complete. Destroying local buffer: {local_file_path}")   
        os.remove(local_file_path)
        return True
    except ClientError as e:
        logger.error(f"❌ S3 Upload Failed: {e}")
        return False
        
def is_future_model_run(date_obj, cycle, model_type="GFS"):
    """
    Physical Boundary Check: Prevents the pipeline from hunting for 'Ghost Data'.
    Safely integrates Pendulum for timezone-aware evaluation.
    """
    try:
        # Wrap the incoming date_obj (often a standard datetime from Airflow) into a Pendulum instance
        dt = pendulum.instance(date_obj) if not isinstance(date_obj, pendulum.DateTime) else date_obj
        run_time = dt.at(cycle, 0, 0).set(tz="UTC")
        current_utc = pendulum.now("UTC")
        
        buffer_hours = 6.5 if model_type == "ECMWF" else 3.5
        release_time = run_time.add(hours=buffer_hours)

        if release_time > current_utc:
            return True
        return False
    except Exception as e:
        logger.error(f"Time validation error: {e}")
        return False

def crop_to_nh_safe(ds):
    """
    [Northern Hemisphere Full Panorama Cropper - Final Version]
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
    """S3 Byte-Range Download Helper Function"""
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

def download_gfs_robust(date_obj, cycle, step):
    """GFS Downloader (GRIB2 -> Delta Lake)"""
    
    if is_future_model_run(date_obj, cycle, model_type="GFS"):
        logger.warning(f"🛑 [GFS ETL blocked] Run {pendulum.instance(date_obj).format('YYYYMMDD')}_{cycle:02d}z is in the future. Aborting to prevent 404 loops.")
        return False
        
    date_str = pendulum.instance(date_obj).format("YYYYMMDD")
    cycle_str = f"{cycle:02d}"
    
    temp_dir = os.path.join(weather_config.BASE_DIR, "Data", "Temp_GFS_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    temp_filename = f"TEMP_gfs_{date_str}_{cycle_str}z_{step}h.grib2"
    temp_path = os.path.join(temp_dir, temp_filename)

    download_success = False

    s3_base = f"https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{date_str}/{cycle_str}/atmos/gfs.t{cycle_str}z.pgrb2.0p25.f{str(step).zfill(3)}"
    if not download_success:
        try:
            r_idx = requests.get(s3_base + ".idx", timeout=10)
            if r_idx.status_code == 200:
                lines = r_idx.text.splitlines()
                
                # Fetch target params directly from the dynamic registry
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

    if not download_success:
        nomads_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
        # Maintaining NOMADS fallback as requested, hardcoded params remain for backwards compatibility
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
                # ---------------------------------------------------------
                # PATH C: NATIVE DELTA LAKE TRANSACTION (GFS)
                # ---------------------------------------------------------
                logger.info("Flattening GFS grid to Pandas DataFrame...")
                df = ds_nh.to_dataframe().reset_index()

                # Drop NaN to compress payload
                target_var = 'gh' if 'gh' in df.columns else ('z' if 'z' in df.columns else None)
                if target_var:
                    df = df.dropna(subset=[target_var])
                
                # Coerce columns to strings for Delta Lake schema compatibility
                df.columns = [str(c) for c in df.columns]
                
                # Convert timedelta/Duration columns into Float Hours
                for col in df.select_dtypes(include=['timedelta64[ns]', 'timedelta64']).columns:
                    df[col] = df[col].dt.total_seconds() / 3600.0

                # ---------------------------------------------------------
                # ✅ SENIOR FIX: Aligned with Airflow Dataset 'gfs_raw'
                # ---------------------------------------------------------
                delta_table_s3_path = f"s3://{weather_config.S3_BUCKET}/weather_data/delta_lake/gfs_raw/"
                logger.info(f"Writing ACID transaction to Delta Table: {delta_table_s3_path}")
                
                storage_options = {
                    "AWS_ACCESS_KEY_ID": weather_config.AWS_ACC_KEY,
                    "AWS_SECRET_ACCESS_KEY": weather_config.AWS_SECRET_KEY,
                    "AWS_REGION": weather_config.AWS_REGION
                }
                
                write_deltalake(
                    delta_table_s3_path,
                    df,
                    mode="append",
                    storage_options=storage_options
                )

                logger.info("✅ [GFS-DELTA] Transaction committed successfully.")
                
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

def download_ecmwf_unified(date_obj, cycle, step, target_models=['AIFS', 'IFS'], task_type=["upper", 'surface', 'spread']):
    """ECMWF Unified Downloader (GRIB2 -> Delta Lake)"""
    
    if is_future_model_run(date_obj, cycle, model_type="ECMWF"):
        logger.warning(f"🛑 [ECMWF ETL blocked] Run {pendulum.instance(date_obj).format('YYYYMMDD')}_{cycle:02d}z is in the future. Aborting request.")
        return False
        
    client = Client("ecmwf", beta=False)
    date_str = pendulum.instance(date_obj).format("YYYYMMDD")
    cycle_str = f"{cycle:02d}z"
    
    temp_dir = os.path.join("/opt/airflow", "Data", "Temp_Global_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    
    tasks = []

    for model_type in target_models:
        if model_type == 'AIFS':
            common_params = {"class": "od", "stream": "oper", "type": "fc", "model": "aifs-single", "step": step}
            file_prefix = "at_aifs"
        elif model_type == 'IFS':
            common_params = {"class": "od", "stream": "oper", "type": "fc", "levtype": "pl", "step": step}
            file_prefix = "at_ifs"   
        elif model_type == 'EPS' and cycle in [0, 12] and 'spread' in task_type:
            tasks.append({
                "name": "eps_spread",
                "temp_name": f"TEMP_GLOBAL_eps_{date_str}_{cycle_str}_{step}h.grib2",
                "params": {"class": "od", "stream": "enfo", "type": "es", "levtype": "pl", 
                           "levelist": [500, 850], "param": ['z', 't'], "step": step}
            })
            continue
        else:
            continue

        if 'upper' in task_type:
            # Map dynamic upper params
            short_names_upper = [v["ecmwf"] for k, v in METEO_REGISTRY["upper"].items()]
            tasks.append({
                "name": f"{file_prefix}_upper",
                "temp_name": f"TEMP_GLOBAL_{file_prefix}_upper_{date_str}_{cycle_str}_{step}h.grib2",
                "params": {**common_params, "levtype": "pl", "levelist": [850, 500, 250], "param": short_names_upper}
            })

        if 'surface' in task_type:
            # Map dynamic surface params
            short_names_surf = [v["ecmwf"] for k, v in METEO_REGISTRY["surface"].items()]
            tasks.append({
                "name": f"{file_prefix}_surface",
                "temp_name": f"TEMP_GLOBAL_{file_prefix}_surface_{date_str}_{cycle_str}_{step}h.grib2",
                "params": {**common_params, "levtype": "sfc", "param": short_names_surf}
            })

    all_success = True
    
    for task in tasks:
        temp_path = os.path.join(temp_dir, task['temp_name'])
        download_ok = False
        
        for attempt in range(3):
            try:
                client.retrieve({"date": date_str, "time": cycle, "target": temp_path, **task['params']})
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
            logger.error(f"❌ Final failure after 3 attempts: {task['temp_name']}")
            all_success = False
            continue

        try:
            ds = xr.open_dataset(temp_path, engine='cfgrib', backend_kwargs={'indexpath': ''})
            ds_nh = crop_to_nh_safe(ds)
            
            if ds_nh is not None:
                # ---------------------------------------------------------
                # PATH C: NATIVE DELTA LAKE TRANSACTION (ECMWF)
                # ---------------------------------------------------------
                logger.info(f"Flattening {task['name']} grid to Pandas DataFrame...")
                df = ds_nh.to_dataframe().reset_index()

                # Clean NaNs dynamically based on likely variable names
                target_var = 'z' if 'z' in df.columns else ('msl' if 'msl' in df.columns else None)
                if target_var and target_var in df.columns:
                    df = df.dropna(subset=[target_var])
                
                df.columns = [str(c) for c in df.columns]
                # ---------------------------------------------------------
                # NEW: Delta Lake Schema Sanitization
                # Convert timedelta/Duration columns (like 'step') into Float Hours
                # ---------------------------------------------------------
                for col in df.select_dtypes(include=['timedelta64[ns]', 'timedelta64']).columns:
                    df[col] = df[col].dt.total_seconds() / 3600.0
                
                # Dynamically construct the Delta table root based on task name
                # ---------------------------------------------------------
                # ✅ SENIOR FIX: Aligned with Airflow Dataset 'ecmwf_raw'
                # Appends task['name'] to isolate schemas (e.g., /ecmwf_raw/at_ifs_upper/)
                # ---------------------------------------------------------
                delta_table_s3_path = f"s3://{weather_config.S3_BUCKET}/weather_data/delta_lake/ecmwf_raw/{task['name']}/"
                logger.info(f"Writing ACID transaction to Delta Table: {delta_table_s3_path}")

                storage_options = {
                    "AWS_ACCESS_KEY_ID": weather_config.AWS_ACC_KEY,
                    "AWS_SECRET_ACCESS_KEY": weather_config.AWS_SECRET_KEY,
                    "AWS_REGION": weather_config.AWS_REGION
                }
                
                write_deltalake(
                    delta_table_s3_path,
                    df,
                    mode="append",
                    storage_options=storage_options #,
                    # engine="rust"
                )

                logger.info(f"✅ [ECMWF-DELTA] Transaction committed for {task['name']}.")
                
                ds.close()
                ds_nh.close()
                os.remove(temp_path)
            else:
                logger.error(f"❌ Crop failed (Empty result): {task['temp_name']}")
                all_success = False
                ds.close()
                
        except Exception as e:
            logger.warning(f"⚠️ ECMWF ETL Error for {task['name']}: {e}")
            all_success = False
            if os.path.exists(temp_path): 
                try: os.remove(temp_path)
                except: pass
    
    try:
        for f in os.listdir(temp_dir):
            if f.startswith("TEMP_GLOBAL"): os.remove(os.path.join(temp_dir, f))
    except: pass
            
    return all_success

# ================= Processing Functions (Unchanged, kept for downstream local compute) =================
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
    # Kept intact to preserve your local NetCDF workflow if you need to bypass Delta/S3 later
    pass