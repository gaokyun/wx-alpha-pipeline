import os
import time
import requests
import xarray as xr
import numpy as np
import cfgrib
from datetime import datetime, timedelta
import logging
from ecmwf.opendata import Client
import boto3
from botocore.exceptions import ClientError
from deltalake.writer import write_deltalake

# Configure logger
logger = logging.getLogger(__name__)

# Dummy config object for structure - replace with actual in usage
class Config:
    BASE_DIR = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    # S3_BUCKET = 'amzn-s3-ykg-storage' # 
    S3_BUCKET = os.environ.get('AWS_S3_BUCKET')
    AWS_ACC_KEY=os.environ.get('AWS_ACC_KEY')
    AWS_SECRET_KEY=os.environ.get('AWS_SECRET_KEY')
    # AWS_REGION='us-east-1' #os.environ.get('AWS_REGION')
    AWS_REGION = os.environ.get('AWS_REGION')

    REGION_US = {
        'top': 90.0,
        'bottom': -10.0,
        'left': -180.0,
        'right': 180.0
    }
weather_config = Config()

def upload_to_s3_and_cleanup(local_file_path, s3_prefix):
    """
    Physical Delivery: Pushes the local NetCDF file to S3 dynamically and destroys the local copy.
    """
    # TRUTH CHECK: boto3 automatically detects AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY 
    # in the OS environment. You do not need to pass them explicitly here.
    s3_client = boto3.client('s3',
                            aws_access_key_id=weather_config.AWS_ACC_KEY,
                            aws_secret_access_key=weather_config.AWS_SECRET_KEY,
                            region_name=weather_config.AWS_REGION)
    
    # Fetch the dynamic bucket name
    file_name = os.path.basename(local_file_path)

    AWS_ACC_KEY=weather_config.AWS_ACC_KEY
    AWS_SECRET_KEY=weather_config.AWS_SECRET_KEY
    AWS_REGION=weather_config.AWS_REGION
    S3_BUCKET=weather_config.S3_BUCKET

    logger.info(f"Config Initialized: S3_BUCKET={S3_BUCKET},AWS_REGION={AWS_REGION}, AWS_ACC_KEY={'***' if AWS_ACC_KEY else 'NOT SET'}, AWS_SECRET_KEY={'**HIDDEN**' if AWS_SECRET_KEY else 'NOT SET'}")

    target_bucket = weather_config.S3_BUCKET
    logger.info(f"******* Uploading {file_name} to s3://{target_bucket}/{s3_prefix}...")

    if not target_bucket or target_bucket is None:
        logger.error("🛑 CRITICAL: AWS_S3_BUCKET environment variable is missing!")
        return False

    file_name = os.path.basename(local_file_path)
    s3_key = f"{s3_prefix}/{file_name}"
    
    try:
        # logger.info(f"Uploading {file_name} to s3://{target_bucket}/{s3_key}...")
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
    Returns True if the requested model run time is ahead of the current UTC time.
    """
    try:
        # Construct the exact UTC time the model cycle is theoretically initiated
        run_time = datetime(date_obj.year, date_obj.month, date_obj.day, cycle, 0, 0)
        current_utc = datetime.utcnow()
        
        # Add a 2-hour buffer (Optional, but highly recommended). 
        # Models are never instantly available at their cycle time. 
        # For example, the 00z GFS is not physically uploaded to AWS until ~03:30z.
        # GFS takes ~3.5 hours. ECMWF takes ~6.5 hours.
        buffer_hours = 6.5 if model_type == "ECMWF" else 3.5
        release_time = run_time + timedelta(hours=buffer_hours)

        if release_time > current_utc:
            return True
        return False
    except Exception as e:
        logger.error(f"Time validation error: {e}")
        return False

def crop_to_nh_safe(ds):
    """
    [Northern Hemisphere Full Panorama Cropper - Final Version]
    Target: Lat [-10, 90], Lon [-180, 180] (Full Zonal Circle)
    Features:
    1. Auto-fix Longitude: Convert 0~360 to -180~180 (Fixes Cartopy white line issue)
    2. Auto-fix Latitude: Force ascending order (Resolves empty slices from 90->-90)
    3. Retain Equator Buffer: Crop to -10 degrees to prevent flow field disruption
    """
    try:
        # --- 1. Longitude Standardization ---
        if 'longitude' in ds.coords:
            ds.coords['longitude'] = (ds.coords['longitude'] + 180) % 360 - 180
            ds = ds.sortby('longitude')
        
        # --- 2. Latitude Standardization ---
        ds = ds.sortby('latitude')

        # --- 3. Safe Crop ---
        if 'latitude' in ds.coords:
            ds_nh = ds.sel(latitude=slice(-10, 90))
        else:
            logger.warning("Warning: No 'latitude' coord found, returning full ds.")
            return ds

        # --- 4. Final Audit ---
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
    """GFS Downloader (GRIB2 -> NetCDF)"""
    
    # ---------------------------------------------------------
    # STRUCTURAL CHECK: Prevent downloading future runs
    # ---------------------------------------------------------
    if is_future_model_run(date_obj, cycle, model_type="GFS"):
        logger.warning(f"🛑 [GFS ETL blocked] Run {date_obj.strftime('%Y%m%d')}_{cycle:02d}z is in the future. Aborting to prevent 404 loops.")
        return False
        
    date_str = date_obj.strftime("%Y%m%d")
    cycle_str = f"{cycle:02d}"
    
    save_dir = os.path.join(weather_config.BASE_DIR, "Data", "Data Sources", "GFS", date_str, 'GFS', f'{cycle_str}z')
    os.makedirs(save_dir, exist_ok=True)
    temp_dir = os.path.join(weather_config.BASE_DIR, "Data", "Temp_GFS_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    
    temp_filename = f"TEMP_gfs_{date_str}_{cycle_str}z_{step}h.grib2"
    temp_path = os.path.join(temp_dir, temp_filename)
    final_filename = f"at_gfs_upper_{date_str}_{cycle_str}z_{step}h_nh.nc"
    final_path = os.path.join(save_dir, final_filename)

    if os.path.exists(final_path):       
        logger.info(f"✅ [GFS-NC] Already exists, uploaded to S3: {final_path}")
        return True

    download_success = False

    s3_base = f"https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{date_str}/{cycle_str}/atmos/gfs.t{cycle_str}z.pgrb2.0p25.f{str(step).zfill(3)}"
    if not download_success:
        try:
            r_idx = requests.get(s3_base + ".idx", timeout=10)
            if r_idx.status_code == 200:
                lines = r_idx.text.splitlines()
                target_vars = [':HGT:500 mb:', ':TMP:850 mb:', ':ABSV:500 mb:'] 
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
            ds_us = crop_to_nh_safe(ds)
            
            if ds_us is not None:
                comp = dict(zlib=True, complevel=5)
                encoding = {var: comp for var in ds_us.data_vars}
                ds_us.to_netcdf(final_path, engine='netcdf4', encoding=encoding)

                ds.close()
                ds_us.close()
                if os.path.exists(temp_path):
                    os.remove(temp_path) # Remove the raw GRIB2
                
                # NEW S3 LOGIC HERE:
                s3_folder_path = f"weather_data/gfs/{date_str}/{cycle:02d}z"
                upload_to_s3_and_cleanup(final_path, s3_folder_path)
                
                if os.path.getsize(final_path) / 1024 > 100:
                    logger.info(f"✅ [GFS-NC] Saved & Cropped: {final_filename}")
                    return True
                else:
                    logger.error(f"❌ [GFS-NC] File too small: {final_filename}")
                    return False
            else:
                ds.close()
                return False
        except Exception as e:
            logger.error(f"❌ [GFS ETL Error] {e}")
            if os.path.exists(temp_path): os.remove(temp_path)
            return False
    return False

def download_ecmwf_unified(date_obj, cycle, step, target_models=['AIFS', 'IFS'], task_type=["upper", 'surface', 'spread']):
    """ECMWF Unified Downloader"""
    
    # ---------------------------------------------------------
    # STRUCTURAL CHECK: Prevent downloading future runs
    # ---------------------------------------------------------
    if is_future_model_run(date_obj, cycle, model_type="ECMWF"):
        logger.warning(f"🛑 [ECMWF ETL blocked] Run {date_obj.strftime('%Y%m%d')}_{cycle:02d}z is in the future. Aborting request.")
        return False
        
    client = Client("ecmwf", beta=False)
    date_str = date_obj.strftime("%Y%m%d")
    cycle_str = f"{cycle:02d}z"
    
    ecmwf_root = os.path.join(weather_config.BASE_DIR, "Data", "Data Sources", "ECMWF", date_str)
    
    # Notice: Keeping the local 'x:\temp' folder mapping consistent with Airflow paths.
    temp_dir = os.path.join("/opt/airflow", "Data", "Temp_Global_Buffer")
    os.makedirs(temp_dir, exist_ok=True)
    
    tasks = []

    for model_type in target_models:
        model_save_dir = os.path.join(ecmwf_root, model_type, cycle_str)
        os.makedirs(model_save_dir, exist_ok=True)

        if model_type == 'AIFS':
            common_params = {"class": "od", "stream": "oper", "type": "fc", "model": "aifs-single", "step": step}
            file_prefix = "at_aifs"
        elif model_type == 'IFS':
            common_params = {"class": "od", "stream": "oper", "type": "fc", "levtype": "pl", "step": step}
            file_prefix = "at_ifs"   
        elif model_type == 'EPS' and cycle in [0, 12] and 'spread' in task_type:
            eps_save_dir = os.path.join(ecmwf_root, "EPS", cycle_str)
            os.makedirs(eps_save_dir, exist_ok=True)
            tasks.append({
                "name": "at_eps_spread",
                "save_dir": eps_save_dir,
                "temp_name": f"TEMP_GLOBAL_eps_{date_str}_{cycle_str}_{step}h.grib2",
                "final_name": f"at_eps_spread_{date_str}_{cycle:02d}z_{step}h_nh.nc",
                "params": {"class": "od", "stream": "enfo", "type": "es", "levtype": "pl", 
                           "levelist": [500, 850], "param": ['z', 't'], "step": step}
            })
            continue
        else:
            continue

        if 'upper' in task_type:
            tasks.append({
                "name": f"{file_prefix}_upper",
                "save_dir": model_save_dir,
                "temp_name": f"TEMP_GLOBAL_{file_prefix}_upper_{date_str}_{cycle_str}_{step}h.grib2",
                "final_name": f"{file_prefix}_upper_{date_str}_{cycle:02d}z_{step}h_nh.nc",
                "params": {**common_params, "levtype": "pl", "levelist": [850, 500, 250], "param": ['z', 'gh', 't', 'u', 'v', 'vo']}
            })

        if 'surface' in task_type:
            tasks.append({
                "name": f"{file_prefix}_surface",
                "save_dir": model_save_dir,
                "temp_name": f"TEMP_GLOBAL_{file_prefix}_surface_{date_str}_{cycle_str}_{step}h.grib2",
                "final_name": f"{file_prefix}_surface_{date_str}_{cycle:02d}z_{step}h_nh.nc",
                "params": {**common_params, "levtype": "sfc", "param": ['tp', 'msl', '2t']}
            })

    all_success = True
    
    for task in tasks:
        final_path = os.path.join(task['save_dir'], task['final_name'])
        temp_path = os.path.join(temp_dir, task['temp_name'])
        
        if os.path.exists(final_path):
            continue 
            
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
            ds_us = crop_to_nh_safe(ds)
            
            if ds_us is not None:
                comp = dict(zlib=True, complevel=5)
                encoding = {var: comp for var in ds_us.data_vars}
                ds_us.to_netcdf(final_path, engine='netcdf4', encoding=encoding)
                ds.close()
                ds_us.close()
                os.remove(temp_path)

                # NEW S3 LOGIC HERE:
                s3_folder_path = f"weather_data/ecmwf/{date_str}/{cycle:02d}z"
                upload_to_s3_and_cleanup(final_path, s3_folder_path)

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

# ================= Processing Functions =================
def process_z500(ds, tag="Z500"):
    """
    Smart Processing for Z500
    """
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
    """
    Extracts point value from DataArray, safely handling longitude bounds.
    """
    if da is None: return 0.0
    try: return float(da.sel(longitude=lon, latitude=lat, method='nearest'))
    except KeyError:
        try:
            lon_query = lon + 360 if lon < 0 else (lon - 360 if lon > 180 else lon)
            return float(da.sel(longitude=lon_query, latitude=lat, method='nearest'))
        except: return 0.0
    except: return 0.0

def ensure_2d(da, tag="Unknown"):
    """
    Ensure dimensionality reduction strictly down to 2 dimensions (lat, lon).
    """
    if da is None: return None
    try: da = da.squeeze()
    except: pass
        
    while da.ndim > 2:
        dims = list(da.dims)
        target_dim = next((d for d in dims if 'lat' not in str(d).lower() and 'lon' not in str(d).lower()), dims[0])
        da = da.isel({target_dim: -1})
    return da

def force_2d(da):
    """Ensure data only has (lat, lon) dimensions, removing single dimensions like time/step"""
    try: return da.squeeze()
    except: return da

def load_data(file_path):
    """
    Alpha Trader NC Loader
    Strategy: Directly read preprocessed NetCDF files (_nh.nc).
    If file doesn't exist, trigger auto-download (ETL Pipeline).
    """
    if not os.path.exists(file_path):
        logger.info(f"Data file {file_path} missing, triggering ETL...")
        try:
            basename = os.path.basename(file_path)
            parts = basename.replace('_nh.nc', '').split('_')
            date_str = next((p for p in parts if len(p)==8 and p.isdigit()), None)
            cycle_str = next((p for p in parts if p.endswith('z') and len(p)==3), None)
            step_str = next((p for p in parts if p.endswith('h') and p[:-1].isdigit()), None)

            if date_str and cycle_str and step_str:
                dt = datetime.strptime(date_str, "%Y%m%d")
                cycle = int(cycle_str.replace('z', ''))
                step = int(step_str.replace('h', ''))
                
                if "gfs" in basename: download_gfs_robust(dt, cycle, step)
                elif ("aifs" in basename or "ifs" in basename) and ("upper" in basename or "basic" in basename):
                    download_ecmwf_unified(dt, cycle, step, target_models=['AIFS'] if "aifs" in basename else ['IFS'], task_type=["upper"])
                elif ("aifs" in basename or "ifs" in basename) and ("surface" in basename or "precip" in basename):
                    download_ecmwf_unified(dt, cycle, step, target_models=['AIFS'] if "aifs" in basename else ['IFS'], task_type=["surface"])
                elif "eps" in basename or "spread" in basename:
                    download_ecmwf_unified(dt, cycle, step, target_models=['EPS'], task_type=["spread"])
        except Exception as e:
            logger.error(f"Auto-download failed: {e}")

    if os.path.exists(file_path):
        try: return xr.open_dataset(file_path)
        except Exception as e:
            logger.error(f"Failed to open NC file {file_path}: {e}")
            try:
                if os.path.getsize(file_path) < 100: os.remove(file_path)
            except: pass
    return None