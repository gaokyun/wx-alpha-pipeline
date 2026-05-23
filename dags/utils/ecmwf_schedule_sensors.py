import requests
from bs4 import BeautifulSoup
from datetime import datetime
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowSensorTimeout

def check_ecmwf_product_ready(**kwargs):
    target_date = kwargs['target_date']   # 20260324
    cycle = kwargs['cycle']               # 06z
    product_path = kwargs['product_path'] # aifs-single/0p25/oper/
    
    # Construct the full URL for the specific product
    url = f"https://data.ecmwf.int/forecasts/{target_date}/{cycle}/{product_path}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 404:
            print(f"⏳ Directory not created yet: {url}")
            return False
            
        soup = BeautifulSoup(response.text, 'html.parser')
        # Look for the last expected file (e.g., 240h) to ensure the run is COMPLETE
        # Senior Tip: Don't just check if the folder exists; check if it's finished!
        files = [a.get('href') for a in soup.find_all('a') if a.get('href')]
        
        target_file = "240h-oper-fc.grib2" if "single" in product_path else "240h-enfo-fc.grib2"
        
        if any(target_file in f for f in files):
            print(f"✅ Product {product_path} is complete including {target_file}")
            return True
        else:
            print(f"⏳ Directory exists but {target_file} is still missing. Propagating...")
            return False
            
    except Exception as e:
        print(f"⚠️ Network error checking index: {e}")
        return False