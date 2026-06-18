import os
import shutil
import re

def main():
    dph_dir = "/home/airflow/dev/wx-alpha-pipeline/physical_meteor/models/dph"
    pdh_dir = "/home/airflow/dev/wx-alpha-pipeline/physical_meteor/models/pdh"

    # Create pdh directory if not exists
    os.makedirs(pdh_dir, exist_ok=True)

    # List of name mappings
    mappings = {
        # Staging
        "stg_gfs_surface": "stg_pdh_gfs_surface",
        "stg_gfs_upper": "stg_pdh_gfs_upper",
        "stg_ecmwf_aifs_spread": "stg_pdh_ecmwf_aifs_spread",
        "stg_ecmwf_aifs_surface": "stg_pdh_ecmwf_aifs_surface",
        "stg_ecmwf_aifs_upper": "stg_pdh_ecmwf_aifs_upper",
        "stg_ecmwf_ifs_spread": "stg_pdh_ecmwf_ifs_spread",
        "stg_ecmwf_ifs_surface": "stg_pdh_ecmwf_ifs_surface",
        "stg_ecmwf_ifs_upper": "stg_pdh_ecmwf_ifs_upper",
        # Marts Weather
        "dim_grid_points": "dim_pdh_grid_points",
        "fct_spread_forecast": "fct_pdh_spread_forecast",
        "fct_surface_forecast": "fct_pdh_surface_forecast",
        "fct_upper_forecast": "fct_pdh_upper_forecast",
        # Marts Atomic
        "fct_aifs_spread": "fct_pdh_aifs_spread",
        "fct_aifs_surface": "fct_pdh_aifs_surface",
        "fct_aifs_upper": "fct_pdh_aifs_upper",
        "fct_gfs_surface": "fct_pdh_gfs_surface",
        "fct_gfs_upper": "fct_pdh_gfs_upper",
        "fct_ifs_spread": "fct_pdh_ifs_spread",
        "fct_ifs_surface": "fct_pdh_ifs_surface",
        "fct_ifs_upper": "fct_pdh_ifs_upper",
    }

    for root, dirs, files in os.walk(dph_dir):
        for file in files:
            # Determine the relative path of root
            rel_root = os.path.relpath(root, dph_dir)
            target_root = os.path.join(pdh_dir, rel_root)
            os.makedirs(target_root, exist_ok=True)
            
            src_path = os.path.join(root, file)
            
            # Determine new filename
            base_name, ext = os.path.splitext(file)
            new_base_name = mappings.get(base_name, base_name)
            # If it's a yml schema file, let's prefix it or rename it
            if ext in [".yml", ".yaml"]:
                if "gfs" in base_name:
                    new_base_name = base_name.replace("gfs", "pdh_gfs")
                elif "ecmwf" in base_name:
                    new_base_name = base_name.replace("ecmwf", "pdh_ecmwf")
                elif "weather" in base_name:
                    new_base_name = base_name.replace("weather", "pdh_weather")
            
            dest_path = os.path.join(target_root, new_base_name + ext)
            
            # Read content and apply replacements
            with open(src_path, "r", encoding="utf-8") as f:
                content = f.read()
                
            # Apply ref replacements
            for old, new in mappings.items():
                # Replace ref('old') with ref('new')
                content = re.sub(rf"ref\(\s*'{old}'\s*\)", f"ref('{new}')", content)
                content = re.sub(rf"ref\(\s*\"{old}\"\s*\)", f"ref('{new}')", content)
                
                # If yml/yaml file, replace model names in configs
                if ext in [".yml", ".yaml"]:
                    content = re.sub(rf"\b{old}\b", new, content)
            
            # Replace source calls in staging files
            content = re.sub(rf"source\(\s*['\"]gfs_raw['\"]\s*,\s*", "source('pdh_raw', ", content)
            content = re.sub(rf"source\(\s*['\"]ecmwf_raw['\"]\s*,\s*", "source('pdh_raw', ", content)
            
            # Replace source references in yaml files
            if ext in [".yml", ".yaml"]:
                content = content.replace("gfs_raw", "pdh_raw")
                content = content.replace("ecmwf_raw", "pdh_raw")
                
            with open(dest_path, "w", encoding="utf-8") as f:
                f.write(content)
                
            print(f"Copied {src_path} -> {dest_path}")

if __name__ == "__main__":
    main()
