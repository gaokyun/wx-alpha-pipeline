{{
    config(
        materialized='incremental',
        partition_by=['cycle_date', 'cycle_hour'],
        unique_key=['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i', 'pressure_level_hpa'],
        tags=["atomic_marts"]
    )
}}

WITH silver_data AS (
    SELECT
        surrogate_merge_key,
        cycle_date,
        cycle_hour,
        forecast_step_hours,
        valid_date,
        valid_hour,
        lat_i,
        lon_i,
        pressure_level_hpa,
        temp_spread_kelvin,
        temp_spread_celsius,
        geopotential_height_spread_m
    FROM {{ ref('stg_ecmwf_ifs_spread') }}

{% if is_incremental() %}
    -- 1. Static Filter (Fast Partition Pruning)
    WHERE cycle_date >= CURRENT_DATE - INTERVAL 4 DAY
    
    -- 2. Dynamic Filter (Precision)
    AND (cycle_date + (cycle_hour * INTERVAL '1 hour')) > (
        SELECT MAX(cycle_date + (cycle_hour * INTERVAL '1 hour')) 
        FROM {{ this }}
        WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
    )
{% endif %}
)

SELECT
    surrogate_merge_key,
    'ifs' as weather_model,
    cycle_date,
    cycle_hour,
    forecast_step_hours,
    valid_date,
    valid_hour,
    lat_i,
    lon_i,
    pressure_level_hpa,
    temp_spread_kelvin,
    temp_spread_celsius,
    geopotential_height_spread_m,
    -- Adding a load timestamp is standard practice for Gold Fact tables
    now() AS dbt_updated_at
FROM silver_data
