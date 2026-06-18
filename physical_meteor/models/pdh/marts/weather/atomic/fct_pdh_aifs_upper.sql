{{ config(
    schema='pdh_marts',
    materialized='table',
    tags=["atomic_marts"]
) }}

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
        geopotential_height_m,
        temp_kelvin,
        temp_celsius,
        u_wind_m_s,
        v_wind_m_s
    FROM {{ ref('stg_pdh_ecmwf_aifs_upper') }}
    WHERE cycle_date >= CURRENT_DATE - INTERVAL '1 days'
      AND (cycle_date + (cycle_hour * INTERVAL '1 hour')) >= (
        SELECT MAX(cycle_date + (cycle_hour * INTERVAL '1 hour')) - INTERVAL '1 day'
        FROM {{ ref('stg_pdh_ecmwf_aifs_upper') }}
        WHERE cycle_date >= CURRENT_DATE - INTERVAL '1 days'
    )
)

SELECT 
    surrogate_merge_key,
    'aifs' AS weather_model,
    cycle_date, cycle_hour, forecast_step_hours, valid_date, valid_hour, 
    lat_i,
    lon_i,
    pressure_level_hpa, geopotential_height_m, temp_kelvin, 
    temp_celsius, u_wind_m_s, v_wind_m_s,
    now() AS dbt_updated_at
FROM silver_data
LIMIT 200000