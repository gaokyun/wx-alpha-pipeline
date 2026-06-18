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
        temp_2m_kelvin,
        temp_2m_celsius,
        dewpoint_2m_kelvin,
        dewpoint_2m_celsius,
        msl_pressure_hpa,
        total_precipitation_m
    FROM {{ ref('stg_pdh_ecmwf_aifs_surface') }}
    WHERE cycle_date >= CURRENT_DATE - INTERVAL '1 days'
      AND (cycle_date + (cycle_hour * INTERVAL '1 hour')) >= (
        SELECT MAX(cycle_date + (cycle_hour * INTERVAL '1 hour')) - INTERVAL '1 day'
        FROM {{ ref('stg_pdh_ecmwf_aifs_surface') }}
        WHERE cycle_date >= CURRENT_DATE - INTERVAL '1 days'
    )
)

SELECT 
    surrogate_merge_key,
    'aifs' AS weather_model,
    cycle_date,
    cycle_hour,
    forecast_step_hours,
    valid_date,
    valid_hour,
    lat_i,
    lon_i,
    temp_2m_kelvin,
    temp_2m_celsius,
    dewpoint_2m_kelvin,
    dewpoint_2m_celsius,
    msl_pressure_hpa,
    total_precipitation_m,
    (total_precipitation_m * 1000.0) AS total_precipitation_mm,
    now() AS dbt_updated_at
FROM silver_data
LIMIT 200000
