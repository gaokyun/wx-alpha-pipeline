{{ config(schema='gold', materialized='table') }}

SELECT
    surrogate_merge_key,
    weather_model,
    cycle_date,
    cycle_hour,
    forecast_step_hours,
    valid_date,
    valid_hour,
    lat,
    lon,
    lat_i,
    lon_i,

    -- Temperature and Dewpoint
    temp_2m_kelvin,
    temp_2m_celsius,
    dewpoint_2m_kelvin,
    dewpoint_2m_celsius,
    msl_pressure_hpa,
    total_precipitation_m,
    total_precipitation_mm,
    dbt_updated_at FROM {{ ref('fct_gfs_surface') }}
UNION ALL
SELECT
    surrogate_merge_key,
    weather_model,
    cycle_date,
    cycle_hour,
    forecast_step_hours,
    valid_date,
    valid_hour,
    lat,
    lon,
    lat_i,
    lon_i,
    -- Temperature and Dewpoint
    temp_2m_kelvin,
    temp_2m_celsius,
    dewpoint_2m_kelvin,
    dewpoint_2m_celsius,
    msl_pressure_hpa,
    
    -- Precipitation (Converting meters from staging to mm for Gold layer usability)
    total_precipitation_m,
    total_precipitation_mm,
    -- Adding a load timestamp is standard practice for Gold Fact tables
    dbt_updated_at FROM {{ ref('fct_aifs_surface') }}
UNION ALL
SELECT
    surrogate_merge_key,
    weather_model,
    cycle_date,
    cycle_hour,
    forecast_step_hours,
    valid_date,
    valid_hour,
    lat,
    lon,
    lat_i,
    lon_i,
 
    -- Temperature and Dewpoint
    temp_2m_kelvin,
    temp_2m_celsius,
    dewpoint_2m_kelvin,
    dewpoint_2m_celsius,

    -- Pressure (Matched to staging msl_pressure_hpa)
    msl_pressure_hpa,
    
    -- Precipitation (Converting meters from staging to mm for Gold layer usability)
    total_precipitation_m,
    total_precipitation_mm,
    dbt_updated_at FROM {{ ref('fct_ifs_surface') }}