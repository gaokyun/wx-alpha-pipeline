{{ config(
    materialized='table',
    alias='fct_surface_forecast'
) }}

SELECT
    surrogate_merge_key,
    weather_model,
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
    total_precipitation_mm,
    dbt_updated_at
FROM {{ ref('fct_dmh_gfs_surface') }}
UNION ALL
SELECT
    surrogate_merge_key,
    weather_model,
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
    total_precipitation_mm,
    dbt_updated_at
FROM {{ ref('fct_dmh_aifs_surface') }}
UNION ALL
SELECT
    surrogate_merge_key,
    weather_model,
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
    total_precipitation_mm,
    dbt_updated_at
FROM {{ ref('fct_dmh_ifs_surface') }}
