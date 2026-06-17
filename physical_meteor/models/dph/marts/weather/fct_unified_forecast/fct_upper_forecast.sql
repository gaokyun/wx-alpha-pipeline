{{ config(schema='gold', materialized='view') }}

SELECT
    surrogate_merge_key,
    'gfs'::character varying AS weather_model,
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
    v_wind_m_s,
    dbt_updated_at
FROM {{ ref('fct_gfs_upper') }}
UNION ALL
SELECT
    surrogate_merge_key,
    'aifs'::character varying AS weather_model,
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
    v_wind_m_s,
    dbt_updated_at
FROM {{ ref('fct_aifs_upper') }}
UNION ALL
SELECT
    surrogate_merge_key,
    'ifs'::character varying AS weather_model,
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
    v_wind_m_s,
    dbt_updated_at
FROM {{ ref('fct_ifs_upper') }}
