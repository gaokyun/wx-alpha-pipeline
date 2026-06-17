{{ config(
    materialized='mysql_view',
    alias='fct_spread_forecast'
) }}

-- depends_on: {{ ref('fct_dmh_aifs_spread') }}
-- depends_on: {{ ref('fct_dmh_ifs_spread') }}

SELECT surrogate_merge_key,
        weather_model,
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
        dbt_updated_at
FROM fct_aifs_spread
UNION ALL
SELECT surrogate_merge_key,
        weather_model,
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
        dbt_updated_at
FROM fct_ifs_spread
