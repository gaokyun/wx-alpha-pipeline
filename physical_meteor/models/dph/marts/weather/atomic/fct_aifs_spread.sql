{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        partition_by=['cycle_date', 'cycle_hour'],
        unique_key=['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i', 'pressure_level_hpa'],
        indexes=[
            {'columns': ['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i', 'pressure_level_hpa'], 'unique': True}
        ],
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
    FROM {{ ref('stg_ecmwf_aifs_spread') }}

{% if is_incremental() %}
    WHERE cycle_date >= CURRENT_DATE - INTERVAL 7 DAY
    AND (cycle_date + (cycle_hour * INTERVAL '1 hour')) > (
        SELECT COALESCE(MAX(cycle_date + (cycle_hour * INTERVAL '1 hour')), '1970-01-01'::timestamp)
        FROM {{ this }}
        WHERE cycle_date >= CURRENT_DATE - INTERVAL 7 DAY
    )
{% endif %}
)

SELECT
    surrogate_merge_key,
    'aifs' as weather_model,
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
