{{ config(
    schema='gold',
    materialized='incremental',
    incremental_strategy='append',
    partition_by=['cycle_date', 'cycle_hour'],
    unique_key=['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i'],
    indexes=[
        {'columns': ['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i'], 'unique': True}
    ],
    tags=["atomic_marts"]
) }}

/* Note: DuckDB doesn't require explicit partition_by in the same way 
   BigQuery/Snowflake do for performance, but keeping your unique_key 
   comprehensive ensures the 'delete' phase of 'delete+insert' is precise.
*/

SELECT 
    surrogate_merge_key,
    'gfs' AS weather_model,
    cycle_date,
    cycle_hour,
    forecast_step_hours,
    valid_date,
    valid_hour,
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
    (total_precipitation_m * 1000.0) AS total_precipitation_mm,
    -- Adding a load timestamp is standard practice for Gold Fact tables
    now() AS dbt_updated_at

FROM {{ ref('stg_gfs_surface') }}

{% if is_incremental() %}
    -- 1. Static Filter (Fast Partition Pruning)
    WHERE cycle_date >= CURRENT_DATE - INTERVAL 7 DAY
    
    -- 2. Dynamic Filter (Precision)
    AND (cycle_date + (cycle_hour * INTERVAL '1 hour')) > (
        SELECT COALESCE(MAX(cycle_date + (cycle_hour * INTERVAL '1 hour')), '1970-01-01'::timestamp)
        FROM {{ this }}
        WHERE cycle_date >= CURRENT_DATE - INTERVAL 7 DAY
    )
{% endif %}