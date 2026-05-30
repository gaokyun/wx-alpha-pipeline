{{ config(
    alias='fct_gfs_upper',
    unique_key=['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i', 'pressure_level_hpa']
) }}

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
    pressure_level_hpa,
    geopotential_height_m,
    temp_kelvin, 
    temp_celsius,
    u_wind_m_s,
    v_wind_m_s,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ ref('stg_dmh_gfs_upper') }}
{% if is_incremental() %}
    WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
{% else %}
    WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY
{% endif %}
