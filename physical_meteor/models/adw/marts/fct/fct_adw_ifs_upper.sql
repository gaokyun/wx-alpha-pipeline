{{ config(
    alias='fct_ifs_upper',
    unique_key=['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i', 'pressure_level_hpa']
) }}

SELECT 
    surrogate_merge_key,
    'ifs' AS weather_model,
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
FROM {{ ref('stg_adw_ifs_upper') }}
{% if is_incremental() %}
    WHERE cycle_date >= TRUNC(SYSDATE) - 1
{% else %}
    WHERE cycle_date >= TRUNC(SYSDATE) - 3
{% endif %}
