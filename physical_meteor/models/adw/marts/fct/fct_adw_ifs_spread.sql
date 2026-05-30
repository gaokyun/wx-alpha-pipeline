{{ config(
    alias='fct_ifs_spread',
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
    temp_spread_kelvin,
    temp_spread_celsius,
    geopotential_height_spread_m,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ ref('stg_adw_ifs_spread') }}
{% if is_incremental() %}
    WHERE cycle_date >= TRUNC(SYSDATE) - 1
{% else %}
    WHERE cycle_date >= TRUNC(SYSDATE) - 3
{% endif %}
