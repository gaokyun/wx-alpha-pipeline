{{ config(
    incremental_strategy='merge',
    alias='fct_aifs_upper',
    unique_key=['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i', 'pressure_level_hpa'],
    parallel=false,
    incremental_predicates=["DBT_INTERNAL_DEST.cycle_date >= TRUNC(SYSDATE) - 1"]
) }}

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
    pressure_level_hpa,
    geopotential_height_m,
    temp_kelvin, 
    temp_celsius,
    u_wind_m_s,
    v_wind_m_s,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ ref('stg_adw_aifs_upper') }}
{% if is_incremental() %}
    WHERE forecast_reference_time >= TRUNC(SYSDATE) - 1
{% else %}
    WHERE forecast_reference_time >= TRUNC(SYSDATE) - 1
{% endif %}
