{{ config(
    incremental_strategy='merge',
    alias='fct_aifs_spread',
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
    temp_spread_kelvin,
    temp_spread_celsius,
    geopotential_height_spread_m,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ ref('stg_adw_aifs_spread') }}
{% if is_incremental() %}
    WHERE forecast_reference_time >= TRUNC(SYSDATE) - 1
{% else %}
    WHERE forecast_reference_time >= TRUNC(SYSDATE) - 1
{% endif %}
