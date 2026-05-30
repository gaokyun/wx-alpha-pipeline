{{ config(
    alias='fct_aifs_surface',
    unique_key=['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i']
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
    temp_2m_kelvin,
    temp_2m_celsius,
    dewpoint_2m_kelvin,
    dewpoint_2m_celsius,
    msl_pressure_hpa,
    total_precipitation_m,
    (total_precipitation_m * 1000.0) AS total_precipitation_mm,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM {{ ref('stg_dmh_aifs_surface') }}
{% if is_incremental() %}
    WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
{% else %}
    WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY
{% endif %}
