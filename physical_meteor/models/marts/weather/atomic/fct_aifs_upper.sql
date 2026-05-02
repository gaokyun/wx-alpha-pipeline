{{ config(
    partition_by=['cycle_date', 'cycle_hour'],
    unique_key=['surrogate_merge_key']
) }}

SELECT 
    surrogate_merge_key as pkey,
    'aifs' AS weather_model,
    cycle_date, cycle_hour, forecast_step_hours, valid_date, valid_hour, 
    lat,     
    lon,
    pressure_level_hpa, geopotential_height_m, temp_kelvin, 
    temp_celsius, u_wind_m_s, v_wind_m_s
FROM {{ ref('stg_ecmwf_aifs_upper') }}

{% if is_incremental() %}
  WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
{% endif %}