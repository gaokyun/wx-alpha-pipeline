{{ config(
    partition_by=['cycle_date', 'cycle_hour'],
    unique_key=['cycle_date', 'cycle_hour', 'forecast_step_hours', 'lat_i', 'lon_i', 'pressure_level_hpa']
) }}

SELECT 
    surrogate_merge_key,
    'ifs' AS weather_model,
    cycle_date, cycle_hour, forecast_step_hours, valid_date, valid_hour, 
    lat_i,
    lon_i,
    pressure_level_hpa, geopotential_height_m, temp_kelvin, 
    temp_celsius, u_wind_m_s, v_wind_m_s,
    -- Adding a load timestamp is standard practice for Gold Fact tables
    now() AS dbt_updated_at
FROM {{ ref('stg_ecmwf_ifs_upper') }}

{% if is_incremental() %}
    -- 1. Static Filter (Fast Partition Pruning)
    WHERE cycle_date >= CURRENT_DATE - INTERVAL 4 DAY
    
    -- 2. Dynamic Filter (Precision)
    AND (cycle_date + (cycle_hour * INTERVAL '1 hour')) > (
        SELECT MAX(cycle_date + (cycle_hour * INTERVAL '1 hour')) 
        FROM {{ this }}
        WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
    )
{% endif %}