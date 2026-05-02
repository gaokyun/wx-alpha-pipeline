{{ config(
    partition_by=['cycle_date', 'cycle_hour'],
    unique_key=['surrogate_merge_key']
) }}

SELECT 
    surrogate_merge_key,
    'gfs' AS weather_model,
    cycle_date, cycle_hour, forecast_step_hours, valid_date, valid_hour, 
    lat, 
    lon,
    pressure_level_hpa, geopotential_height_m, temp_kelvin, 
    temp_celsius, u_wind_m_s, v_wind_m_s,
    -- Adding a load timestamp is standard practice for Gold Fact tables
    now() AS dbt_updated_at
FROM {{ ref('stg_gfs_upper') }}

{% if is_incremental() %}
    -- 1. Static Filter (Fast Partition Pruning)
    WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
    
    -- 2. Dynamic Filter (Precision)
    AND (cycle_date + cycle_hour/24.0) > (
        SELECT MAX(cycle_date + cycle_hour/24.0) 
        FROM {{ this }}
        -- 3. Optimization: Limit the subquery scan to the last 3 days of history
        WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
    )
{% endif %}