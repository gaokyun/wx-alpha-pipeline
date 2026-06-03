{{ config(
    alias='dim_locations',
    unique_key=['lat_i', 'lon_i']
) }}

{% if is_incremental() %}
-- Grid locations are completely static and already fully populated.
-- Returning an empty set here avoids scanning millions of rows of forecast data on incremental runs.
SELECT 
    CAST(NULL AS INTEGER) AS lat_i, 
    CAST(NULL AS INTEGER) AS lon_i, 
    CAST(NULL AS DOUBLE) AS lat, 
    CAST(NULL AS DOUBLE) AS lon 
WHERE 1=0
{% else %}
WITH all_locations AS (
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_gfs_surface') }} 
    WHERE forecast_step_hours = 240 AND cycle_hour = 0
      AND cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_ifs_surface') }} 
    WHERE forecast_step_hours = 240 AND cycle_hour = 0
      AND cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_aifs_surface') }} 
    WHERE forecast_step_hours = 240 AND cycle_hour = 0
      AND cycle_date >= CURRENT_DATE - INTERVAL 1 DAY
)
SELECT DISTINCT lat_i, lon_i, lat, lon
FROM all_locations
{% endif %}
