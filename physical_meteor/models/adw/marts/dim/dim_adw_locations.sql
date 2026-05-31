{{ config(
    alias='dim_locations',
    unique_key=['lat_i', 'lon_i']
) }}
{% if is_incremental() %}
-- Grid locations are completely static and already fully populated.
-- Returning an empty set here avoids scanning millions of rows of forecast data on incremental runs.
SELECT 
    CAST(NULL AS NUMBER) AS lat_i, 
    CAST(NULL AS NUMBER) AS lon_i, 
    CAST(NULL AS BINARY_DOUBLE) AS lat, 
    CAST(NULL AS BINARY_DOUBLE) AS lon 
FROM DUAL 
WHERE 1=0
{% else %}
WITH all_locations AS (
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_gfs_surface') }} 
    WHERE forecast_step_hours = 240 AND cycle_hour = 0
      AND cycle_date >= TRUNC(SYSDATE) - 1
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_ifs_surface') }} 
    WHERE forecast_step_hours = 240 AND cycle_hour = 0
      AND cycle_date >= TRUNC(SYSDATE) - 1
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_aifs_surface') }} 
    WHERE forecast_step_hours = 240 AND cycle_hour = 0
      AND cycle_date >= TRUNC(SYSDATE) - 1
)
SELECT DISTINCT lat_i, lon_i, lat, lon
FROM all_locations
{% endif %}
