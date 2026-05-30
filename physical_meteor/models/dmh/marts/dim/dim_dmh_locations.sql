{{ config(
    alias='dim_locations',
    unique_key=['lat_i', 'lon_i']
) }}

WITH all_locations AS (
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_gfs_surface') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_gfs_upper') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_ifs_surface') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_ifs_upper') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_ifs_spread') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_aifs_surface') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_aifs_upper') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_dmh_aifs_spread') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
)
SELECT DISTINCT lat_i, lon_i, lat, lon
FROM all_locations
