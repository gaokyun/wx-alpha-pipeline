{{ config(
    alias='dim_locations',
    unique_key=['lat_i', 'lon_i']
) }}
WITH all_locations AS (
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_gfs_surface') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_gfs_upper') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_ifs_surface') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_ifs_upper') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_ifs_spread') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_aifs_surface') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_aifs_upper') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT lat_i, lon_i, lat, lon FROM {{ ref('stg_adw_aifs_spread') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
)
SELECT DISTINCT lat_i, lon_i, lat, lon
FROM all_locations
