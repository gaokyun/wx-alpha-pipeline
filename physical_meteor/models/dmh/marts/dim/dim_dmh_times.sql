{{ config(
    alias='dim_times',
    unique_key=['cycle_date', 'cycle_hour', 'valid_date', 'valid_hour']
) }}
WITH all_times AS (
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_dmh_gfs_surface') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_dmh_gfs_upper') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_dmh_ifs_surface') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_dmh_ifs_upper') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_dmh_ifs_spread') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_dmh_aifs_surface') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_dmh_aifs_upper') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_dmh_aifs_spread') }} {% if is_incremental() %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 1 DAY {% else %} WHERE cycle_date >= CURRENT_DATE - INTERVAL 3 DAY {% endif %}
)
SELECT DISTINCT cycle_date, cycle_hour, valid_date, valid_hour
FROM all_times
