{{ config(
    alias='dim_times',
    unique_key=['cycle_date', 'cycle_hour', 'valid_date', 'valid_hour']
) }}
WITH all_times AS (
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_gfs_surface') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_gfs_upper') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_ifs_surface') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_ifs_upper') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_ifs_spread') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_aifs_surface') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_aifs_upper') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_aifs_spread') }} {% if is_incremental() %} WHERE cycle_date >= TRUNC(SYSDATE) - 1 {% else %} WHERE cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
)
SELECT DISTINCT cycle_date, cycle_hour, valid_date, valid_hour
FROM all_times
