{{ config(
    alias='dim_times',
    unique_key=['cycle_date', 'cycle_hour', 'valid_date', 'valid_hour']
) }}
WITH all_times AS (
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_gfs_surface') }} 
    WHERE lat = 0.0 AND lon = 0.0
      {% if is_incremental() %} AND cycle_date >= TRUNC(SYSDATE) - 1 {% else %} AND cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_ifs_surface') }} 
    WHERE lat = 0.0 AND lon = 0.0
      {% if is_incremental() %} AND cycle_date >= TRUNC(SYSDATE) - 1 {% else %} AND cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
    UNION
    SELECT cycle_date, cycle_hour, valid_date, valid_hour FROM {{ ref('stg_adw_aifs_surface') }} 
    WHERE lat = 0.0 AND lon = 0.0
      {% if is_incremental() %} AND cycle_date >= TRUNC(SYSDATE) - 1 {% else %} AND cycle_date >= TRUNC(SYSDATE) - 3 {% endif %}
)
SELECT DISTINCT cycle_date, cycle_hour, valid_date, valid_hour, CURRENT_TIMESTAMP AS dbt_updated_at
FROM all_times
