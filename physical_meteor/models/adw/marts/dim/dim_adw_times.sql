{{ config(
    alias='dim_times',
    unique_key=['cycle_date', 'cycle_hour', 'valid_date', 'valid_hour']
) }}
WITH cycle_dates AS (
    SELECT TRUNC(SYSDATE) - (LEVEL - 1) AS cycle_date
    FROM DUAL
    CONNECT BY LEVEL <= {% if is_incremental() %} 2 {% else %} 4 {% endif %}
),
cycle_hours AS (
    SELECT column_value AS cycle_hour FROM TABLE(sys.odcinumberlist(0, 6, 12, 18))
),
steps AS (
    SELECT column_value AS step_hours FROM TABLE(sys.odcinumberlist(192, 240, 288, 360))
),
combinations AS (
    SELECT 
        d.cycle_date,
        h.cycle_hour,
        d.cycle_date + (h.cycle_hour + s.step_hours) / 24 AS valid_time
    FROM cycle_dates d
    CROSS JOIN cycle_hours h
    CROSS JOIN steps s
)
SELECT DISTINCT
    cycle_date,
    cycle_hour,
    TRUNC(valid_time) AS valid_date,
    CAST(TO_CHAR(valid_time, 'HH24') AS NUMBER) AS valid_hour,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM combinations
