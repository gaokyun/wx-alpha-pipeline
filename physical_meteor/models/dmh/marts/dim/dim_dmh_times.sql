{{ config(
    alias='dim_times',
    unique_key=['cycle_date', 'cycle_hour', 'valid_date', 'valid_hour']
) }}

WITH cycle_dates AS (
    SELECT (CURRENT_DATE - i::INTEGER) AS cycle_date
    FROM range(0, {% if is_incremental() %} 2 {% else %} 4 {% endif %}) t(i)
),
cycle_hours AS (
    SELECT i AS cycle_hour FROM (VALUES (0), (6), (12), (18)) t(i)
),
steps AS (
    SELECT i AS step_hours FROM (VALUES (192), (240), (288), (360)) t(i)
),
combinations AS (
    SELECT 
        d.cycle_date,
        h.cycle_hour,
        (CAST(d.cycle_date AS TIMESTAMP) + (h.cycle_hour + s.step_hours) * INTERVAL '1 hour') AS valid_time
    FROM cycle_dates d
    CROSS JOIN cycle_hours h
    CROSS JOIN steps s
)
SELECT DISTINCT
    cycle_date,
    cycle_hour,
    CAST(valid_time AS DATE) AS valid_date,
    CAST(extract('hour' FROM valid_time) AS INTEGER) AS valid_hour,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM combinations
