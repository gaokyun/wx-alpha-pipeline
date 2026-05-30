{{ config(
    materialized = 'view',
    alias = 'gfs_surface'
) }}

WITH raw_surface AS (
    SELECT * FROM {{ source('gfs_raw', 'gfs_surface') }}
    WHERE latitude BETWEEN -90 AND 90
      AND longitude BETWEEN -180 AND 360
      {% if target.name in ['dev_duckdb_mysql', 'dev_duckdb'] %}
      -- Developer optimization: filter coordinates to a US-bounding box to speed up runs
      AND latitude BETWEEN 30 AND 50
      AND (longitude BETWEEN -125 AND -70 OR longitude BETWEEN 235 AND 290)
      {% endif %}
),

renamed_and_casted AS (
    SELECT
        CAST(forecast_reference_time AS DATE) AS cycle_date,
        CAST(forecast_cycle AS INTEGER) AS cycle_hour,
        CAST(step_hours AS INTEGER) AS forecast_step_hours,
        CAST(valid_time AS DATE) AS valid_date,
        EXTRACT(HOUR FROM valid_time) AS valid_hour,
        CAST(latitude AS FLOAT) AS lat,
        CAST(longitude AS FLOAT) AS lon,
        COALESCE(CAST(ROUND((latitude + 90) * 1000) AS INTEGER), -1) AS lat_i,
        COALESCE(CAST(ROUND((longitude + 180) * 1000) AS INTEGER), -1) AS lon_i,
        CAST(t2m AS FLOAT) AS temp_2m_kelvin,
        (CAST(t2m AS FLOAT) - 273.15) AS temp_2m_celsius,
        CAST(d2m AS FLOAT) AS dewpoint_2m_kelvin,
        (CAST(d2m AS FLOAT) - 273.15) AS dewpoint_2m_celsius,
        CAST(msl AS FLOAT) / 100.0 AS msl_pressure_hpa, 
        CAST(tp AS FLOAT) AS total_precipitation_m,
        heightAboveGround AS height_above_ground_metadata,
        surface AS surface_type_metadata
    FROM raw_surface
)

SELECT *,
    '' AS surrogate_merge_key
FROM renamed_and_casted
