{{ config(
    materialized = 'view',
    alias = 'gfs_upper'
) }}

WITH raw_gfs AS (
    SELECT * FROM {{ source('gfs_raw', 'gfs_upper') }}
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
        CAST(isobaricInhPa AS INTEGER) AS pressure_level_hpa,
        CAST(gh AS FLOAT) AS geopotential_height_m,
        CAST(t AS FLOAT) AS temp_kelvin,
        (CAST(t AS FLOAT) - 273.15) AS temp_celsius,
        CAST(u AS FLOAT) AS u_wind_m_s,
        CAST(v AS FLOAT) AS v_wind_m_s
    FROM raw_gfs
)

SELECT *,
    '' AS surrogate_merge_key
  FROM renamed_and_casted
