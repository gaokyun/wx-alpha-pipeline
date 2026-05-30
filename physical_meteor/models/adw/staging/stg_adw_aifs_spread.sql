{{ config(
    alias='aifs_spread'
) }}

WITH raw_spread AS (
    SELECT * FROM {{ source('adw_raw', 'ext_aifs_spread') }}
    WHERE latitude BETWEEN -90 AND 90
      AND longitude BETWEEN -180 AND 360
),

renamed_and_casted AS (
    SELECT
        TRUNC(CAST(forecast_reference_time AS DATE)) AS cycle_date,
        CAST(EXTRACT(HOUR FROM forecast_reference_time) AS NUMBER) AS cycle_hour,
        CAST(step_hours AS NUMBER) AS forecast_step_hours,
        TRUNC(CAST(valid_time AS DATE)) AS valid_date,
        EXTRACT(HOUR FROM valid_time) AS valid_hour,
        CAST(latitude AS BINARY_DOUBLE) AS lat,
        CAST(longitude AS BINARY_DOUBLE) AS lon,
        COALESCE(CAST(ROUND((latitude + 90) * 1000) AS NUMBER), -1) AS lat_i,
        COALESCE(CAST(ROUND((longitude + 180) * 1000) AS NUMBER), -1) AS lon_i,
        CAST(isobaricinhpa AS NUMBER) AS pressure_level_hpa,
        CAST(t AS BINARY_FLOAT) AS temp_spread_kelvin,
        (CAST(t AS BINARY_FLOAT) - 273.15) AS temp_spread_celsius,
        CAST(gh AS BINARY_FLOAT) AS geopotential_height_spread_m
    FROM raw_spread
)

SELECT renamed_and_casted.*,
    CAST(NULL AS VARCHAR2(32)) AS surrogate_merge_key
FROM renamed_and_casted
