WITH raw_surface AS (
    SELECT * FROM {{ source('ecmwf_raw', 'ifs_surface') }} -- or 'ifs_surface'
),

renamed_and_casted AS (
    SELECT
        -- 1. Temporal
        TO_TIMESTAMP_NTZ("time" / 1000000)::DATE AS cycle_date,
        HOUR(TO_TIMESTAMP_NTZ("time" / 1000000)) AS cycle_hour,
        CAST("step" AS INTEGER) AS forecast_step_hours,
        TO_TIMESTAMP_NTZ("valid_time" / 1000000)::DATE AS valid_date,
        HOUR(TO_TIMESTAMP_NTZ("valid_time" / 1000000)) AS valid_hour,

        -- 2. Spatial
        CAST("latitude" AS FLOAT) AS lat,
        CAST("longitude" AS FLOAT) AS lon,

        -- 3. Meteorological (Using the correct raw names: t2m, d2m)
        CAST("t2m" AS FLOAT) AS temp_2m_kelvin,
        (CAST("t2m" AS FLOAT) - 273.15) AS temp_2m_celsius,
        
        CAST("d2m" AS FLOAT) AS dewpoint_2m_kelvin,
        (CAST("d2m" AS FLOAT) - 273.15) AS dewpoint_2m_celsius,
        
        CAST("msl" AS FLOAT) / 100.0 AS msl_pressure_hpa, 
        CAST("tp" AS FLOAT) AS total_precipitation_m

    FROM raw_surface
)

SELECT * FROM renamed_and_casted