WITH raw_aifs AS (
    SELECT * FROM {{ source('ecmwf_raw', 'aifs_upper') }}
),

renamed_and_casted AS (
    SELECT
        -- 1. Temporal Identifiers (Strictly quoted and divided by 1,000,000)
        TO_TIMESTAMP_NTZ("time" / 1000000)::DATE AS cycle_date,
        HOUR(TO_TIMESTAMP_NTZ("time" / 1000000)) AS cycle_hour,
        
        CAST("step" AS INTEGER) AS forecast_step_hours,
        
        TO_TIMESTAMP_NTZ("valid_time" / 1000000)::DATE AS valid_date,
        HOUR(TO_TIMESTAMP_NTZ("valid_time" / 1000000)) AS valid_hour,

        -- 2. Spatial Identifiers
        CAST("latitude" AS FLOAT) AS lat,
        CAST("longitude" AS FLOAT) AS lon,
        CAST("isobaricInhPa" AS INTEGER) AS pressure_level_hpa,

        -- 3. Meteorological Variables
        CAST("t" AS FLOAT) AS temp_kelvin,
        (CAST("t" AS FLOAT) - 273.15) AS temp_celsius,

        CAST("u" AS FLOAT) AS u_wind_m_s,
        CAST("v" AS FLOAT) AS v_wind_m_s

    FROM raw_aifs
)

SELECT * FROM renamed_and_casted