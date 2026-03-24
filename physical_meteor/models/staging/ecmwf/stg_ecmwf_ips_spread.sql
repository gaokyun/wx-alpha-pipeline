WITH raw_spread AS (
    SELECT * FROM {{ source('ecmwf_raw', 'ifs_spread') }}
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
        CAST("isobaricInhPa" AS INTEGER) AS pressure_level_hpa,

        -- 3. Meteorological (Spread is a delta, so we keep it in Kelvin/Celsius scale units)
        CAST("t" AS FLOAT) AS temp_spread_k

    FROM raw_spread
)

SELECT * FROM renamed_and_casted