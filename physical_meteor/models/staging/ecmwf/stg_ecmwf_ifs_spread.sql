WITH raw_spread AS (
    SELECT * FROM {{ source('ecmwf_raw', 'ifs_spread') }}
),

renamed_and_casted AS (
    SELECT
        -- 1. Temporal (Native timestamps from Delta)
        CAST(forecast_reference_time AS DATE) AS cycle_date,
        EXTRACT(HOUR FROM forecast_reference_time) AS cycle_hour,
        
        CAST(step_hours AS INTEGER) AS forecast_step_hours,
        
        CAST(valid_time AS DATE) AS valid_date,
        EXTRACT(HOUR FROM valid_time) AS valid_hour,

        -- 2. Spatial
        CAST(latitude AS FLOAT) AS lat,
        CAST(longitude AS FLOAT) AS lon,
        CAST(isobaricInhPa AS INTEGER) AS pressure_level_hpa,

        -- 3. Meteorological 
        -- Ensuring we pull both Temperature and Geopotential Height spread
        CAST(t AS FLOAT) AS temp_spread_k,
        CAST(z AS FLOAT) / 9.80665 AS geopotential_height_spread_m

    FROM raw_spread
)

SELECT * FROM renamed_and_casted