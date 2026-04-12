WITH raw_ifs AS (
    SELECT * FROM {{ source('ecmwf_raw', 'at_ifs_upper') }}
),

renamed_and_casted AS (
    SELECT
        -- 1. Temporal Identifiers (Directly from Delta native timestamps)
        CAST(forecast_reference_time AS DATE) AS cycle_date,
        EXTRACT(HOUR FROM forecast_reference_time) AS cycle_hour,
        
        CAST(step_hours AS INTEGER) AS forecast_step_hours,
        
        CAST(valid_time AS DATE) AS valid_date,
        EXTRACT(HOUR FROM valid_time) AS valid_hour,

        -- 2. Spatial Identifiers
        CAST(latitude AS FLOAT) AS lat,
        CAST(longitude AS FLOAT) AS lon,
        CAST(isobaricInhPa AS INTEGER) AS pressure_level_hpa,

        -- 3. Meteorological Variables
        -- Traditional IFS often provides Geopotential Height directly as 'gh'
        CAST(gh AS FLOAT) AS geopotential_height_m,

        CAST(t AS FLOAT) AS temp_kelvin,
        (CAST(t AS FLOAT) - 273.15) AS temp_celsius,

        CAST(u AS FLOAT) AS u_wind_m_s,
        CAST(v AS FLOAT) AS v_wind_m_s

    FROM raw_ifs
)

SELECT * FROM renamed_and_casted