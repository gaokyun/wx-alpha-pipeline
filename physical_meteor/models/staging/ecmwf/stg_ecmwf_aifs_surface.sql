WITH raw_surface AS (
    SELECT * FROM {{ source('ecmwf_raw', 'at_aifs_surface') }}
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

        -- 3. Meteorological (Using mapped ECMWF short names)
        -- CAST("2t" AS FLOAT) AS temp_2m_kelvin,
        -- (CAST("2t" AS FLOAT) - 273.15) AS temp_2m_celsius,

        CAST(t2m AS FLOAT) AS temp_2m_kelvin,
        (CAST(t2m AS FLOAT) - 273.15) AS temp_2m_celsius,

        CAST(d2m AS FLOAT) AS dewpoint_2m_kelvin,
        (CAST(d2m AS FLOAT) - 273.15) AS dewpoint_2m_celsius,

        -- CAST("2d" AS FLOAT) AS dewpoint_2m_kelvin,
        -- (CAST("2d" AS FLOAT) - 273.15) AS dewpoint_2m_celsius,
        
        CAST(msl AS FLOAT) / 100.0 AS msl_pressure_hpa, 
        CAST(tp AS FLOAT) AS total_precipitation_m

    FROM raw_surface
)

SELECT * FROM renamed_and_casted