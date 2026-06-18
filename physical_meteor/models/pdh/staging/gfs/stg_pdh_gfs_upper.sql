WITH raw_gfs AS (
    -- Updated source name from 'delta_lake' to 'gfs_raw' to match your YAML
    SELECT * FROM {{ source('pdh_raw', 'gfs_upper') }}
        -- Surgical strike: Remove coordinates that are physically impossible
    WHERE latitude BETWEEN -90 AND 90
      AND longitude BETWEEN -180 AND 360
),

renamed_and_casted AS (
    SELECT
        -- 1. Temporal Identifiers (DuckDB reads Delta timestamps natively)
        CAST(forecast_reference_time AS DATE) AS cycle_date,
        CAST(forecast_cycle AS INTEGER) AS cycle_hour,
        
        CAST(step_hours AS INTEGER) AS forecast_step_hours,
        
        CAST(valid_time AS DATE) AS valid_date,
        EXTRACT(HOUR FROM CAST(valid_time AS TIMESTAMP)) AS valid_hour,

        -- 2. Spatial Identifiers
        CAST(latitude AS FLOAT) AS lat,
        CAST(longitude AS FLOAT) AS lon,

        -- Fixed-Point Integer Indices
        -- Shift by 90/360 to ensure positive values, then scale to remove decimals
        COALESCE(CAST(ROUND((latitude + 90) * 1000) AS INTEGER), -1) AS lat_i,
        COALESCE(CAST(ROUND((longitude + 180) * 1000) AS INTEGER), -1) AS lon_i,

        CAST(isobaricInhPa AS INTEGER) AS pressure_level_hpa,

        -- 3. Meteorological Variables
        CAST(gh AS FLOAT) AS geopotential_height_m,
        CAST(t AS FLOAT) AS temp_kelvin,
        (CAST(t AS FLOAT) - 273.15) AS temp_celsius,
        CAST(u AS FLOAT) AS u_wind_m_s,
        CAST(v AS FLOAT) AS v_wind_m_s

    FROM raw_gfs
)

SELECT *,
    -- MD5(
    --     CAST(cycle_date AS VARCHAR) || '-' ||
    --     CAST(cycle_hour AS VARCHAR) || '-' ||
    --     CAST(forecast_step_hours AS VARCHAR) || '-' ||
    --     CAST(lat_i AS VARCHAR) || '-' ||
    --     CAST(lon_i AS VARCHAR)
    -- ) 
    '' AS surrogate_merge_key
 FROM renamed_and_casted