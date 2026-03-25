WITH ecmwf_data AS (
    SELECT * FROM {{ ref('stg_ecmwf_aifs_upper') }}
),
gfs_data AS (
    SELECT * FROM {{ ref('stg_gfs_upper') }}
)

SELECT 
    -- 1. Unifying the Primary Keys (Defining the exact Grain)
    COALESCE(e.cycle_time, g.cycle_time) AS cycle_time,
    COALESCE(e.valid_time, g.valid_time) AS valid_time,
    COALESCE(e.pressure_level_hpa, g.pressure_level_hpa) AS pressure_level_hpa,
    COALESCE(e.lat, g.lat) AS lat,
    COALESCE(e.lon, g.lon) AS lon,

    -- 2. Meteorological Variables
    e.temp_celsius AS ecmwf_temp_c,
    g.temp_celsius AS gfs_temp_c,
    (e.temp_celsius - g.temp_celsius) AS temp_model_spread,
    
    e.u_wind_m_s AS ecmwf_u_wind,
    g.u_wind_m_s AS gfs_u_wind,
    e.v_wind_m_s AS ecmwf_v_wind,
    g.v_wind_m_s AS gfs_v_wind

FROM ecmwf_data e
FULL OUTER JOIN gfs_data g
    ON e.cycle_time = g.cycle_time
    AND e.valid_time = g.valid_time 
    AND e.pressure_level_hpa = g.pressure_level_hpa
    AND e.lat = g.lat 
    AND e.lon = g.lon