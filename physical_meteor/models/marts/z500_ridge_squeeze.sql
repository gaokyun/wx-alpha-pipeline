-- models/marts/z500_ridge_squeeze.sql

{{ config(
    materialized='table',
    tags=['meteorology', 'z500_dynamics', 'hdd_risk']
) }}

WITH gfs_z500 AS (
    SELECT 
        valid_time,
        forecast_hour,
        lat,
        lon,
        geopotential_height AS z500_gfs
    FROM {{ ref('stg_gfs_upper_air') }}
    WHERE pressure_level = 500
),

ecmwf_z500 AS (
    SELECT 
        valid_time,
        forecast_hour,
        lat,
        lon,
        geopotential_height AS z500_ecmwf
    FROM {{ ref('stg_ecmwf_upper_air') }}
    WHERE pressure_level = 500
),

-- Physical Targeting: Isolating the critical HDD pricing hubs
target_regions AS (
    SELECT 'Chicago' AS region_name, 41.8 AS lat_center, -87.6 AS lon_center
    UNION ALL
    SELECT 'New York', 40.7, -74.0
    UNION ALL
    SELECT 'New England', 43.0, -71.0
),

-- Structural Test: Is the 570dam (5700m) ridge physically present in the grid?
model_consensus AS (
    SELECT 
        g.valid_time,
        g.forecast_hour,
        r.region_name,
        g.z500_gfs,
        e.z500_ecmwf,
        -- Flagging the 570dam presence
        CASE WHEN g.z500_gfs >= 5700 THEN 1 ELSE 0 END AS gfs_ridge_flag,
        CASE WHEN e.z500_ecmwf >= 5700 THEN 1 ELSE 0 END AS ecmwf_ridge_flag
    FROM gfs_z500 g
    JOIN ecmwf_z500 e 
        ON g.valid_time = e.valid_time 
        AND g.lat = e.lat 
        AND g.lon = e.lon
    JOIN target_regions r
        -- Creating a local bounding box (approx. 2-degree radius around hubs)
        ON g.lat BETWEEN r.lat_center - 2 AND r.lat_center + 2
        AND g.lon BETWEEN r.lon_center - 2 AND r.lon_center + 2
)

-- Market Translation (So What): Aggregation and Signal Generation
SELECT 
    valid_time,
    forecast_hour,
    region_name,
    AVG(z500_gfs) AS avg_z500_gfs,
    AVG(z500_ecmwf) AS avg_z500_ecmwf,
    SUM(gfs_ridge_flag) AS gfs_ridge_grid_count,
    SUM(ecmwf_ridge_flag) AS ecmwf_ridge_grid_count,
    
    -- Eliminating Fake Signals: Both models must agree on the 570dam inland expansion
    CASE 
        WHEN SUM(gfs_ridge_flag) > 0 AND SUM(ecmwf_ridge_flag) > 0 THEN 'Bearish'
        WHEN SUM(gfs_ridge_flag) = 0 AND SUM(ecmwf_ridge_flag) = 0 THEN 'Bullish'
        ELSE 'Neutral'
    END AS structural_hdd_risk,

    CASE 
        WHEN SUM(gfs_ridge_flag) > 0 AND SUM(ecmwf_ridge_flag) > 0 THEN 'Ridge Squeeze Confirmed'
        WHEN SUM(gfs_ridge_flag) = 0 AND SUM(ecmwf_ridge_flag) = 0 THEN 'Trough/Meridional Cold Potential'
        ELSE 'Model Divergence (Fake Signal Risk)'
    END AS structural_narrative,
    
    -- Gas Score / Uncertainty Metric (Difference in geopotential meters)
    CASE 
        WHEN ABS(AVG(z500_gfs) - AVG(z500_ecmwf)) < 30 THEN 'High'
        WHEN ABS(AVG(z500_gfs) - AVG(z500_ecmwf)) BETWEEN 30 AND 60 THEN 'Medium'
        ELSE 'Low'
    END AS confidence_score

FROM model_consensus
GROUP BY 
    valid_time,
    forecast_hour,
    region_name
ORDER BY 
    valid_time, 
    forecast_hour
    