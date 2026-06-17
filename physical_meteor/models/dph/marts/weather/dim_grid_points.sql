{{ config(
    materialized='table',
    database='PHYSICAL_METEOR_DB',
    schema='gold',
    unique_key=['lat_i', 'lon_i'],
    indexes=[
        {'columns': ['lat_i', 'lon_i'], 'unique': True},
        {'columns': ['city_name']}
    ],
    tags=['silver', 'staging', 'gold']
) }}

-- Extract unique grid points from GFS as the baseline
WITH base_grid AS (
    SELECT DISTINCT
        lat_i,
        lon_i,
        lat,
        lon
    FROM {{ ref('stg_gfs_upper') }}
)

SELECT 
    g.lat_i,
    g.lon_i,
    g.lat,
    g.lon,
    c.city_name,
    c.state,
    c.region,
    c.market_weight,
    c.associated_hub,
    CASE WHEN c.city_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_city
FROM base_grid g
LEFT JOIN {{ ref('ref_weather_station') }} c
  ON g.lat_i = c.lat_i AND g.lon_i = c.lon_i
