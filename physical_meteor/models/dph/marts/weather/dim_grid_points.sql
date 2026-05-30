{{ config(
    materialized='table',
    database='PHYSICAL_METEOR_DB',
    schema='gold',
    unique_key=['lat_i', 'lon_i'],
    tags=['silver', 'staging', 'gold']
) }}

-- Extract unique grid points from GFS as the baseline
SELECT DISTINCT
    lat_i,
    lon_i,
    lat,
    lon
FROM {{ ref('stg_gfs_upper') }}
