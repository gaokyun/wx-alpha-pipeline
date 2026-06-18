{{ config(
    materialized='table',
    database='PHYSICAL_METEOR_DB',
    schema='pdh_marts',
    unique_key=['lat_i', 'lon_i'],
    tags=['silver', 'staging', 'gold']
) }}

-- Extract unique grid points from GFS as the baseline
SELECT DISTINCT
    lat_i,
    lon_i,
    lat,
    lon
FROM {{ ref('stg_pdh_gfs_upper') }}
