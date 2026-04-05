WITH stg_upper AS (
    SELECT 'aifs' as model, * FROM {{ ref('stg_ecmwf_aifs_upper') }}
union all
    SELECT 'ifs' as model, * FROM {{ ref('stg_ecmwf_ifs_upper') }}
union all
    SELECT 'gfs' as model, * FROM {{ ref('stg_gfs_upper') }}   
)

SELECT * FROM stg_upper
