{{ config(schema='gold', materialized='view') }}

SELECT * FROM {{ ref('fct_gfs_upper') }}
UNION ALL
SELECT * FROM {{ ref('fct_aifs_upper') }}
UNION ALL
SELECT * FROM {{ ref('fct_ifs_upper') }}