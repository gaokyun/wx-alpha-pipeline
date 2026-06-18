{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='unique_key',
    indexes=[
        {'columns': ['city_name', 'valid_date', 'valid_hour']},
        {'columns': ['weather_model', 'cycle_date', 'cycle_hour']}
    ]
) }}

SELECT 
    md5(concat_ws('-', f.weather_model, f.cycle_date::text, f.cycle_hour::text, f.forecast_step_hours::text, f.lat_i::text, f.lon_i::text)) as unique_key,
    f.*,
    d.city_name,
    d.state,
    d.region,
    d.market_weight,
    d.associated_hub
FROM {{ ref('fct_surface_forecast') }} f
JOIN {{ ref('dim_grid_points') }} d
  ON f.lat_i = d.lat_i AND f.lon_i = d.lon_i
WHERE d.is_city = TRUE

{% if is_incremental() %}
  AND f.dbt_updated_at >= (select max(dbt_updated_at) from {{ this }})
{% endif %}
