{{ config(materialized='table') }}

SELECT
    town,
    flat_type,
    flat_model,
    AVG(floor_area_sqm) AS avg_floor_area_sqm,
    AVG(resale_price) AS avg_resale_price,
    AVG(price_per_sqm) AS avg_price_per_sqm
FROM {{ ref('stg_hdb_resale') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3