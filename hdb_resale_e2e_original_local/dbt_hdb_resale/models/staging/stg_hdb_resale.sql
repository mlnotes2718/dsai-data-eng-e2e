{{ config(materialized='view') }}

WITH RAW AS (
    SELECT
        id,
        PARSE_DATE('%Y-%m', month) AS resale_month,
        town,
        flat_type,
        block,
        street_name,
        storey_range,
        CAST(floor_area_sqm AS FLOAT64) AS floor_area_sqm,
        flat_model,
        lease_commence_date,
        remaining_lease,
            -- Extract years, default to 0 if not found, multiply by 12
            COALESCE(CAST(REGEXP_EXTRACT(remaining_lease, r'(\d+) year') AS INT64), 0) * 12 +
            -- Extract months, default to 0 if not found
            COALESCE(CAST(REGEXP_EXTRACT(remaining_lease, r'(\d+) month') AS INT64), 0) 
        AS remaining_lease_months,
        CAST(resale_price AS FLOAT64) AS resale_price
    FROM {{ source('postgres_hdb_resale_raw', 'public_resale_flat_prices_from_jan_2017') }}
)
SELECT
    *,
    resale_price / floor_area_sqm AS price_per_sqm
FROM raw