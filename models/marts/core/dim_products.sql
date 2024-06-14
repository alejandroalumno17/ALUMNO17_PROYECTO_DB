{{
  config(
    materialized='table'
  )
}}

WITH snap_dim_products AS (
    SELECT *
    FROM {{ ref('snap_dim_products') }}
)

SELECT 
     product_id,
     products_name,
     price_euro,
     inventory,
     date_load
FROM snap_dim_products
WHERE dbt_valid_to IS NULL AND products_name IS NOT NULL