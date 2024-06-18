{{
  config(
    materialized='incremental',
    unique_key= 'budget_id'
  )
}}

WITH src_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
    ),
    

renamed_casted AS (
    SELECT
         {{ dbt_utils.generate_surrogate_key(['_row']) }} AS budget_id,
         product_id,
         quantity::integer AS quantity,
         month AS date,
         _fivetran_synced AS date_load
    FROM src_budget_products
    )

SELECT * FROM renamed_casted

{% if is_incremental() %}

  where date_load > (select max(date_load) from {{ this }})

{% endif %}