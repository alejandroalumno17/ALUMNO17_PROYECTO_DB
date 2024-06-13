{% snapshot snap_dim_products %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='date_load',
    )
}}


WITH stg_sql_server_dbo__products AS (
    SELECT DISTINCT product_id, date_load
    FROM {{ ref('stg_sql_server_dbo__products') }}
),

stg_google_sheets__budget AS (
    SELECT DISTINCT product_id,date_load
    FROM {{ ref('stg_google_sheets__budget') }}
),

stg_sql_server_dbo__events AS (
    SELECT DISTINCT product_id,date_load
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

products_all_with_duplicates AS (
    SELECT *
    FROM stg_sql_server_dbo__products
    UNION ALL
    SELECT *
    FROM stg_google_sheets__budget
    UNION ALL
    SELECT *
    FROM stg_sql_server_dbo__events
),

removing_duplicates AS (
    SELECT DISTINCT product_id
    FROM products_all_with_duplicates
)

    SELECT 
         product_id,
         products_name,
         price_usd,
         inventory,
         date_load

    FROM removing_duplicates
    FULL JOIN
    {{ ref('stg_sql_server_dbo__products') }} AS stg_sql_server_dbo__products
    USING (product_id)

{% endsnapshot %}