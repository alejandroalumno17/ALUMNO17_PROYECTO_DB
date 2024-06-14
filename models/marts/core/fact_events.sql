{{
  config(
    materialized='table'
  )
}}

WITH stg_sql_server_dbo__events AS 
(
    SELECT *
    FROM {{ ref("stg_sql_server_dbo__events") }}
)

SELECT
    event_id,
    event_type_id,
    page_url,
    user_id,
    session_id,
    order_id,
    product_id,
    created_at_date,
    created_at_time_utc
FROM stg_sql_server_dbo__events