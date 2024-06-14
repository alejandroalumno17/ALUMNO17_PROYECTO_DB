{{
  config(
    materialized='table'
  )
}}

WITH stg_sql_server_dbo__addresses AS 
(
    SELECT DISTINCT address_id
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),

stg_sql_server_dbo__users AS 
(
    SELECT DISTINCT address_id 
    FROM {{ ref('stg_sql_server_dbo__users') }}
),

address_all_with_duplicates AS 
(
    SELECT *
    FROM stg_sql_server_dbo__addresses
    UNION ALL
    SELECT *
    FROM stg_sql_server_dbo__users
),

removing_duplicates AS 
(
    SELECT DISTINCT(address_id)
    FROM address_all_with_duplicates
)

SELECT
      address_id,
      address,
      zipcode,
      state,
      country,
      date_load
FROM removing_duplicates
FULL JOIN {{ ref('stg_sql_server_dbo__addresses') }}
USING(address_id)