{{
  config(
    materialized='table'
  )
}}

WITH stg_sql_server_dbo__vendedores AS (
    SELECT DISTINCT *
    FROM {{ ref('stg_sql_server_dbo__vendedores') }}
)

SELECT 
    vendedor_id,
    first_name,
    last_name,
    salary,
    commission,
    address_id,
    contract_start_date_at_date,
    contract_start_date_at_time_utc,
    contract_end_date_at_date,
    contract_end_date_at_time_utc,
    date_load 
FROM stg_sql_server_dbo__vendedores