{{
  config(
    materialized='table'
  )
}}

WITH stg_sql_server_dbo__events_types AS (
    SELECT DISTINCT event_type_id, event_type
    FROM {{ ref('stg_sql_server_dbo__events') }}
)

SELECT 
     event_type_id,
     event_type
FROM stg_sql_server_dbo__events_types