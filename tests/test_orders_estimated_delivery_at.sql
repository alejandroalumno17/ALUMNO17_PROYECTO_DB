SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }}
WHERE  estimated_delivery_at_date < created_at_date AND estimated_delivery_at_time_utc < created_at_time_utc