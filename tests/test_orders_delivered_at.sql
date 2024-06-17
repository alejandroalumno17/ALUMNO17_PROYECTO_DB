SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }}
WHERE delivered_at_date < created_at_date AND delivered_at_time_utc < created_at_time_utc


