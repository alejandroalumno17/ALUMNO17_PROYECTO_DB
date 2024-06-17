SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }}
WHERE  status='preparing' and tracking_id is null