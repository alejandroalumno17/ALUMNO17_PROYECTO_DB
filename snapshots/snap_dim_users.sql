{% snapshot snap_dim_users %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='timestamp',
      updated_at='date_load'
    )
}}


WITH distinct_stg_sql_server_dbo__users AS
(
    SELECT DISTINCT(user_id)
    FROM {{ ref('stg_sql_server_dbo__users') }}
),

distinct_stg_sql_server_dbo__events AS 
(
    SELECT DISTINCT(user_id)
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

distinct_stg_sql_server_dbo__orders AS 
(
    SELECT DISTINCT(user_id)
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

union_all_with_duplicates AS 
(
    SELECT *
    FROM distinct_stg_sql_server_dbo__users
    UNION ALL
    SELECT *
    FROM distinct_stg_sql_server_dbo__events
    UNION ALL
    SELECT *
    FROM distinct_stg_sql_server_dbo__orders
),

removing_duplicates AS 
(
    SELECT DISTINCT(user_id)
    FROM union_all_with_duplicates
)

SELECT *
FROM removing_duplicates
FULL JOIN
{{ ref('stg_sql_server_dbo__users') }} AS users
USING (user_id)

{% endsnapshot %}