{% snapshot snap_dim_vendedores %}

{{
    config(
      target_schema='snapshots',
      unique_key='vendedor_id',
      strategy='timestamp',
      updated_at='date_load',
    )
}}

WITH distinct_stg_sql_server_dbo__vendedores AS (
    SELECT DISTINCT (vendedor_id)
    FROM {{ ref('stg_sql_server_dbo__vendedores') }}
),

distinct_stg_sql_server_dbo__orders AS (
    SELECT DISTINCT (vendedor_id)
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

union_all_with_duplicates AS 
(
    SELECT *
    FROM distinct_stg_sql_server_dbo__vendedores
    UNION ALL
    SELECT *
    FROM distinct_stg_sql_server_dbo__orders
),

removing_duplicates AS 
(
    SELECT DISTINCT(vendedor_id)
    FROM union_all_with_duplicates
)

SELECT *
FROM removing_duplicates
FULL JOIN
{{ ref('stg_sql_server_dbo__vendedores') }} AS vendedores
USING (vendedor_id)



{% endsnapshot %}