SELECT
    order_cost_euro,
    shipping_cost_euro,
    order_total_euro,
    discount
FROM {{ ref('stg_sql_server_dbo__orders') }}
JOIN {{ ref('stg_sql_server_dbo__promos') }}
USING(promo_id)
WHERE (order_cost_euro + shipping_cost_euro) - discount != order_total_euro
