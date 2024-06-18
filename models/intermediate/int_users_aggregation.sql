WITH users AS (
    SELECT * 
    FROM {{ref('stg_sql_server_dbo__users')}}
),

orders AS (
    SELECT *
    FROM {{ref('stg_sql_server_dbo__orders')}}
),

order_items AS (
    SELECT *
    FROM {{ref('stg_sql_server_dbo__order_items')}}
),

final AS (
    SELECT
        u.user_id,
        COUNT(o.order_id) AS total_orders,
        ROUND(SUM(o.order_cost_euro),2) AS total_orders_cost,
        ROUND(SUM(o.shipping_cost_euro),2) AS total_shipping_cost,
        SUM(oi.quantity) AS total_products,
        COUNT(oi.product_id) AS different_products,
        ROUND(SUM(o.order_total_euro - (o.shipping_cost_euro + o.order_cost_euro)),2) AS total_discounts
    FROM users u 
    LEFT JOIN orders o ON u.user_id = o.user_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY 1
)

SELECT * FROM final