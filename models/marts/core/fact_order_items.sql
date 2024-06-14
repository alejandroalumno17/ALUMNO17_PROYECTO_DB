{{
  config(
    materialized='incremental',
    unique_key='order_item_id',
  )
}}

WITH stg_sql_server_dbo__order_items AS
(
    SELECT *
    FROM {{ref("snap_fact_order_items")}}

    {% if is_incremental() %}

	  where snap_fact_order_items.date_load > (select max(this.date_load) from {{ this }} as this)

    {% endif %}
)


SELECT *
FROM stg_sql_server_dbo__order_items
WHERE dbt_valid_to IS NULL