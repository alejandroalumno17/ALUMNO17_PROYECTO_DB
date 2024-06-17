{{
  config(
    materialized='view'
  )
}}

with 

source as (

    select * from {{ source('sql_server_dbo', 'orders') }}

),

renamed as (

    select
        order_id,
        shipping_service,
        shipping_cost as shipping_cost_euro,
        address_id,
        {{ to_utc('created_at') }} as created_at_time_utc,
        promo_id,
        {{ to_utc('estimated_delivery_at') }} as estimated_delivery_at_time_utc,
        order_cost as order_cost_euro,
        user_id,
        order_total as order_total_euro,
        {{ to_utc('delivered_at') }} as delivered_at_time_utc,
        IFF(tracking_id = '', null, tracking_id) as tracking_id,
        status,
        _fivetran_deleted,
        {{ to_utc('_fivetran_synced') }} as date_load

    from source

)

select * from renamed