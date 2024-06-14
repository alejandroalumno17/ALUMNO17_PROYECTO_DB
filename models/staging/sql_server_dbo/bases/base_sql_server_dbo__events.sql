{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('sql_server_dbo', 'events') }}
),

renamed as (
    select
        event_id,
        page_url,
        event_type,
        user_id,
        product_id,
        session_id,
        convert_timezone('UTC', created_at) as created_at_time_utc,
        order_id,
        _fivetran_deleted,
        convert_timezone('UTC', _fivetran_synced) as date_load
    from source
)

select * from renamed
