{{
  config(
    materialized='view'
  )
}}

{{
  config(
    materialized='view'
  )
}}

with 

source as (

    select * from {{ source('sql_server_dbo', 'promos') }}

),

renamed as (

    select
        distinct md5(promo_id) as promo_id,
        promo_id as promo_name,
        discount,
        status as promo_status,
        _fivetran_deleted,
        {{ to_utc('_fivetran_synced') }} as _fivetran_synced_utc

    from source

)

select * from renamed