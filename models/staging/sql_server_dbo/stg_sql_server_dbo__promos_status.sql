{{
  config(
    materialized='view'
  )
}}

with 

source as (

    select * from {{ ref('base_sql_server_dbo__promos') }}

),

renamed as (

    select
        IFF(promo_status = 'active', 1, 0)::number(2,0) as promo_status_id,
        promo_status

    from source
    group by promo_status
    
)

select * from renamed