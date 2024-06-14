{{
  config(
    materialized='view'
  )
}}

with 

source as (

    select status from {{ ref('base_sql_server_dbo__orders') }}

),

renamed as (

    select
        distinct md5(status) as order_status_id,
        status

    from source
    union all
    select
        md5('sin_status'),
        'Sin estado'

)

select * from renamed