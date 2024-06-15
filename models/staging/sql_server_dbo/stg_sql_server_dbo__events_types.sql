{{
  config(
    materialized='view'
  )
}}

with 

source as (

    select event_type from {{ ref('base_sql_server_dbo__events') }}

),

renamed as (

    select
        distinct md5(event_type) as event_type_id,
        event_type

    from source

)

select * from renamed