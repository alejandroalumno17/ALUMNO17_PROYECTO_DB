{{
  config(
    materialized='table'
  )
}}

WITH snap_dim_users AS 
(
    SELECT * 
    FROM {{ ref('snap_dim_users') }}
)

    select     
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_id_key,
    first_name,
    last_name,
    address_id,
    email,
    phone_number,
    created_at_date,
    created_at_time_utc,
    updated_at_date,
    updated_at_time_utc
    from snap_dim_users
    where dbt_valid_to is null