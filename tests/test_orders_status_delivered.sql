SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE status = 'delivered'  AND estimated_delivery_at_date is null 
                            AND delivered_at_date is null
                            AND estimated_delivery_at_time_utc is null 
                            AND delivered_at_time_utc is null  
                            AND tracking_id ='' 
                            AND shipping_service=''