{% set events_types = ["checkout", "package_shipped", "add_to_cart","page_view"] %}
WITH stg_sql_server_dbo__events AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__events') }}
),

renamed_casted AS (
    SELECT
        user_id,
        {%- for event_type in events_types   %}
        sum(case when event_type = '{{event_type}}' then 1 end) as {{event_type}}_amount
        {%- if not loop.last %},{% endif -%}
        {% endfor %}
    FROM stg_sql_server_dbo__events
    GROUP BY 1
    )

SELECT * FROM renamed_casted