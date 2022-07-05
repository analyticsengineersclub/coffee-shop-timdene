with source as (
    select * from {{ source('web_tracking', 'pageviews') }}
),

renamed as (
    select
        id as pageview_id,
        visitor_id,
        customer_id,
        page as url_path,
        device_type,
        timestamp,
    from source
)

select * from renamed