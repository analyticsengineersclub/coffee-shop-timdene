with user_stitch as (

  select distinct
    visitor_id,
    customer_id,
  from {{ ref('stg_pageviews') }}
  where customer_id is not null

)

select
  pageview_id,
  user_stitch.customer_id as customer_visitor_id,
  pageview.visitor_id as network_visitor_id,
  pageview.customer_id,
  url_path,
  device_type,
  timestamp,

from {{ ref('stg_pageviews') }} pageview
left join user_stitch
  on pageview.visitor_id = user_stitch.visitor_id