{{
  config(
    materialized = 'view',
    )
}}

--USER STITCHING

with user_visitor as (
--get all visitor_id customer_id pairings
    select distinct
        visitor_id,
        customer_id,
    from {{ ref('stg_pageviews') }}
    where customer_id is not null

),

user_stitch as (
--create customer_visitor_id for each known visitor
    select
        pageview_id,
        coalesce(user_visitor.customer_id, user_visitor.visitor_id) as customer_visitor_id,
        pageview.visitor_id as network_visitor_id,
        timestamp,
    from {{ ref('stg_pageviews') }} pageview
    left join user_visitor
        on pageview.visitor_id = user_visitor.visitor_id
),



--SESSIONIZE

time_between as (
--get time between pageviews
    select
        pageview_id,
        customer_visitor_id,
        unix_seconds(timestamp) as unix_timestamp,
        unix_seconds(lag(timestamp) over (partition by customer_visitor_id order by timestamp)) as lag_timestamp,
    from user_stitch
),

session_increment as (
--make session increment 1 where pageview is first ever for visitor, or is 30 minutes after previous pageview
    select 
        pageview_id,
        customer_visitor_id,
        unix_timestamp,
        case 
            when lag_timestamp is null then 1
            when unix_timestamp - lag_timestamp > 1800 then 1
            else 0 
            end as increment,
    from time_between
),

sessionize as (
--create session id by incrementing and concat with customer_visitor_id
    select
        pageview_id,
        customer_visitor_id || '-' || sum(increment) over (partition by customer_visitor_id order by unix_timestamp) as session_id
    from session_increment
)


--FINAL

select
  pageview.pageview_id,
  sessionize.session_id,
  user_stitch.customer_visitor_id,
  pageview.visitor_id as network_visitor_id,
  pageview.customer_id,
  pageview.url_path,
  pageview.device_type,
  pageview.timestamp,
  min(pageview.timestamp) over (partition by sessionize.session_id) as session_start_at,
  max(pageview.timestamp) over (partition by sessionize.session_id) as session_end_at,

from {{ ref('stg_pageviews') }} pageview

left join user_stitch
  on pageview.pageview_id = user_stitch.pageview_id

left join sessionize
  on pageview.pageview_id = sessionize.pageview_id
