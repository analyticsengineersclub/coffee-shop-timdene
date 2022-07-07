with session_length as (
    select distinct
        session_id,
        unix_millis(session_end_at) as millis_end_at,
        unix_millis(session_start_at) as millis_start_at,
    from {{ ref('pageviews') }}
),

pages_visited as (
    select
        session_id,
        count(distinct url_path) total_pages
    from {{ ref('pageviews') }}
    group by session_id
),

purchases as (
    select distinct
        session_id,
    from {{ ref('pageviews') }}
    where url_path like "order-confirmation"
)

select
    session_length.session_id,
    millis_end_at - millis_start_at as session_duration,
    pages_visited.total_pages as total_pages_visited,
    if(purchases.session_id is null, false, true ) as has_purchase,
    
from session_length

join pages_visited
    on session_length.session_id = pages_visited.session_id 

left join purchases
    on session_length.session_id = purchases.session_id
    