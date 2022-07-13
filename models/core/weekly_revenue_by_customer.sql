with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="week",
        start_date="cast('2021-01-01' as date)",
        end_date="current_date"
    ) }}
    ),

--weekly revenue by customer
weekly as (
    select
        date_trunc(created_at, week(friday)) as date_week,
        customer_id,
        sum(total) as weekly_revenue,
    from {{ ref('stg_orders') }}

    group by date_trunc(created_at, week(friday)), customer_id
),

--all customers an all weeks since each customer's first order
customer_date_spine as (
    select distinct
        date_spine.date_week, 
        weekly.customer_id 
    from date_spine 
    cross join weekly
    where timestamp(date_spine.date_week) >= weekly.date_week
),

--weekly revenue for each customer and 0 where no orders
revenue as (
    select
        customer_date_spine.date_week,
        customer_date_spine.customer_id,
        coalesce(weekly_revenue,0) as weekly_revenue,
    from customer_date_spine

    left join weekly
        on timestamp(customer_date_spine.date_week) = weekly.date_week
        and customer_date_spine.customer_id = weekly.customer_id
),

--get week number and cumulative revenue
final as (

    select 
        date_week,
        row_number() over (partition by customer_id order by date_week) as week,
        customer_id,
        weekly_revenue,
        sum(weekly_revenue) over (partition by customer_id order by date_week) as cumulative_revenue,
    from revenue
    
)

select * from final order by date_week

