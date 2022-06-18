{{
    config(
        materialized='table'
    )
}}

with customer_orders as (
  select
    customer_id,
    count(*) as n_orders,
    min(created_at) as first_order_at,
  from {{ source('coffee_shop','orders') }}
  group by
    customer_id

)

select 
  customers.id as customer_id,
  customers.name,
  customers.email,
  coalesce(customer_orders.n_orders,0) as number_of_orders,
  customer_orders.first_order_at
from {{ source('coffee_shop','customers') }} customers
left join customer_orders
  on customers.id = customer_orders.customer_id
  