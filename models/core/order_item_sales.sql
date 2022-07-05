with customer_orders as (
--index customer orders to get new and returning
  select
    order_id,
    created_at,
    total,
    row_number() over (partition by customer_id order by created_at) as order_index,
  from {{ ref('stg_orders') }}
),

prices as (
--get product prices for each order item
  select
    orders.order_id,
    order_items.order_item_id,
    product_prices.price as time_precision_price,
    missing_prices.price as day_precision_price,
  from {{ ref('stg_orders') }} as orders
  
  left join {{ ref('stg_order_items') }} as order_items
    on orders.order_id = order_items.order_id
  
  left join {{ ref('stg_product_prices') }} as product_prices
    on order_items.product_id = product_prices.product_id
    and orders.created_at between product_prices.created_at and product_prices.ended_at
  
  --get the most recent price when order occurs before product_price table reflects price for that day. assumes product_prices updates daily
  left join {{ ref('stg_product_prices') }} as missing_prices
    on order_items.product_id = missing_prices.product_id
    and date_trunc(orders.created_at, day) = date_trunc(missing_prices.ended_at, day)

),

product as (
--get product name and category
  select distinct
    order_items.order_item_id,
    product.product_category,
    product.product_name,
  from {{ ref('stg_order_items') }} as order_items
  
  left join {{ ref('stg_products') }} as product
    on order_items.product_id = product.product_id
),

final as (
  select
    prices.order_item_id,
    customer_orders.order_id,
    if(customer_orders.order_index = 1, 'new','returning') as returning_customer,
    product.product_category,
    product.product_name,
    customer_orders.total as order_total,
    --if order occurs before product_price table reflects price for that day return the last day's price. assumes product_prices updates daily
    if(prices.time_precision_price is null, prices.day_precision_price, prices.time_precision_price) as order_item_total,
    customer_orders.created_at as created_at,

  from prices
  
  left join customer_orders
    on customer_orders.order_id = prices.order_id

  left join product
    on prices.order_item_id = product.order_item_id

)

select * from final