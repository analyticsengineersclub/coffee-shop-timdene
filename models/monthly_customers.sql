select  
    date_trunc(first_order_at, month) as month_date,
    count(*) as total_customers,
from {{ ref('customers')}}
group by date_trunc(first_order_at, month)