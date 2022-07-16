select
  date_trunc(created_at, month) as date_month,
  {% set product_categories = ['coffee beans', 'merch', 'brewing supplies'] -%}
  {% for product_category in product_categories -%}
  sum(case 
    when product_category =  '{{ product_category }}' then order_total 
    end) as {{ product_category|replace(' ','_') }}_total,
  {% endfor %}
from {{ ref('order_item_sales') }}
group by 1