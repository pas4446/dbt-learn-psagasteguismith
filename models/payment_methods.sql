{# {% set payment_methoods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] %} #}
{% set payment_methoods = dbt_utils.get_column_values(table = ref('stg_payments'), column = 'payment_method') %} 

select order_id
  {%- for payment_method in payment_methoods -%}
  ,sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
  {% endfor -%}
from {{ ref('stg_payments')}}
group by order_id

-- select order_id,
--   {% for payment_method in payment_methoods %}
--   sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
--   {% if not loop.last %}, {% endif %}
--   {% endfor %}
-- from {{ ref('stg_payments')}}
-- group by order_id