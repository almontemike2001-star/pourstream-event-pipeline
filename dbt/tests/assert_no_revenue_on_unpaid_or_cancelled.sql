select order_id
from {{ ref('fct_orders') }}
where net_revenue <> 0
  and (paid_at is null or order_status = 'cancelled')
