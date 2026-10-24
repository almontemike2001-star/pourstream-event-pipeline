-- Lifecycle timestamps must never go backwards: placed <= paid <= ready <= picked up.
select order_id
from {{ ref('fct_orders') }}
where paid_at < placed_at
   or ready_at < paid_at
   or picked_up_at < ready_at
