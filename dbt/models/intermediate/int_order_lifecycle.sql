-- Pivot an order's events into one row of lifecycle timestamps.
-- Event arrival order is not trusted: every timestamp comes from event_ts.
select
    order_id,
    max(store_id) as store_id,
    max(session_id) as session_id,
    max(customer_id) as customer_id,
    max(channel) as channel,
    min(event_ts) filter (where event_type = 'order_placed') as placed_at,
    max(amount) filter (where event_type = 'order_placed') as order_amount,
    min(event_ts) filter (where event_type = 'payment_succeeded') as paid_at,
    max(payment_method) filter (
        where event_type in ('payment_succeeded', 'payment_failed')
    ) as payment_method,
    count(*) filter (where event_type = 'payment_failed') as payment_failures,
    min(event_ts) filter (where event_type = 'order_ready') as ready_at,
    min(event_ts) filter (where event_type = 'order_picked_up') as picked_up_at,
    min(event_ts) filter (where event_type = 'order_cancelled') as cancelled_at,
    max(cancel_reason) filter (where event_type = 'order_cancelled') as cancel_reason,
    count(*) as event_count,
    count(*) filter (where is_late) as late_event_count,
    max(ingested_at) as last_ingested_at
from {{ ref('stg_order_events') }}
where order_id is not null
group by order_id
