-- One row per session with the furthest funnel stage it reached. A stage counts as reached
-- if any later stage happened, so a dead-lettered event cannot break the funnel.
with flags as (
    select
        session_id,
        max(store_id) as store_id,
        max(channel) as channel,
        max(customer_id) as customer_id,
        min(event_ts) as session_started_at,
        bool_or(event_type = 'menu_viewed') as menu_viewed,
        bool_or(event_type = 'item_added') as item_added,
        bool_or(event_type = 'checkout_started') as checkout_started,
        bool_or(event_type in (
            'order_placed', 'payment_failed', 'payment_succeeded',
            'order_ready', 'order_picked_up', 'order_cancelled'
        )) as order_placed,
        coalesce(sum(quantity) filter (where event_type = 'item_added'), 0) as cart_units,
        max(order_id) as order_id
    from {{ ref('stg_order_events') }}
    group by session_id
)

select
    session_id,
    store_id,
    channel,
    customer_id,
    session_started_at,
    order_id,
    cart_units,
    order_placed as reached_order,
    checkout_started or order_placed as reached_checkout,
    item_added or checkout_started or order_placed as reached_cart,
    menu_viewed or item_added or checkout_started or order_placed as reached_menu
from flags
