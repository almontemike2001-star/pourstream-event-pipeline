select
    session_date_local,
    channel,
    store_name,
    region,
    count(*) as sessions,
    count(*) filter (where reached_menu) as menu_views,
    count(*) filter (where reached_cart) as carts,
    count(*) filter (where reached_checkout) as checkouts,
    count(*) filter (where reached_order) as orders,
    count(*) filter (where reached_payment) as paid_orders,
    count(*) filter (where reached_pickup) as picked_up_orders,
    round(count(*) filter (where reached_order) / count(*), 4) as session_to_order_rate,
    round(
        count(*) filter (where reached_order) / nullif(count(*) filter (where reached_cart), 0), 4
    ) as cart_to_order_rate
from {{ ref('fct_sessions') }}
group by all
