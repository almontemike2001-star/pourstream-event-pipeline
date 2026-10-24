select session_date_local, channel
from {{ ref('agg_daily_funnel') }}
where not (
    sessions >= menu_views
    and menu_views >= carts
    and carts >= checkouts
    and checkouts >= orders
    and orders >= paid_orders
    and paid_orders >= picked_up_orders
)
