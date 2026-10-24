-- Same rule as int_sessions: a later stage implies the earlier ones, so a dead-lettered
-- payment_succeeded event does not make a completed order look unpaid.
select
    sessions.*,
    stores.store_name,
    stores.region,
    cast({{ to_local('session_started_at') }} as date) as session_date_local,
    orders.order_status,
    coalesce(orders.order_status = 'completed', false) as reached_pickup,
    coalesce(
        orders.paid_at is not null
        or orders.ready_at is not null
        or orders.picked_up_at is not null,
        false
    ) as reached_payment
from {{ ref('int_sessions') }} as sessions
left join {{ ref('fct_orders') }} as orders using (order_id)
left join {{ ref('stores') }} as stores on sessions.store_id = stores.store_id
