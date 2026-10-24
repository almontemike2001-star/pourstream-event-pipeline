select
    events.event_id as cart_item_id,
    events.session_id,
    events.store_id,
    events.channel,
    events.item_sku,
    menu.item_name,
    menu.category,
    events.quantity,
    events.quantity * menu.unit_price as line_amount,
    cast(events.event_ts_local as date) as added_date_local,
    sessions.reached_order as converted_to_order
from {{ ref('stg_order_events') }} as events
inner join {{ ref('menu_items') }} as menu on events.item_sku = menu.sku
inner join {{ ref('int_sessions') }} as sessions using (session_id)
where events.event_type = 'item_added'
