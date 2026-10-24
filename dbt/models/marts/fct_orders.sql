{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- Incremental with late-data handling: on each run, any order that received an event in
-- the lookback window is rebuilt from *all* of its events, so late arrivals and
-- out-of-order lifecycle updates correct the existing row instead of being dropped.
with touched_orders as (
    select distinct order_id
    from {{ ref('stg_order_events') }}
    where order_id is not null
    {% if is_incremental() %}
        and ingested_at > (
            select max(last_ingested_at) - interval {{ var('incremental_lookback_minutes') }} minute
            from {{ this }}
        )
    {% endif %}
),

orders as (
    select lifecycle.*
    from {{ ref('int_order_lifecycle') }} as lifecycle
    inner join touched_orders using (order_id)
)

select
    order_id,
    store_id,
    session_id,
    customer_id,
    channel,
    case
        when cancelled_at is not null then 'cancelled'
        when picked_up_at is not null then 'completed'
        when ready_at is not null then 'ready'
        when paid_at is not null then 'preparing'
        when placed_at is not null then 'awaiting_payment'
        else 'incomplete'  -- order_placed never arrived (e.g. dead-lettered)
    end as order_status,
    cancel_reason,
    payment_method,
    payment_failures,
    order_amount,
    case
        when paid_at is not null and cancelled_at is null then coalesce(order_amount, 0) else 0
    end as net_revenue,  -- amount unknown when order_placed was dead-lettered
    placed_at,
    paid_at,
    ready_at,
    picked_up_at,
    cancelled_at,
    cast({{ to_local('coalesce(placed_at, paid_at, ready_at)') }} as date) as order_date_local,
    hour({{ to_local('coalesce(placed_at, paid_at, ready_at)') }}) as order_hour_local,
    date_diff('second', paid_at, ready_at) as prep_seconds,
    date_diff('second', ready_at, picked_up_at) as pickup_wait_seconds,
    date_diff('second', paid_at, ready_at) > {{ var('prep_sla_minutes') }} * 60
        as is_prep_sla_breached,
    event_count,
    late_event_count,
    last_ingested_at
from orders
