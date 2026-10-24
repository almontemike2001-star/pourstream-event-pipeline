-- One row per event_id. Duplicate deliveries are collapsed onto the first arrival.
with deliveries as (
    select
        *,
        count(*) over (partition by event_id) as delivery_count,
        row_number() over (
            partition by event_id
            order by _message_ts, _ingested_at, _partition, _offset
        ) as delivery_rank
    from {{ source('bronze', 'order_events') }}
),

first_delivery as (
    select
        event_id,
        event_type,
        timezone('UTC', event_ts) as event_ts,
        session_id,
        customer_id,
        store_id,
        channel,
        order_id,
        item_sku,
        quantity,
        amount,
        payment_method,
        cancel_reason,
        timezone('UTC', _message_ts) as first_arrived_at,
        timezone('UTC', _ingested_at) as ingested_at,
        delivery_count
    from deliveries
    where delivery_rank = 1
)

select
    *,
    {{ to_local('event_ts') }} as event_ts_local,
    date_diff('second', event_ts, first_arrived_at) as arrival_lag_seconds,
    date_diff('second', event_ts, first_arrived_at) > {{ var('late_arrival_minutes') }} * 60
        as is_late
from first_delivery
