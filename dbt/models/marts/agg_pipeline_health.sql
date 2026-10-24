-- Stream quality by arrival date: duplicates, late arrivals, contract rejections, lag.
with ranked_deliveries as (
    select
        _message_ts,
        row_number() over (
            partition by event_id order by _message_ts, _ingested_at, _partition, _offset
        ) as delivery_rank
    from {{ source('bronze', 'order_events') }}
),

deliveries as (
    select
        cast({{ to_local("timezone('UTC', _message_ts)") }} as date) as arrival_date_local,
        count(*) as delivered_messages,
        count(*) filter (where delivery_rank > 1) as duplicate_deliveries
    from ranked_deliveries
    group by 1
),

events as (
    select
        cast({{ to_local('first_arrived_at') }} as date) as arrival_date_local,
        count(*) as unique_events,
        count(*) filter (where is_late) as late_events,
        quantile_cont(arrival_lag_seconds, 0.5) as p50_arrival_lag_seconds,
        quantile_cont(arrival_lag_seconds, 0.95) as p95_arrival_lag_seconds,
        max(arrival_lag_seconds) as max_arrival_lag_seconds
    from {{ ref('stg_order_events') }}
    group by 1
),

dead_letters as (
    select
        cast({{ to_local('arrived_at') }} as date) as arrival_date_local,
        count(*) as dead_lettered_messages
    from {{ ref('stg_dead_letters') }}
    group by 1
)

select
    arrival_date_local,
    coalesce(d.delivered_messages, 0) + coalesce(dl.dead_lettered_messages, 0) as total_messages,
    coalesce(e.unique_events, 0) as unique_events,
    coalesce(d.duplicate_deliveries, 0) as duplicate_deliveries,
    coalesce(e.late_events, 0) as late_events,
    coalesce(dl.dead_lettered_messages, 0) as dead_lettered_messages,
    round(e.late_events / nullif(e.unique_events, 0), 4) as late_event_rate,
    round(
        coalesce(dl.dead_lettered_messages, 0)
        / nullif(coalesce(d.delivered_messages, 0) + coalesce(dl.dead_lettered_messages, 0), 0),
        4
    ) as dead_letter_rate,
    e.p50_arrival_lag_seconds,
    e.p95_arrival_lag_seconds,
    e.max_arrival_lag_seconds
from deliveries as d
full outer join events as e using (arrival_date_local)
full outer join dead_letters as dl using (arrival_date_local)
