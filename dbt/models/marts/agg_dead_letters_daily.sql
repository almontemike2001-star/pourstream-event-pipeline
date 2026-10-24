select
    cast({{ to_local('arrived_at') }} as date) as arrival_date_local,
    error_type,
    count(*) as dead_lettered_messages,
    -- see agg_pipeline_health.is_spillover_day
    cast({{ to_local('arrived_at') }} as date) > (
        select max(cast(event_ts_local as date)) from {{ ref('stg_order_events') }}
    ) as is_spillover_day
from {{ ref('stg_dead_letters') }}
group by all
