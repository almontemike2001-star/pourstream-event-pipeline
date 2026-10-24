select
    store_id,
    store_name,
    region,
    store_format,
    order_date_local,
    order_hour_local,
    count(*) as orders,
    count(*) filter (where paid_at is not null) as paid_orders,
    count(*) filter (where order_status = 'completed') as completed_orders,
    count(*) filter (where order_status = 'cancelled') as cancelled_orders,
    sum(net_revenue) as net_revenue,
    round(avg(prep_seconds) / 60.0, 2) as avg_prep_minutes,
    round(quantile_cont(prep_seconds, 0.9) / 60.0, 2) as p90_prep_minutes,
    round(avg(case when is_prep_sla_breached then 1.0 else 0.0 end)
        filter (where prep_seconds is not null), 4) as prep_sla_breach_rate
from {{ ref('fct_orders') }}
where order_date_local is not null
group by all
