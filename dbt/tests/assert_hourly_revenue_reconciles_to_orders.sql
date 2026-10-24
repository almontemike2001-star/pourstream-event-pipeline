-- The hourly mart must not lose or double-count revenue relative to the order fact.
with fact as (
    select sum(net_revenue) as revenue from {{ ref('fct_orders') }} where order_date_local is not null
),
mart as (
    select sum(net_revenue) as revenue from {{ ref('agg_store_hourly') }}
)
select fact.revenue, mart.revenue
from fact cross join mart
where fact.revenue is distinct from mart.revenue
