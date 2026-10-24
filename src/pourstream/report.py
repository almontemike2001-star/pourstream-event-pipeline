"""Render a self-contained HTML KPI report from the gold marts."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

QUERIES = {
    "kpis": """
        select
            count(*) as orders,
            sum(net_revenue) as net_revenue,
            avg(case when order_status = 'completed' then 1.0 else 0.0 end) as completion_rate,
            avg(case when order_status = 'cancelled' then 1.0 else 0.0 end) as cancellation_rate,
            avg(prep_seconds) / 60.0 as avg_prep_minutes,
            avg(case when is_prep_sla_breached then 1.0 else 0.0 end)
                filter (where prep_seconds is not null) as sla_breach_rate
        from gold.fct_orders
    """,
    "health_totals": """
        select sum(total_messages) as total_messages, sum(unique_events) as unique_events,
               sum(duplicate_deliveries) as duplicates, sum(late_events) as late_events,
               sum(dead_lettered_messages) as dead_letters,
               (select quantile_cont(arrival_lag_seconds, 0.95)
                from silver.stg_order_events) as p95_lag_seconds
        from gold.agg_pipeline_health
    """,
    "daily_revenue": """
        select order_date_local as day, sum(net_revenue) as revenue, sum(orders) as orders
        from gold.agg_store_hourly group by 1 order by 1
    """,
    "hourly_orders": """
        select order_hour_local as hour, sum(orders) as orders,
               round(sum(p90_prep_minutes * orders) / sum(orders), 2) as p90_prep_minutes
        from gold.agg_store_hourly group by 1 order by 1
    """,
    "funnel": """
        select sum(sessions) as "Sessions", sum(menu_views) as "Viewed menu",
               sum(carts) as "Added to cart", sum(checkouts) as "Checkout",
               sum(orders) as "Placed order", sum(paid_orders) as "Paid",
               sum(picked_up_orders) as "Picked up"
        from gold.agg_daily_funnel
    """,
    "store_sla": """
        select s.store_name as store, s.format,
               round(avg(case when o.is_prep_sla_breached then 1.0 else 0.0 end), 4) as breach_rate,
               round(avg(o.prep_seconds) / 60.0, 2) as avg_prep_minutes,
               count(*) as orders
        from gold.fct_orders o join gold.dim_stores s using (store_id)
        where o.prep_seconds is not null
        group by all order by breach_rate desc
    """,
    "health_daily": """
        select arrival_date_local as day, unique_events, duplicate_deliveries,
               dead_lettered_messages, late_events, late_event_rate, dead_letter_rate
        from gold.agg_pipeline_health order by 1
    """,
    "top_items": """
        select item_name as item, category, sum(quantity) as units,
               round(avg(case when converted_to_order then 1.0 else 0.0 end), 4) as conversion
        from gold.fct_cart_items group by all order by units desc limit 10
    """,
    "dlq": """
        select error_type, count(*) as messages from silver.stg_dead_letters
        group by 1 order by 2 desc
    """,
}


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


def collect(warehouse: Path) -> dict[str, list[dict]]:
    with duckdb.connect(str(warehouse), read_only=True) as con:
        data = {}
        for name, sql in QUERIES.items():
            cur = con.execute(sql)
            cols = [c[0] for c in cur.description]
            data[name] = [
                {c: _jsonable(v) for c, v in zip(cols, row, strict=True)} for row in cur.fetchall()
            ]
    return data


def render(warehouse: Path, out: Path) -> Path:
    data = collect(warehouse)
    template = (Path(__file__).parent / "report_template.html").read_text()
    html = template.replace("__DATA__", json.dumps(data))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    return out
