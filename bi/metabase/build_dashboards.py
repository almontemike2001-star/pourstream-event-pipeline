"""Build the PourStream Metabase dashboards through the Metabase API (dashboards as code).

Idempotent: on every run it (1) completes first-time setup or logs in, (2) registers the
Postgres serving DB, (3) archives the previous PourStream questions/dashboards and recreates
them, and (4) enables public links so the dashboards can be screenshotted or embedded.

    python bi/metabase/build_dashboards.py            # reads bi/metabase/.env
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
MB_URL = os.environ.get("MB_URL", "http://localhost:3001")
COLLECTION = "PourStream"
DB_NAME = "PourStream Serving (gold)"

# Palette from the report (validated for colour-vision deficiency, light mode).
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
NO_OTHER = {"graph.max_categories_enabled": True, "graph.max_categories": 50}

# --------------------------------------------------------------------------- http


def _env() -> dict[str, str]:
    env = {}
    for line in (HERE / ".env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return {**env, **os.environ}


class Metabase:
    def __init__(self, url: str):
        self.url = url
        self.session: str | None = None

    def call(self, method: str, path: str, body: dict | None = None):
        req = urllib.request.Request(
            f"{self.url}/api{path}",
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"}
            | ({"X-Metabase-Session": self.session} if self.session else {}),
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"{method} {path} -> {exc.code}: {exc.read()[:500]!r}") from exc
        return json.loads(raw) if raw else None

    get = lambda self, p: self.call("GET", p)  # noqa: E731
    post = lambda self, p, b=None: self.call("POST", p, b or {})  # noqa: E731
    put = lambda self, p, b: self.call("PUT", p, b)  # noqa: E731


def connect(env: dict[str, str]) -> Metabase:
    mb = Metabase(MB_URL)
    for _ in range(60):
        try:
            if mb.get("/health")["status"] == "ok":
                break
        except Exception:
            pass
        time.sleep(5)
    props = mb.get("/session/properties")
    email, password = env["MB_ADMIN_EMAIL"], env["MB_ADMIN_PASSWORD"]
    if props.get("setup-token") and not props.get("has-user-setup"):
        result = mb.post(
            "/setup",
            {
                "token": props["setup-token"],
                "user": {
                    "email": email,
                    "password": password,
                    "first_name": "PourStream",
                    "last_name": "Admin",
                    "site_name": "PourStream",
                },
                "prefs": {"site_name": "PourStream", "allow_tracking": False},
            },
        )
        mb.session = result["id"]
    else:
        mb.session = mb.post("/session", {"username": email, "password": password})["id"]
    return mb


# --------------------------------------------------------------------------- setup


def ensure_database(mb: Metabase, env: dict[str, str]) -> int:
    existing = mb.get("/database")
    existing = existing.get("data", existing)
    for db in existing:
        if db["name"] == DB_NAME:
            mb.post(f"/database/{db['id']}/sync_schema")
            return db["id"]
    db = mb.post(
        "/database",
        {
            "engine": "postgres",
            "name": DB_NAME,
            "details": {
                "host": "serving-db",
                "port": 5432,
                "dbname": "analytics",
                "user": env["SERVING_DB_USER"],
                "password": env["SERVING_DB_PASSWORD"],
                "schema-filters-type": "inclusion",
                "schema-filters-patterns": "gold",
                "ssl": False,
            },
        },
    )
    return db["id"]


def field_ids(mb: Metabase, db_id: int, expected_tables: set[str]) -> dict[tuple[str, str], int]:
    for _ in range(60):
        meta = mb.get(f"/database/{db_id}/metadata")
        fields = {
            (t["name"], f["name"]): f["id"]
            for t in meta["tables"]
            if t.get("schema") == "gold"
            for f in t["fields"]
        }
        if expected_tables <= {table for table, _ in fields}:
            return fields
        time.sleep(3)
    raise RuntimeError("Metabase did not finish syncing the gold schema")


def reset_collection(mb: Metabase) -> int:
    for coll in mb.get("/collection"):
        if coll.get("name") == COLLECTION and not coll.get("archived"):
            for item in mb.get(f"/collection/{coll['id']}/items")["data"]:
                kind = {"card": "card", "dataset": "card", "dashboard": "dashboard"}.get(
                    item["model"]
                )
                if kind:
                    mb.put(f"/{kind}/{item['id']}", {"archived": True})
            return coll["id"]
    return mb.post("/collection", {"name": COLLECTION, "color": BLUE})["id"]


# --------------------------------------------------------------------------- cards

PARAMS = {
    "date": {"name": "Date", "slug": "date", "type": "date/all-options", "sectionId": "date"},
    "region": {"name": "Region", "slug": "region", "type": "string/=", "sectionId": "string"},
    "store": {"name": "Store", "slug": "store", "type": "string/=", "sectionId": "string"},
    "channel": {"name": "Channel", "slug": "channel", "type": "string/=", "sectionId": "string"},
}


class Builder:
    def __init__(self, mb: Metabase, db_id: int, fields: dict, collection_id: int):
        self.mb, self.db_id, self.fields, self.collection_id = mb, db_id, fields, collection_id

    def card(self, name: str, sql: str, display: str, filters: dict[str, tuple], viz=None) -> dict:
        """filters maps a filter key (date/region/store/channel) to its (table, column)."""
        tags = {
            key: {
                "id": f"{key}-{abs(hash((name, key))) % 10**8}",
                "name": key,
                "display-name": PARAMS[key]["name"],
                "type": "dimension",
                "dimension": ["field", self.fields[target], None],
                "widget-type": PARAMS[key]["type"],
            }
            for key, target in filters.items()
        }
        card = self.mb.post(
            "/card",
            {
                "name": name,
                "collection_id": self.collection_id,
                "display": display,
                "visualization_settings": viz or {},
                "dataset_query": {
                    "type": "native",
                    "database": self.db_id,
                    "native": {"query": sql.strip(), "template-tags": tags},
                },
            },
        )
        card["_filters"] = list(filters)
        return card

    def dashboard(self, name: str, description: str, params: list[str], layout: list) -> dict:
        dash = self.mb.post(
            "/dashboard",
            {"name": name, "description": description, "collection_id": self.collection_id},
        )
        parameters = [{"id": key, **PARAMS[key]} for key in params]
        dashcards = []
        for i, (card, row, col, sx, sy) in enumerate(layout):
            dashcards.append(
                {
                    "id": -(i + 1),
                    "card_id": card["id"],
                    "row": row,
                    "col": col,
                    "size_x": sx,
                    "size_y": sy,
                    "parameter_mappings": [
                        {
                            "parameter_id": key,
                            "card_id": card["id"],
                            "target": ["dimension", ["template-tag", key]],
                        }
                        for key in card["_filters"]
                        if key in params
                    ],
                }
            )
        self.mb.put(f"/dashboard/{dash['id']}", {"parameters": parameters, "dashcards": dashcards})
        uuid = self.mb.post(f"/dashboard/{dash['id']}/public_link")["uuid"]
        return {"id": dash["id"], "name": name, "public_url": f"{MB_URL}/public/dashboard/{uuid}"}


def money(col: str) -> dict:
    return {f'["name","{col}"]': {"prefix": "₱", "decimals": 0}}


def pct(col: str, decimals: int = 1) -> dict:
    return {f'["name","{col}"]': {"number_style": "percent", "decimals": decimals}}


def chart(x: str, ys: list[str], colors: list[str], x_title: str = "", y_title: str = "", **extra):
    return {
        "graph.dimensions": [x],
        "graph.metrics": ys,
        "graph.x_axis.title_text": x_title,
        "graph.y_axis.title_text": y_title,
        "series_settings": {y: {"color": c} for y, c in zip(ys, colors, strict=True)},
        **extra,
    }


def build(b: Builder) -> list[dict]:
    o = {  # fct_orders filters
        "date": ("fct_orders", "order_date_local"),
        "region": ("fct_orders", "region"),
        "store": ("fct_orders", "store_name"),
        "channel": ("fct_orders", "channel"),
    }
    s = {
        "date": ("fct_sessions", "session_date_local"),
        "region": ("fct_sessions", "region"),
        "store": ("fct_sessions", "store_name"),
        "channel": ("fct_sessions", "channel"),
    }
    ci = {
        "date": ("fct_cart_items", "added_date_local"),
        "region": ("fct_cart_items", "region"),
        "store": ("fct_cart_items", "store_name"),
        "channel": ("fct_cart_items", "channel"),
    }
    where_o = "where {{date}} and {{region}} and {{store}} and {{channel}}"

    # ---------------------------------------------------------------- operations
    ops = [
        b.card("Orders", f"select count(*) as orders from gold.fct_orders {where_o}", "scalar", o),
        b.card(
            "Net revenue",
            f"select sum(net_revenue) as net_revenue from gold.fct_orders {where_o}",
            "scalar",
            o,
            {"column_settings": money("net_revenue")},
        ),
        b.card(
            "Completion rate",
            f"select avg(case when order_status = 'completed' then 1.0 else 0 end) as completion_rate "
            f"from gold.fct_orders {where_o}",
            "scalar",
            o,
            {"column_settings": pct("completion_rate")},
        ),
        b.card(
            "Avg prep time (min)",
            f"select round(avg(prep_seconds) / 60.0, 1) as avg_prep_minutes from gold.fct_orders {where_o}",
            "scalar",
            o,
        ),
        b.card(
            "SLA breaches (>10 min)",
            "select avg(case when is_prep_sla_breached then 1.0 else 0 end) as sla_breach_rate "
            f"from gold.fct_orders {where_o} and prep_seconds is not null",
            "scalar",
            o,
            {"column_settings": pct("sla_breach_rate")},
        ),
        b.card(
            "Daily net revenue",
            f"""select order_date_local as day, sum(net_revenue) as net_revenue
                from gold.fct_orders {where_o} group by 1 order by 1""",
            "line",
            o,
            chart("day", ["net_revenue"], [BLUE], "Order date", "Net revenue (PHP)")
            | {"column_settings": money("net_revenue")},
        ),
        b.card(
            "Orders by local hour",
            f"""select order_hour_local as hour, count(*) as orders
                from gold.fct_orders {where_o} group by 1 order by 1""",
            "bar",
            o,
            chart("hour", ["orders"], [BLUE], "Hour of day (Manila)", "Orders"),
        ),
        b.card(
            "Prep SLA breach rate by store",
            f"""select replace(store_name, 'PourStream ', '') as store,
                       avg(case when is_prep_sla_breached then 1.0 else 0 end) as breach_rate
                from gold.fct_orders {where_o} and prep_seconds is not null
                group by 1 order by 2 desc""",
            "row",
            o,
            chart("store", ["breach_rate"], [BLUE], **NO_OTHER)
            | {"column_settings": pct("breach_rate")},
        ),
        b.card(
            "Cancellations by reason",
            f"""select initcap(replace(cancel_reason, '_', ' ')) as cancel_reason,
                       count(*) as orders from gold.fct_orders
                {where_o} and order_status = 'cancelled' group by 1 order by 2 desc""",
            "bar",
            o,
            chart("cancel_reason", ["orders"], [BLUE], "Reason", "Orders"),
        ),
        b.card(
            "Store scorecard",
            f"""select replace(store_name, 'PourStream ', '') as "Store", region as "Region", store_format as "Format",
                       count(*) as "Orders", sum(net_revenue) as "Net revenue",
                       round(avg(case when order_status = 'completed' then 1.0 else 0 end), 4)
                           as "Completion",
                       round(avg(prep_seconds) / 60.0, 1) as "Avg prep (min)",
                       round((percentile_cont(0.9) within group (order by prep_seconds) / 60.0)::numeric, 1)
                           as "P90 prep (min)",
                       round(avg(case when is_prep_sla_breached then 1.0 else 0 end)
                           filter (where prep_seconds is not null), 4) as "SLA breach"
                from gold.fct_orders {where_o}
                group by 1, 2, 3 order by "Net revenue" desc""",
            "table",
            o,
            {
                "column_settings": money("Net revenue") | pct("Completion") | pct("SLA breach"),
            },
        ),
    ]
    operations = b.dashboard(
        "Store Operations",
        "Orders, revenue and prep-time SLA by store and hour. Synthetic data.",
        ["date", "region", "store", "channel"],
        [
            (ops[0], 0, 0, 5, 3),
            (ops[1], 0, 5, 5, 3),
            (ops[2], 0, 10, 4, 3),
            (ops[3], 0, 14, 5, 3),
            (ops[4], 0, 19, 5, 3),
            (ops[5], 3, 0, 12, 6),
            (ops[6], 3, 12, 12, 6),
            (ops[7], 9, 0, 12, 9),
            (ops[8], 9, 12, 12, 9),
            (ops[9], 18, 0, 24, 11),
        ],
    )

    # ---------------------------------------------------------------- funnel & menu
    where_s = "where {{date}} and {{region}} and {{store}} and {{channel}}"
    fun = [
        b.card(
            "Sessions", f"select count(*) as sessions from gold.fct_sessions {where_s}", "scalar", s
        ),
        b.card(
            "Session → order conversion",
            "select avg(case when reached_order then 1.0 else 0 end) as conversion "
            f"from gold.fct_sessions {where_s}",
            "scalar",
            s,
            {"column_settings": pct("conversion")},
        ),
        b.card(
            "Cart → order conversion",
            "select avg(case when reached_order then 1.0 else 0 end) as conversion "
            f"from gold.fct_sessions {where_s} and reached_cart",
            "scalar",
            s,
            {"column_settings": pct("conversion")},
        ),
        b.card(
            "Average order value",
            f"select avg(order_amount) as aov from gold.fct_orders {where_o} and order_amount is not null",
            "scalar",
            o,
            {"column_settings": money("aov")},
        ),
        b.card(
            "Ordering funnel",
            f"""with s as (select * from gold.fct_sessions {where_s})
                select stage, sessions from (
                    select 1 as k, 'Session started' as stage, count(*) as sessions from s
                    union all select 2, 'Viewed menu', count(*) filter (where reached_menu) from s
                    union all select 3, 'Added to cart', count(*) filter (where reached_cart) from s
                    union all select 4, 'Checkout', count(*) filter (where reached_checkout) from s
                    union all select 5, 'Placed order', count(*) filter (where reached_order) from s
                    union all select 6, 'Paid', count(*) filter (where reached_payment) from s
                    union all select 7, 'Picked up', count(*) filter (where reached_pickup) from s
                ) stages order by k""",
            "funnel",
            s,
            {"funnel.dimension": "stage", "funnel.metric": "sessions"},
        ),
        b.card(
            "Conversion by channel",
            f"""select channel, avg(case when reached_order then 1.0 else 0 end) as conversion
                from gold.fct_sessions {where_s} group by 1 order by 2 desc""",
            "bar",
            s,
            chart("channel", ["conversion"], [BLUE], "Channel", "Session → order")
            | {"column_settings": pct("conversion")},
        ),
        b.card(
            "Daily session → order conversion",
            f"""select session_date_local as day,
                       avg(case when reached_order then 1.0 else 0 end) as conversion
                from gold.fct_sessions {where_s} group by 1 order by 1""",
            "line",
            s,
            chart("day", ["conversion"], [BLUE], "Session date", "Conversion")
            | {"column_settings": pct("conversion")},
        ),
        b.card(
            "Top items added to cart",
            """select item_name as item, sum(quantity) as units from gold.fct_cart_items
               where {{date}} and {{region}} and {{store}} and {{channel}}
               group by 1 order by 2 desc limit 10""",
            "row",
            ci,
            chart("item", ["units"], [BLUE], **NO_OTHER),
        ),
        b.card(
            "Units by category",
            """select initcap(replace(category, '_', ' ')) as category, sum(quantity) as units
               from gold.fct_cart_items
               where {{date}} and {{region}} and {{store}} and {{channel}}
               group by 1 order by 2 desc""",
            "bar",
            ci,
            chart("category", ["units"], [BLUE], "Category", "Units"),
        ),
        b.card(
            "Item cart-to-order conversion",
            """select item_name as "Item", initcap(replace(category, '_', ' ')) as "Category", sum(quantity) as "Units",
                      sum(line_amount) as "Cart value",
                      round(avg(case when converted_to_order then 1.0 else 0 end), 4) as "Converted"
               from gold.fct_cart_items
               where {{date}} and {{region}} and {{store}} and {{channel}}
               group by 1, 2 order by "Units" desc""",
            "table",
            ci,
            {"column_settings": money("Cart value") | pct("Converted")},
        ),
    ]
    funnel = b.dashboard(
        "Customer Funnel & Menu",
        "Session conversion funnel, channel conversion and item demand. Synthetic data.",
        ["date", "region", "store", "channel"],
        [
            (fun[0], 0, 0, 6, 3),
            (fun[1], 0, 6, 6, 3),
            (fun[2], 0, 12, 6, 3),
            (fun[3], 0, 18, 6, 3),
            (fun[4], 3, 0, 24, 7),
            (fun[5], 10, 0, 12, 6),
            (fun[6], 10, 12, 12, 6),
            (fun[7], 16, 0, 12, 9),
            (fun[8], 16, 12, 12, 9),
            (fun[9], 25, 0, 24, 9),
        ],
    )

    # ---------------------------------------------------------------- stream health
    h = {"date": ("agg_pipeline_health", "arrival_date_local")}
    d = {"date": ("agg_dead_letters_daily", "arrival_date_local")}
    hp = "from gold.agg_pipeline_health where {{date}}"
    health = [
        b.card("Messages received", f"select sum(total_messages) as messages {hp}", "scalar", h),
        b.card(
            "Duplicate rate",
            f"select sum(duplicate_deliveries)::numeric / nullif(sum(total_messages), 0) as rate {hp}",
            "scalar",
            h,
            {"column_settings": pct("rate", 2)},
        ),
        b.card(
            "Late event rate (>15 min)",
            f"select sum(late_events)::numeric / nullif(sum(unique_events), 0) as rate {hp}",
            "scalar",
            h,
            {"column_settings": pct("rate", 2)},
        ),
        b.card(
            "Dead-letter rate",
            f"select sum(dead_lettered_messages)::numeric / nullif(sum(total_messages), 0) as rate {hp}",
            "scalar",
            h,
            {"column_settings": pct("rate", 2)},
        ),
        b.card(
            "Messages by arrival date",
            f"""select arrival_date_local as day,
                       unique_events as "Unique events",
                       duplicate_deliveries as "Duplicates",
                       dead_lettered_messages as "Dead-lettered"
                {hp} order by 1""",
            "bar",
            h,
            chart(
                "day",
                ["Unique events", "Duplicates", "Dead-lettered"],
                [BLUE, ORANGE, AQUA],
                "Arrival date",
                "Messages",
                **{"stackable.stack_type": "stacked"},
            ),
        ),
        b.card(
            "Late events by arrival date",
            f"select arrival_date_local as day, late_events {hp} order by 1",
            "bar",
            h,
            chart("day", ["late_events"], [BLUE], "Arrival date", "Events > 15 min late"),
        ),
        b.card(
            "Dead letters by error type",
            """select arrival_date_local as day,
                      sum(dead_lettered_messages) filter (where error_type = 'schema_violation')
                          as "Schema violation",
                      sum(dead_lettered_messages) filter (where error_type = 'future_timestamp')
                          as "Future timestamp",
                      sum(dead_lettered_messages) filter (where error_type = 'invalid_json')
                          as "Invalid JSON"
               from gold.agg_dead_letters_daily where {{date}} group by 1 order by 1""",
            "bar",
            d,
            chart(
                "day",
                ["Schema violation", "Future timestamp", "Invalid JSON"],
                [BLUE, ORANGE, AQUA],
                "Arrival date",
                "Messages",
                **{"stackable.stack_type": "stacked"},
            ),
        ),
        b.card(
            "Daily pipeline health",
            f"""select arrival_date_local as "Arrival date", total_messages as "Messages",
                       unique_events as "Unique", duplicate_deliveries as "Duplicates",
                       late_events as "Late", dead_lettered_messages as "Dead-lettered",
                       p50_arrival_lag_seconds as "p50 lag (s)",
                       p95_arrival_lag_seconds as "p95 lag (s)"
                {hp} order by 1""",
            "table",
            h,
        ),
    ]
    stream = b.dashboard(
        "Stream Health",
        "Data quality of the order-event stream: duplicates, late arrivals, dead letters, lag.",
        ["date"],
        [
            (health[0], 0, 0, 6, 3),
            (health[1], 0, 6, 6, 3),
            (health[2], 0, 12, 6, 3),
            (health[3], 0, 18, 6, 3),
            (health[4], 3, 0, 12, 7),
            (health[5], 3, 12, 12, 7),
            (health[6], 10, 0, 24, 7),
            (health[7], 17, 0, 24, 9),
        ],
    )
    return [operations, funnel, stream]


def main() -> None:
    env = _env()
    mb = connect(env)
    mb.put("/setting/enable-public-sharing", {"value": True})
    db_id = ensure_database(mb, env)
    fields = field_ids(
        mb,
        db_id,
        {"fct_orders", "fct_sessions", "fct_cart_items", "agg_pipeline_health",
         "agg_dead_letters_daily"},
    )  # fmt: skip
    builder = Builder(mb, db_id, fields, reset_collection(mb))
    dashboards = build(builder)
    (HERE / "dashboards.json").write_text(json.dumps(dashboards, indent=2) + "\n")
    for dash in dashboards:
        print(f"{dash['name']:<24} {dash['public_url']}")


if __name__ == "__main__":
    main()
