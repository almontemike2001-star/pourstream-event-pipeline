"""Publish gold marts from DuckDB to the Postgres serving database that Metabase reads.

DuckDB allows a single writer, so BI tools never read the warehouse file directly: dbt keeps
building in DuckDB while dashboards query a Postgres copy that is refreshed per run.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

DEFAULT_DSN = "host=localhost port=5434 dbname=analytics user=pourstream password=pourstream"
SCHEMA = "gold"


def publish(warehouse: Path, dsn: str | None = None, target_schema: str = SCHEMA) -> list[str]:
    dsn = dsn or os.environ.get("SERVING_DSN", DEFAULT_DSN)
    con = duckdb.connect()  # in-memory hub: warehouse attached read-only, Postgres writable
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{warehouse}' AS warehouse (READ_ONLY)")
        con.execute(f"ATTACH '{dsn}' AS serving (TYPE postgres)")
        tables = [
            row[0]
            for row in con.execute(
                "select table_name from information_schema.tables "
                "where table_catalog = 'warehouse' and table_schema = ? order by 1",
                [SCHEMA],
            ).fetchall()
        ]
        con.execute(f"CREATE SCHEMA IF NOT EXISTS serving.{target_schema}")
        for table in tables:
            con.execute(f"DROP TABLE IF EXISTS serving.{target_schema}.{table}")
            con.execute(
                f"CREATE TABLE serving.{target_schema}.{table} AS "
                f"SELECT * FROM warehouse.{SCHEMA}.{table}"
            )
            log.info("published %s.%s", target_schema, table)
    finally:
        con.close()
    return tables
