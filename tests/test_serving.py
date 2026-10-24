"""Publish gold marts to the Postgres serving DB. Skipped unless the BI stack is running."""

from __future__ import annotations

import os
import socket
from datetime import date

import duckdb
import pytest

from pourstream import pipeline
from pourstream.generator import SimConfig
from pourstream.serving import DEFAULT_DSN, publish

DSN = os.environ.get("SERVING_DSN", DEFAULT_DSN)


def _reachable() -> bool:
    parts = dict(p.split("=", 1) for p in DSN.split())
    try:
        with socket.create_connection((parts["host"], int(parts["port"])), timeout=1):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(not _reachable(), reason=f"serving DB not reachable ({DSN})"),
]


def test_publish_copies_every_gold_table_exactly(settings):
    # Publish into a throwaway schema so the dashboards' gold schema is left untouched.
    pipeline.produce(settings, "file", SimConfig(start_date=date(2026, 10, 1), days=1, seed=2))
    pipeline.load(settings, "file")
    pipeline.transform(settings)

    tables = publish(settings.warehouse_path, DSN, target_schema="gold_test")
    assert "fct_orders" in tables and "agg_pipeline_health" in tables

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{settings.warehouse_path}' AS wh (READ_ONLY)")
    con.execute(f"ATTACH '{DSN}' AS pg (TYPE postgres)")
    try:
        for table in tables:
            wh = con.execute(f"select count(*) from wh.gold.{table}").fetchone()[0]
            pg = con.execute(f"select count(*) from pg.gold_test.{table}").fetchone()[0]
            assert wh == pg, table
    finally:
        con.execute("CALL postgres_execute('pg', 'drop schema if exists gold_test cascade')")
