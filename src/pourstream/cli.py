"""Command line entry point: ``pourstream <command>``."""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

from pourstream import pipeline
from pourstream.config import PROJECT_ROOT, Settings
from pourstream.generator import SimConfig
from pourstream.report import render


def _sim_config(args: argparse.Namespace) -> SimConfig:
    return SimConfig(
        start_date=date.fromisoformat(args.start_date),
        days=args.days,
        sessions_per_store_day=args.sessions,
        seed=args.seed,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="pourstream", description=__doc__)
    parser.add_argument("--transport", choices=["kafka", "file"], default="file")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_sim_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--start-date", default="2026-10-01")
        p.add_argument("--days", type=int, default=7)
        p.add_argument("--sessions", type=int, default=90, help="sessions per store-day (base)")
        p.add_argument("--seed", type=int, default=42)

    add_sim_args(sub.add_parser("produce", help="simulate sessions and publish events"))
    load = sub.add_parser("load", help="consume events into the Parquet bronze lake")
    load.add_argument("--batch-size", type=int, default=5000)
    transform = sub.add_parser("transform", help="run dbt (default: build)")
    transform.add_argument("dbt_args", nargs="*", default=["build"])
    report = sub.add_parser("report", help="render the HTML KPI report")
    report.add_argument("--out", default=str(PROJECT_ROOT / "reports" / "kpi_report.html"))
    run = sub.add_parser("run", help="produce + load + transform + report")
    add_sim_args(run)
    run.add_argument("--out", default=str(PROJECT_ROOT / "reports" / "kpi_report.html"))

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = Settings.from_env()

    if args.command in {"produce", "run"}:
        pipeline.produce(settings, args.transport, _sim_config(args))
    if args.command in {"load", "run"}:
        pipeline.load(settings, args.transport, getattr(args, "batch_size", 5000))
    if args.command in {"transform", "run"}:
        pipeline.transform(settings, getattr(args, "dbt_args", ["build"]))
    if args.command in {"report", "run"}:
        out = render(settings.warehouse_path, Path(args.out))
        logging.getLogger("pourstream").info("report written to %s", out)


if __name__ == "__main__":
    main()
