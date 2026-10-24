"""Runtime configuration, read from environment variables with local defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SEEDS_DIR = PROJECT_ROOT / "dbt" / "seeds"
DBT_DIR = PROJECT_ROOT / "dbt"

TOPIC = "app.order_events"
CONSUMER_GROUP = "pourstream-bronze-loader"


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    kafka_bootstrap: str
    topic: str = TOPIC
    consumer_group: str = CONSUMER_GROUP

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake"

    @property
    def file_broker_dir(self) -> Path:
        return self.data_dir / "file_broker"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            data_dir=Path(os.environ.get("POURSTREAM_DATA_DIR", PROJECT_ROOT / "data")).resolve(),
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        )
