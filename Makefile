.PHONY: setup broker-up broker-down run run-kafka produce load transform report bi-up bi-down publish dashboards screenshots test test-fast lint clean

TRANSPORT ?= file

setup:            ## install Python deps into .venv
	uv sync

broker-up:        ## start Redpanda + console (http://localhost:8080)
	docker compose up -d --wait

broker-down:
	docker compose down -v

run:              ## end to end on the file broker (no Docker needed)
	uv run pourstream --transport $(TRANSPORT) run

run-kafka: broker-up
	uv run pourstream --transport kafka run

produce:
	uv run pourstream --transport $(TRANSPORT) produce
load:
	uv run pourstream --transport $(TRANSPORT) load
transform:
	uv run pourstream transform build
report:
	uv run pourstream report

bi-up:            ## start Postgres serving DB + Metabase (http://localhost:3001)
	cd bi/metabase && ([ -f .env ] || cp .env.example .env) && docker compose up -d --wait

bi-down:
	cd bi/metabase && docker compose down -v

publish:          ## copy gold marts from DuckDB to the Postgres serving DB
	uv run pourstream publish

dashboards:       ## (re)build the Metabase dashboards through the API
	uv run python bi/metabase/build_dashboards.py

screenshots:      ## capture the public dashboards into docs/images
	cd bi/metabase && npm install --silent
	node bi/metabase/screenshot.mjs bi/metabase/dashboards.json docs/images

test:             ## full suite (Kafka tests run when a broker is reachable)
	uv run pytest -v

test-fast:        ## unit tests only
	uv run pytest -v -m "not e2e and not kafka"

lint:
	uv run ruff check . && uv run ruff format --check .

clean:
	rm -rf data reports/*.html dbt/target dbt/logs
