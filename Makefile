.PHONY: setup broker-up broker-down run run-kafka produce load transform report test test-fast lint clean

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

test:             ## full suite (Kafka tests run when a broker is reachable)
	uv run pytest -v

test-fast:        ## unit tests only
	uv run pytest -v -m "not e2e and not kafka"

lint:
	uv run ruff check . && uv run ruff format --check .

clean:
	rm -rf data reports/*.html dbt/target dbt/logs
