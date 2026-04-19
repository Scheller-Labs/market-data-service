.PHONY: help up down install test test-unit test-integration lint typecheck clean health get-sample

# ── Help ──────────────────────────────────────────────────
help:
	@echo ""
	@echo "Market Data Service"
	@echo "-------------------"
	@echo "  make up              Start Docker infrastructure (TimescaleDB, Redis, MinIO)"
	@echo "  make down            Stop Docker infrastructure"
	@echo "  make install         Install Python dependencies via Poetry"
	@echo "  make test            Run all unit tests"
	@echo "  make test-unit       Run unit tests only (no API calls)"
	@echo "  make test-integration Run integration tests (requires API keys in .env)"
	@echo "  make lint            Lint with ruff"
	@echo "  make typecheck       Type check with mypy"
	@echo "  make health          Check all infrastructure components"
	@echo "  make get-sample      Fetch AAPL OHLCV (365 days) as a smoke test"
	@echo "  make clean           Remove __pycache__ and .pyc files"
	@echo ""

# ── Infrastructure ────────────────────────────────────────
up:
	docker compose up -d timescaledb redis
	@echo "Waiting for services to be healthy..."
	@sleep 8
	docker compose ps

down:
	docker compose down timescaledb redis

down-volumes:
	docker compose down -v
	@echo "WARNING: All stored market data has been deleted."

logs:
	docker compose logs -f timescaledb redis

# ── Python Setup ──────────────────────────────────────────
install:
	pip install poetry --quiet
	poetry install

# ── Tests ─────────────────────────────────────────────────
test:
	poetry run pytest tests/unit/ -v

test-unit:
	poetry run pytest tests/unit/ -v -m "not integration"

test-integration:
	@echo "Running integration tests — requires docker compose up and API keys in .env"
	poetry run pytest tests/integration/ -v -m integration --tb=short

test-coverage:
	poetry run pytest tests/unit/ --cov=market_data --cov-report=html
	@echo "Coverage report: htmlcov/index.html"

# ── Code Quality ──────────────────────────────────────────
lint:
	poetry run ruff check market_data/ tests/

lint-fix:
	poetry run ruff check market_data/ tests/ --fix

typecheck:
	poetry run mypy market_data/

format:
	poetry run black market_data/ tests/

# ── CLI Smoke Tests ───────────────────────────────────────
health:
	poetry run market-data health

get-sample:
	@echo "Fetching AAPL OHLCV (365 days)..."
	poetry run market-data get --symbol AAPL --type ohlcv --days 365 --format json | python3 -c \
		"import json,sys; d=json.load(sys.stdin); print(f\"  rows={d['rows']} source={d['source']} coverage={d['coverage']}\")"

status-sample:
	poetry run market-data status --symbol AAPL --type ohlcv --days 365

# ── Utilities ─────────────────────────────────────────────
init-coverage-dir:
	mkdir -p /data/manifest

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	find . -name ".coverage" -delete
	rm -rf htmlcov/ .pytest_cache/ .mypy_cache/
