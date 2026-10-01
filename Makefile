.PHONY: help install install-dev test test-unit test-integration lint format type-check clean build docs serve-docs

help:
	@echo "Available commands:"
	@echo "  make install         Install package in production mode"
	@echo "  make install-dev     Install package in development mode"
	@echo "  make test            Run all tests"
	@echo "  make test-unit       Run unit tests only"
	@echo "  make test-integration Run integration tests (requires NATS)"
	@echo "  make lint            Run linting checks"
	@echo "  make format          Format code with black and isort"
	@echo "  make type-check      Run type checking with mypy"
	@echo "  make clean           Clean build artifacts"
	@echo "  make build           Build distribution packages"
	@echo "  make docs            Build documentation"
	@echo "  make serve-docs      Serve documentation locally"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,test,docs]"
	pre-commit install

test:
	pytest tests/ -v --cov=tc_nats_events --cov-report=term-missing --cov-report=html

test-audit:
	SKIP_INTEGRATION_TESTS=false pytest tests/audit -v --no-cov

test-cluster:
	docker compose -f tests/cluster/docker-compose.yml up -d
	NATS_CLUSTER_URL=nats://localhost:14222,nats://localhost:14223,nats://localhost:14224 \
		pytest tests/cluster -v --no-cov -m cluster
	docker compose -f tests/cluster/docker-compose.yml down -v

test-unit:
	pytest tests/unit -v -m "not integration"

test-integration:
	@echo "Starting NATS server for integration tests..."
	@docker run -d --name nats-test -p 4222:4222 nats:2.10-alpine -js || true
	@sleep 2
	SKIP_INTEGRATION_TESTS=false pytest tests/integration -v -m integration
	@docker stop nats-test && docker rm nats-test || true

lint:
	flake8 src/ tests/
	black --check src/ tests/
	isort --check-only src/ tests/

format:
	black src/ tests/
	isort src/ tests/

type-check:
	mypy src/tc_nats_events

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

docs:
	mkdocs build

serve-docs:
	mkdocs serve

# Development shortcuts
.PHONY: dev-nats dev-example

dev-nats:
	@echo "Starting NATS server for development..."
	docker run --rm -it -p 4222:4222 -p 8222:8222 nats:2.10-alpine -js -m 8222

dev-example:
	@echo "Running basic example..."
	cd examples && python basic_pubsub.py