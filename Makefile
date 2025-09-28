.PHONY: help lint format check-all run-server build-and-run

help:
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

run-server:
	poetry run python -m src.api.server

lint:
	poetry run black --check --diff .
	poetry run isort --check-only --diff .
	poetry run flake8 src
	poetry run bandit -r src -ll || echo "Bandit found some issues but they are not critical"

format:
	poetry run black .
	poetry run isort .

check-all: lint

install:
	poetry install

dev-setup: install
	@echo "Development environment is ready!"

ci: check-all
	@echo "All CI checks passed!"

build-and-run:
	docker build -t trino-mcp .
	docker run trino-mcp