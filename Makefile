VENV ?= $(HOME)/.venv-spot
PY := $(VENV)/bin/python

.PHONY: venv test lint up down logs
venv:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -r requirements-dev.txt

test:
	$(PY) -m pytest

lint:
	$(VENV)/bin/ruff check .

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

dev:  ## whole platform in one process (no Docker): http://127.0.0.1:8000
	$(PY) tools/devstack.py

# --- database migrations (one history per service) ---
migration:  ## make migration svc=market m="add column"  -> autogenerates the next revision
	$(PY) tools/new_migration.py $(svc) "$(m)"
check-migrations:
	@for s in wallet bid capacity market settlement gateway; do $(PY) tools/check_migrations.py $$s || exit 1; done

soak:
	$(PY) -m pytest tests -q

bench:
	$(PY) tools/bench.py clearing && $(PY) tools/bench.py tick && $(PY) tools/bench.py submit && $(PY) tools/bench.py soak 12 100
