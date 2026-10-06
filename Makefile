PYTHON ?= python

.PHONY: install migrate check test run build
install:
	$(PYTHON) -m pip install -r requirements-dev.txt
migrate:
	$(PYTHON) -m alembic upgrade head
check:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m mypy
test:
	$(PYTHON) -m pytest
run:
	$(PYTHON) -m uvicorn main:app --host 127.0.0.1 --port 55585 --no-access-log
build:
	docker build -t conversation-service:local .
