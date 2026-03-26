PYTHON ?= python3

.PHONY: install dev legacy run run-legacy test test-legacy doctor migrate-legacy

install:
	$(PYTHON) -m pip install -e .

dev:
	$(PYTHON) -m pip install -e '.[dev]'

legacy:
	$(PYTHON) -m pip install -e '.[legacy]'

run:
	$(PYTHON) -m kkbot

run-legacy:
	PYTHONPATH=src $(PYTHON) -m kkbot.tools.run_legacy_v1

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v

test-legacy:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests_legacy -v

doctor:
	cd go && go run ./cmd/kkbotctl doctor

migrate-legacy:
	PYTHONPATH=src $(PYTHON) -m kkbot.tools.migrate_legacy
