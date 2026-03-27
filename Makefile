PYTHON ?= python3

.PHONY: install dev run test doctor migrate-legacy

install:
	$(PYTHON) -m pip install -e .

dev:
	$(PYTHON) -m pip install -e '.[dev]'

run:
	$(PYTHON) -m kkbot

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v

doctor:
	cd go && go run ./cmd/kkbotctl doctor

migrate-legacy:
	PYTHONPATH=src $(PYTHON) -m kkbot.tools.migrate_legacy
