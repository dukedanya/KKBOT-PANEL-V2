PYTHON ?= $(shell if [ -x ./venv/bin/python ]; then echo ./venv/bin/python; else echo python3; fi)

.PHONY: install dev run test doctor self-check migrate-legacy

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

self-check:
	PYTHONPATH=src $(PYTHON) -m kkbot.tools.self_check

migrate-legacy:
	PYTHONPATH=src $(PYTHON) -m kkbot.tools.migrate_legacy
