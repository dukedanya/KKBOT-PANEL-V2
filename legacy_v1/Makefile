PYTHON ?= python

install:
	$(PYTHON) -m pip install -r requirements.txt

run:
	$(PYTHON) main.py

test:
	$(PYTHON) -m unittest discover -s tests -v

backup-db:
	$(PYTHON) scripts/db_backup.py --gzip

verify-backup:
	@if [ -z "$(BACKUP_FILE)" ]; then echo "Usage: make verify-backup BACKUP_FILE=/path/to/backup.sqlite3.gz"; exit 1; fi
	$(PYTHON) scripts/db_restore_check.py "$(BACKUP_FILE)"
