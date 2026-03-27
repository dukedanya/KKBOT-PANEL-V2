# KKBOT PANEL V2.0 Architecture

## Runtime split

### Python

- Telegram bot runtime
- business workflows
- startup orchestration
- SQLite -> PostgreSQL import
- panel HTTP integrations

### Go

- operational CLI
- infra diagnostics
- fast connectivity checks

## Database strategy

### Phase 1

- old SQLite is preserved as import source
- first boot imports it into PostgreSQL
- all runtime work goes only through PostgreSQL

### Phase 2

- SQLite is no longer used by runtime
- SQLite file remains only as backup / rollback artifact

## Why this split

- no risky big-bang rewrite of data at runtime
- bot gets a clean PostgreSQL-only runtime
- migration stays reversible because original SQLite is untouched
