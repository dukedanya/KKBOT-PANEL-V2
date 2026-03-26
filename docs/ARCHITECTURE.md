# KKBOT PANEL V2.0 Architecture

## Runtime split

### Python

- Telegram bot runtime
- business workflows
- startup orchestration
- legacy SQLite import
- panel HTTP integrations

### Go

- operational CLI
- infra diagnostics
- fast connectivity checks

## Database strategy

### Phase 1

- old SQLite is preserved as legacy source
- first boot imports it into PostgreSQL
- all new runtime work goes only through PostgreSQL

### Phase 2

- legacy SQLite stops being used operationally
- legacy file remains only as backup / rollback artifact

### Phase 3

- legacy import path can be removed once production is stable

## Why this split

- no risky big-bang rewrite of data at runtime
- bot gets a clean PostgreSQL-first core immediately
- migration stays reversible because original SQLite is untouched
