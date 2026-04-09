# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Databricks SQL CLI — an interactive terminal client for Databricks SQL with auto-completion and syntax highlighting. Fork of the original [databricks/databricks-sql-cli](https://github.com/databricks/databricks-sql-cli), updated for Python 3.11–3.14.

## Common Commands

```bash
# Install dependencies
poetry install

# Run the CLI
poetry run dbsqlcli

# Run all tests
poetry run pytest test/

# Run a single test file
poetry run pytest test/test_format_utils.py

# Run a specific test
poetry run pytest test/test_format_utils.py::test_json_safe_preprocessor_converts_datetime

# Check formatting
poetry run black --check dbsqlcli

# Auto-format
poetry run black dbsqlcli

# Regenerate lock file after dependency changes
poetry lock
```

## Architecture

The CLI is a Click application (`dbsqlcli.main:cli`) that runs an interactive REPL using `prompt-toolkit`.

### Core modules

- **`main.py`** — `DBSQLCli` class owns the REPL loop, output formatting, and prompt session. Entry point is the `cli()` Click command at the bottom. The `-e` flag runs a single query via `run_query()` instead of the REPL.
- **`sqlexecute.py`** — `SQLExecute` wraps `databricks-sql-connector`. All database calls (queries, metadata introspection) go through here. The `run()` method yields `(title, rows, headers, status)` tuples.
- **`completer.py`** — `DBSQLCompleter` extends prompt-toolkit's `Completer`. Populated by the completion refresher.
- **`completion_refresher.py`** — Runs metadata queries in a background daemon thread. Uses a `@refresher(name)` decorator to register refresh functions (databases, schemata, tables, special_commands). Thread-safe via `_completer_lock` in `DBSQLCli`.
- **`config.py`** — Reads `configobj`-format config files. Default config at `dbsqlcli/dbsqlclirc` is merged with user's `~/.dbsqlcli/dbsqlclirc`.

### packages/ directory

- **`special/`** — Backslash commands (`\T`, `\R`, `\P`, etc.) registered via `@special_command` decorator into a global `COMMANDS` dict. `iocommands.py` handles editor integration, file I/O, pager. `dbcommands.py` handles database introspection commands.
- **`completion_engine.py`** — `suggest_type()` parses partial SQL to determine what kind of completion to offer (Table, Column, Keyword, etc.).
- **`parseutils.py`** — SQL parsing utilities (`extract_tables`, `last_word`, `query_starts_with`).
- **`tabular_output/sql_format.py`** — Registers custom SQL output formatters (sql-insert, sql-update) with `cli-helpers`' `TabularOutputFormatter`.

### Data flow for a query

1. User input → `sqlparse.split()` splits into statements
2. Each statement → `SQLExecute.run()` → yields `(title, rows, headers, status)`
3. Results → `DBSQLCli.format_output()` → `TabularOutputFormatter.format_output()` with preprocessors
4. For jsonl formats, `_json_safe_preprocessor` converts datetime/date/time to ISO strings before JSON serialization

### Key patterns

- **Preprocessors** must return `(data, headers)` tuple — this is the `cli-helpers` contract.
- **Special commands** use `@special_command` decorator and return `(title, rows, headers, status)` tuples matching `SQLExecute.run()`.
- **Config hierarchy**: default config → user config → CLI args → environment variables.
