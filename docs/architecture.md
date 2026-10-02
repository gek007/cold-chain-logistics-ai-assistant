# Architecture

## Overview

Cold-chain logistics AI assistant. Current state: data layer only.

```
data/dynamic_supply_chain_logistics_dataset.csv   (legacy data, hourly, 2021-01 → 2024-08)
        │
        ▼  scripts/ingest_legacy_data.py
SQL Server 2022 (docker-compose, container `cold-chain-sqlserver`)
        └─ database `cold_chain`
             └─ dbo.legacy_logistics_events
```

## Components

| Component | Path | Notes |
|---|---|---|
| SQL Server | `docker-compose.yml`, `run.sh` | Started with `./run.sh`. Config in `.env` (see `.env.example`). `./data` is mounted read-only at `/data`. |
| Legacy ingestion | `scripts/ingest_legacy_data.py` | Creates the DB/table if missing and loads the CSV. |
| Python environment | `pyproject.toml`, `uv.lock`, `.python-version`, `requirements.txt` | Python 3.12, managed with `uv`. `pyproject.toml` is the source of truth (`uv sync`); `requirements.txt` mirrors it for pip users. |
| Tests | `tests/` | `pytest`; unit tests run offline, integration tests use a throwaway DB and skip when SQL Server is down. |

## Python environment

```bash
uv sync                 # creates .venv (Python 3.12) from pyproject.toml / uv.lock
uv run pytest           # run tests in that environment
```

Pinned dependencies, grouped by purpose (planned use in later steps unless noted):

| Purpose | Packages |
|---|---|
| LLM / agent orchestration | `langgraph`, `langgraph-checkpoint`, `langchain-community`, `langchain-openai`, `langchain-huggingface`, `langsmith`, `openai` |
| Vector search | `faiss-cpu`, `pinecone`, `langchain-pinecone` |
| Data and documents | `pandas`, `openpyxl`, `pypdf`, `altair` |
| Config | `pydantic-settings`, `python-dotenv` (used today) |
| SQL Server access | `pymssql` (used by the ingestion script), `pyodbc` (needs `brew install unixodbc` on macOS, plus Microsoft's ODBC driver 18 to connect) |
| Dev | `pytest` (dependency group `dev`) |

`requires-python` is `>=3.12` because `pandas==3.0.5` needs Python 3.11+.

## Data model: `dbo.legacy_logistics_events`

- `timestamp` `DATETIME2(0)` — primary key (one row per hour, unique in the source).
- 23 `FLOAT` measurement columns (GPS, fuel, traffic, inventory, IoT temperature, scores, …), named exactly like the CSV headers.
- `risk_classification` `VARCHAR(20)` — constrained to `Low Risk` / `Moderate Risk` / `High Risk`.
- `delivery_time_deviation` `FLOAT`.
- `ingested_at` `DATETIME2(0)` — UTC load time (default).

`FLOAT` (double) is used so values round-trip exactly with the CSV.

## Ingestion script

```bash
./run.sh                                              # start SQL Server
uv run scripts/ingest_legacy_data.py                  # load (idempotent)
uv run scripts/ingest_legacy_data.py --replace        # wipe table and reload
uv run scripts/ingest_legacy_data.py --dry-run        # validate CSV only
```

The script declares its dependencies inline (PEP 723), so `uv` installs them on
demand; no manual venv is needed. `./scripts/ingest_legacy_data.py` also works.
Without uv: create a Python 3.12 venv and `pip install -r requirements.txt`.

- Connection settings come from `.env`: `SQL_SERVER_HOST`, `SQL_SERVER_PORT`, `SQL_SERVER_USER`, `SQL_SERVER_DATABASE`, and the password from `MSSQL_SA_PASSWORD` (or `SQL_SERVER_PASSWORD`).
- The whole CSV is validated (types, finite numbers, known risk class, unique timestamps) before anything is written.
- Re-runs skip rows whose `timestamp` already exists, so the script is safe to repeat.
- Driver: `pymssql` (no system ODBC driver required). Loading ~32k rows takes ~1 min on Apple Silicon because SQL Server runs under amd64 emulation.

## Testing

```bash
uv run pytest
```
