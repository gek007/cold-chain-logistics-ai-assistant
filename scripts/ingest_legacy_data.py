#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pymssql>=2.3,<3",
#     "python-dotenv>=1.0,<2",
# ]
# ///
"""Ingest the legacy supply-chain logistics CSV into SQL Server.

Reads ``data/dynamic_supply_chain_logistics_dataset.csv`` and loads it into
``dbo.legacy_logistics_events`` in the database configured via ``.env``
(``SQL_SERVER_HOST``, ``SQL_SERVER_PORT``, ``SQL_SERVER_USER``,
``SQL_SERVER_DATABASE`` and ``MSSQL_SA_PASSWORD``).

The script is idempotent: the database and table are created when missing and
rows whose ``timestamp`` is already stored are skipped, so it is safe to re-run.
Use ``--replace`` to wipe the table first and reload everything.

Usage:
    uv run scripts/ingest_legacy_data.py
    uv run scripts/ingest_legacy_data.py --replace
    uv run scripts/ingest_legacy_data.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = PROJECT_ROOT / "data" / "dynamic_supply_chain_logistics_dataset.csv"
DEFAULT_ENV = PROJECT_ROOT / ".env"

TABLE = "dbo.legacy_logistics_events"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
RISK_CLASSES = ("Low Risk", "Moderate Risk", "High Risk")
DEFAULT_BATCH_SIZE = 1000

log = logging.getLogger("ingest_legacy_data")


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #

# Column order matches the CSV header. Everything except `timestamp` and
# `risk_classification` is a floating point measurement.
TIMESTAMP_COLUMN = "timestamp"
CATEGORY_COLUMN = "risk_classification"
FLOAT_COLUMNS: tuple[str, ...] = (
    "vehicle_gps_latitude",
    "vehicle_gps_longitude",
    "fuel_consumption_rate",
    "eta_variation_hours",
    "traffic_congestion_level",
    "warehouse_inventory_level",
    "loading_unloading_time",
    "handling_equipment_availability",
    "order_fulfillment_status",
    "weather_condition_severity",
    "port_congestion_level",
    "shipping_costs",
    "supplier_reliability_score",
    "lead_time_days",
    "historical_demand",
    "iot_temperature",
    "cargo_condition_status",
    "route_risk_level",
    "customs_clearance_time",
    "driver_behavior_score",
    "fatigue_monitoring_score",
    "disruption_likelihood_score",
    "delay_probability",
)
TRAILING_FLOAT_COLUMNS: tuple[str, ...] = ("delivery_time_deviation",)

CSV_COLUMNS: tuple[str, ...] = (
    TIMESTAMP_COLUMN,
    *FLOAT_COLUMNS,
    CATEGORY_COLUMN,
    *TRAILING_FLOAT_COLUMNS,
)
ALL_FLOAT_COLUMNS: tuple[str, ...] = FLOAT_COLUMNS + TRAILING_FLOAT_COLUMNS

CREATE_TABLE_SQL = f"""
IF OBJECT_ID(N'{TABLE}', N'U') IS NULL
BEGIN
    CREATE TABLE {TABLE} (
        {TIMESTAMP_COLUMN} DATETIME2(0) NOT NULL CONSTRAINT PK_legacy_logistics_events PRIMARY KEY,
{chr(10).join(f"        {c} FLOAT NOT NULL," for c in FLOAT_COLUMNS)}
        {CATEGORY_COLUMN} VARCHAR(20) NOT NULL
            CONSTRAINT CK_legacy_logistics_events_risk
            CHECK ({CATEGORY_COLUMN} IN ({", ".join(f"'{r}'" for r in RISK_CLASSES)})),
{chr(10).join(f"        {c} FLOAT NOT NULL," for c in TRAILING_FLOAT_COLUMNS)}
        ingested_at DATETIME2(0) NOT NULL
            CONSTRAINT DF_legacy_logistics_events_ingested_at DEFAULT SYSUTCDATETIME()
    );
END
"""

INSERT_SQL = (
    f"INSERT INTO {TABLE} ({', '.join(CSV_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(CSV_COLUMNS))})"
)

Row = tuple  # (datetime, float..., str, float)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    def __post_init__(self) -> None:
        # The database name is interpolated into DDL, so keep it to a safe set.
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", self.database):
            raise ValueError(f"Unsafe database name: {self.database!r}")


def load_env_file(path: Path) -> None:
    """Load ``KEY=VALUE`` pairs from *path* into ``os.environ`` (no override)."""
    if not path.is_file():
        return
    from dotenv import load_dotenv

    load_dotenv(path, override=False)


def config_from_env(env: dict[str, str] | None = None) -> DbConfig:
    env = os.environ if env is None else env
    password = env.get("SQL_SERVER_PASSWORD") or env.get("MSSQL_SA_PASSWORD")
    if not password:
        raise ValueError("Set MSSQL_SA_PASSWORD (or SQL_SERVER_PASSWORD) in .env")
    return DbConfig(
        host=env.get("SQL_SERVER_HOST", "localhost"),
        port=int(env.get("SQL_SERVER_PORT", "1433")),
        user=env.get("SQL_SERVER_USER", "sa"),
        password=password,
        database=env.get("SQL_SERVER_DATABASE", "cold_chain"),
    )


# --------------------------------------------------------------------------- #
# CSV parsing / validation
# --------------------------------------------------------------------------- #

class DataError(ValueError):
    """Raised when the CSV does not match the expected structure or values."""


def parse_row(record: dict[str, str], line_no: int) -> Row:
    """Convert a CSV record to a typed tuple ordered like ``CSV_COLUMNS``."""
    try:
        ts = datetime.strptime(record[TIMESTAMP_COLUMN].strip(), TIMESTAMP_FORMAT)
        floats = {c: float(record[c]) for c in ALL_FLOAT_COLUMNS}
    except (ValueError, TypeError, KeyError) as exc:
        raise DataError(f"line {line_no}: {exc}") from exc

    for column, value in floats.items():
        if value != value or value in (float("inf"), float("-inf")):
            raise DataError(f"line {line_no}: {column} is not finite")

    risk = (record[CATEGORY_COLUMN] or "").strip()
    if risk not in RISK_CLASSES:
        raise DataError(f"line {line_no}: unknown {CATEGORY_COLUMN} {risk!r}")

    return (
        ts,
        *(floats[c] for c in FLOAT_COLUMNS),
        risk,
        *(floats[c] for c in TRAILING_FLOAT_COLUMNS),
    )


def read_rows(csv_path: Path) -> Iterator[Row]:
    """Yield validated rows from *csv_path*; raises ``DataError`` on bad data."""
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        missing = set(CSV_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise DataError(f"CSV is missing columns: {sorted(missing)}")
        for line_no, record in enumerate(reader, start=2):  # header is line 1
            yield parse_row(record, line_no)


def batched(rows: Sequence[Row], size: int) -> Iterator[Sequence[Row]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #

def connect(cfg: DbConfig, database: str, autocommit: bool = False):
    import pymssql

    return pymssql.connect(
        server=cfg.host,
        port=str(cfg.port),
        user=cfg.user,
        password=cfg.password,
        database=database,
        autocommit=autocommit,
        login_timeout=15,
    )


def ensure_database(cfg: DbConfig) -> None:
    with connect(cfg, "master", autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"IF DB_ID(N'{cfg.database}') IS NULL CREATE DATABASE [{cfg.database}]"
            )


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def existing_timestamps(conn) -> set[datetime]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {TIMESTAMP_COLUMN} FROM {TABLE}")
        return {row[0] for row in cur.fetchall()}


def insert_rows(conn, rows: Sequence[Row], batch_size: int) -> int:
    inserted = 0
    with conn.cursor() as cur:
        for chunk in batched(rows, batch_size):
            cur.executemany(INSERT_SQL, list(chunk))
            conn.commit()
            inserted += len(chunk)
            log.info("Inserted %d/%d rows", inserted, len(rows))
    return inserted


def ingest(
    cfg: DbConfig,
    csv_path: Path,
    *,
    replace: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int]:
    """Load *csv_path* into SQL Server. Returns row counters."""
    rows = list(read_rows(csv_path))
    if not rows:
        raise DataError(f"{csv_path} contains no data rows")

    seen: set[datetime] = set()
    for row in rows:
        if row[0] in seen:
            raise DataError(f"duplicate timestamp in CSV: {row[0]}")
        seen.add(row[0])

    ensure_database(cfg)
    with connect(cfg, cfg.database) as conn:
        ensure_table(conn)
        if replace:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {TABLE}")
            conn.commit()
            log.info("Cleared existing rows from %s", TABLE)
            existing: set[datetime] = set()
        else:
            existing = existing_timestamps(conn)

        new_rows = [r for r in rows if r[0] not in existing]
        inserted = insert_rows(conn, new_rows, batch_size) if new_rows else 0

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            total = cur.fetchone()[0]

    return {
        "read": len(rows),
        "skipped_existing": len(rows) - len(new_rows),
        "inserted": inserted,
        "table_total": total,
    }


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="CSV file to load")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV, help="Path to .env")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--replace",
        action="store_true",
        help=f"Delete all rows from {TABLE} before loading",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate the CSV without touching the database",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.batch_size < 1:
        log.error("--batch-size must be >= 1")
        return 2
    if not args.csv.is_file():
        log.error("CSV not found: %s", args.csv)
        return 2

    try:
        if args.dry_run:
            count = sum(1 for _ in read_rows(args.csv))
            log.info("Dry run OK: %d valid rows in %s", count, args.csv)
            return 0

        load_env_file(args.env_file)
        cfg = config_from_env()
        log.info("Target: %s:%d/%s as %s", cfg.host, cfg.port, cfg.database, cfg.user)
        stats = ingest(cfg, args.csv, replace=args.replace, batch_size=args.batch_size)
    except (DataError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    except Exception as exc:  # database / network failures
        log.error("Ingestion failed: %s", exc)
        return 1

    log.info(
        "Done: read=%(read)d inserted=%(inserted)d skipped_existing=%(skipped_existing)d "
        "table_total=%(table_total)d",
        stats,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
