"""Tests for scripts/ingest_legacy_data.py.

Unit tests run offline. The integration test needs the docker-compose SQL Server
and uses a throwaway database that is dropped afterwards.
"""

from __future__ import annotations

import csv
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import ingest_legacy_data as m  # noqa: E402


def make_record(**overrides) -> dict[str, str]:
    record = {c: "1.5" for c in m.ALL_FLOAT_COLUMNS}
    record[m.TIMESTAMP_COLUMN] = "2021-01-01 00:00:00"
    record[m.CATEGORY_COLUMN] = "High Risk"
    record.update(overrides)
    return record


def write_csv(path: Path, records: list[dict[str, str]], columns=m.CSV_COLUMNS) -> Path:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    return path


# ---------------------------- parsing ------------------------------------- #

def test_parse_row_orders_values_like_csv_columns():
    row = m.parse_row(make_record(vehicle_gps_latitude="40.25", delivery_time_deviation="9.5"), 2)
    assert len(row) == len(m.CSV_COLUMNS)
    assert row[0] == datetime(2021, 1, 1)
    assert row[1] == 40.25
    assert row[-2] == "High Risk"
    assert row[-1] == 9.5


@pytest.mark.parametrize(
    "overrides",
    [
        {"timestamp": "01/01/2021"},
        {"fuel_consumption_rate": "abc"},
        {"fuel_consumption_rate": ""},
        {"fuel_consumption_rate": "nan"},
        {"fuel_consumption_rate": "inf"},
        {"risk_classification": "Extreme"},
        {"risk_classification": ""},
    ],
)
def test_parse_row_rejects_bad_values(overrides):
    with pytest.raises(m.DataError):
        m.parse_row(make_record(**overrides), 7)


def test_parse_row_error_mentions_line_number():
    with pytest.raises(m.DataError, match="line 7"):
        m.parse_row(make_record(risk_classification="Nope"), 7)


def test_read_rows_reports_missing_columns(tmp_path):
    path = write_csv(tmp_path / "x.csv", [make_record()], columns=m.CSV_COLUMNS[:-1])
    with pytest.raises(m.DataError, match="missing columns"):
        list(m.read_rows(path))


def test_read_rows_reports_csv_line_of_bad_row(tmp_path):
    records = [make_record(), make_record(timestamp="2021-01-01 01:00:00", risk_classification="x")]
    path = write_csv(tmp_path / "x.csv", records)
    with pytest.raises(m.DataError, match="line 3"):
        list(m.read_rows(path))


def test_real_dataset_is_valid():
    rows = list(m.read_rows(m.DEFAULT_CSV))
    assert len(rows) == 32065
    assert len({r[0] for r in rows}) == len(rows)


def test_batched_splits_without_loss():
    chunks = list(m.batched(list(range(10)), 4))
    assert [len(c) for c in chunks] == [4, 4, 2]


# ---------------------------- config -------------------------------------- #

def test_config_defaults_and_password_fallback():
    cfg = m.config_from_env({"MSSQL_SA_PASSWORD": "pw"})
    assert (cfg.host, cfg.port, cfg.user, cfg.database, cfg.password) == (
        "localhost", 1433, "sa", "cold_chain", "pw",
    )


def test_config_prefers_sql_server_password():
    cfg = m.config_from_env({"SQL_SERVER_PASSWORD": "a", "MSSQL_SA_PASSWORD": "b"})
    assert cfg.password == "a"


def test_config_requires_password():
    with pytest.raises(ValueError, match="MSSQL_SA_PASSWORD"):
        m.config_from_env({})


def test_config_rejects_unsafe_database_name():
    with pytest.raises(ValueError, match="Unsafe database name"):
        m.config_from_env({"MSSQL_SA_PASSWORD": "pw", "SQL_SERVER_DATABASE": "x]; DROP DATABASE y;--"})


# ---------------------------- CLI ----------------------------------------- #

def test_cli_dry_run_does_not_need_database(tmp_path):
    path = write_csv(tmp_path / "x.csv", [make_record()])
    assert m.main(["--csv", str(path), "--dry-run"]) == 0


def test_cli_dry_run_fails_on_invalid_data(tmp_path):
    path = write_csv(tmp_path / "x.csv", [make_record(risk_classification="bad")])
    assert m.main(["--csv", str(path), "--dry-run"]) == 1


def test_cli_missing_csv(tmp_path):
    assert m.main(["--csv", str(tmp_path / "nope.csv"), "--dry-run"]) == 2


# ---------------------------- integration --------------------------------- #

@pytest.fixture()
def test_db():
    m.load_env_file(m.DEFAULT_ENV)
    try:
        base = m.config_from_env()
        name = f"cold_chain_test_{uuid.uuid4().hex[:8]}"
        cfg = m.DbConfig(base.host, base.port, base.user, base.password, name)
        m.connect(cfg, "master").close()
    except Exception as exc:  # no .env / SQL Server not running
        pytest.skip(f"SQL Server unavailable: {exc}")
    yield cfg
    with m.connect(cfg, "master", autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"IF DB_ID(N'{name}') IS NOT NULL BEGIN "
                f"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                f"DROP DATABASE [{name}]; END"
            )


def test_ingest_is_idempotent_and_lossless(tmp_path, test_db):
    records = [
        make_record(timestamp=f"2021-01-01 0{i}:00:00", vehicle_gps_latitude=f"{40 + i / 7!r}")
        for i in range(5)
    ]
    path = write_csv(tmp_path / "x.csv", records)

    first = m.ingest(test_db, path, batch_size=2)
    assert first == {"read": 5, "skipped_existing": 0, "inserted": 5, "table_total": 5}

    second = m.ingest(test_db, path)
    assert second == {"read": 5, "skipped_existing": 5, "inserted": 0, "table_total": 5}

    with m.connect(test_db, test_db.database) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT {', '.join(m.CSV_COLUMNS)} FROM {m.TABLE} ORDER BY timestamp")
        stored = [tuple(r) for r in cur.fetchall()]
    assert stored == list(m.read_rows(path))

    # New rows are appended; --replace reloads from scratch.
    more = write_csv(tmp_path / "y.csv", records + [make_record(timestamp="2021-01-02 00:00:00")])
    assert m.ingest(test_db, more)["inserted"] == 1
    assert m.ingest(test_db, path, replace=True)["table_total"] == 5


def test_ingest_rejects_duplicate_timestamps(tmp_path, test_db):
    path = write_csv(tmp_path / "x.csv", [make_record(), make_record()])
    with pytest.raises(m.DataError, match="duplicate timestamp"):
        m.ingest(test_db, path)
