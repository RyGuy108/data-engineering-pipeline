"""Warehouse correctness tests using real Parquet fixtures, without Spark."""

import csv
import json
from copy import deepcopy
from pathlib import Path

import duckdb
import pytest

from weather_pipeline.warehouse import (
    export_dashboard,
    get_checkpoints,
    load_batch,
    warehouse_summary,
)


def make_batch(tmp_path, batch_id="batch-1", rows=None,
               ingested_at="2026-09-12T02:00:00+00:00",
               end="2026-09-12T02:00:00+00:00", advance=True):
    directory = tmp_path / batch_id
    directory.mkdir(exist_ok=True)
    if rows is None:
        rows = [{}]
    observations = []
    for updates in rows:
        observation = {
            "station_id": "KORD", "observed_at": "2026-09-12T01:00:00",
            "temperature_c": 20.0, "humidity_pct": 50.0, "wind_kph": 12.0,
            "text_description": "Mostly clear", "latitude": 41.98, "longitude": -87.9,
            "payload_hash": "aaa", "source_ingested_at": ingested_at,
        }
        observation.update(updates)
        observations.append(observation)
    raw = directory / "raw.ndjson"
    raw.write_text("\n".join(json.dumps(row, sort_keys=True) for row in observations))
    curated = directory / "curated"
    curated.mkdir(exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute("""
            CREATE TABLE fixture (
                station_id VARCHAR, observed_at TIMESTAMP, temperature_c DOUBLE,
                humidity_pct DOUBLE, wind_kph DOUBLE, text_description VARCHAR,
                latitude DOUBLE, longitude DOUBLE, payload_hash VARCHAR,
                source_ingested_at TIMESTAMP
            )
        """)
        if observations:
            connection.executemany("INSERT INTO fixture VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                   [list(row.values()) for row in observations])
        connection.execute("COPY fixture TO ? (FORMAT PARQUET)", [str(curated / "part-0.parquet")])
    finally:
        connection.close()
    manifest = {
        "batch_id": batch_id, "source": "nws", "ingested_at": ingested_at,
        "start": "2026-09-12T00:00:00+00:00", "end": end,
        "raw_path": str(raw), "manifest_path": str(directory / "manifest.json"),
        "stations": [{"station_id": "KORD", "name": "Chicago, O'Hare",
                      "latitude": 41.98, "longitude": -87.9}],
        "windows": [{"station_id": "KORD", "start": "2026-09-12T00:00:00+00:00", "end": end}],
        "advance_checkpoint": advance,
    }
    result = {
        "curated_path": str(curated), "rejected_path": str(directory / "rejected"),
        "report": {
            "input_count": len(observations), "accepted_count": len(observations),
            "rejected_count": 0, "duplicate_count": 0,
            "missing_measurement_count": sum(
                row[field] is None for row in observations
                for field in ("temperature_c", "humidity_pct", "wind_kph")
            ),
        },
    }
    return manifest, result


def query(db, sql):
    with duckdb.connect(str(db)) as connection:
        return connection.execute(sql).fetchall()


def test_can_initialize_missing_warehouse(tmp_path):
    db = tmp_path / "nested" / "warehouse.duckdb"
    assert get_checkpoints(db) == {}
    summary = warehouse_summary(db)
    assert summary["observation_count"] == 0
    assert summary["latest_observed_at"] is None


def test_loads_star_schema_and_missing_measurement_metrics(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    manifest, result = make_batch(tmp_path, rows=[{}, {
        "observed_at": "2026-09-12T01:30:00", "temperature_c": None,
        "wind_kph": None, "payload_hash": "bbb",
    }])
    outcome = load_batch(db, manifest, result)
    assert outcome["inserted_count"] == 2
    assert query(db, "SELECT count(*) FROM dim_date") == [(1,)]
    assert query(db, "SELECT observation_count, missing_measurement_count, measurement_completeness_pct FROM dashboard_daily") == [
        (2, 2, pytest.approx(100 * 4 / 6))
    ]
    assert query(db, "SELECT station_name FROM dashboard_daily") == [("Chicago, O'Hare",)]
    assert warehouse_summary(db)["observation_count"] == 2
    assert get_checkpoints(db) == {"KORD": manifest["end"]}


def test_replaying_committed_batch_is_exact_noop(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    manifest, result = make_batch(tmp_path)
    load_batch(db, manifest, result)
    before = query(db, "SELECT * FROM fact_observation")
    checkpoint_before = query(db, "SELECT * FROM source_checkpoints")
    second = load_batch(db, manifest, result, fail_before_commit=True)
    assert second["status"] == "already_loaded"
    assert second["inserted_count"] == second["updated_count"] == 0
    assert query(db, "SELECT * FROM fact_observation") == before
    assert query(db, "SELECT * FROM source_checkpoints") == checkpoint_before
    assert query(db, "SELECT count(*) FROM etl_runs") == [(1,)]


def test_changed_payload_updates_and_older_replay_cannot_clobber(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    load_batch(db, *make_batch(tmp_path))
    corrected = make_batch(tmp_path, "correction", rows=[{"temperature_c": 22.0, "payload_hash": "bbb"}],
                           ingested_at="2026-09-12T04:00:00+00:00")
    outcome = load_batch(db, *corrected)
    assert outcome["updated_count"] == 1
    stale = make_batch(tmp_path, "stale", rows=[{"temperature_c": 19.0, "payload_hash": "ccc"}],
                       ingested_at="2026-09-12T03:00:00+00:00")
    assert load_batch(db, *stale)["unchanged_count"] == 1
    assert query(db, "SELECT temperature_c, batch_id FROM fact_observation") == [(22.0, "correction")]
    assert query(db, "SELECT count(*) FROM fact_observation") == [(1,)]


def test_newer_unchanged_payload_protects_against_intermediate_stale_version(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    load_batch(db, *make_batch(tmp_path))
    before = query(db, "SELECT * FROM fact_observation")
    same = make_batch(tmp_path, "same-newer", ingested_at="2026-09-12T05:00:00+00:00")
    assert load_batch(db, *same)["unchanged_count"] == 1
    assert query(db, "SELECT * FROM fact_observation") == before
    stale = make_batch(tmp_path, "intermediate", rows=[{"temperature_c": 17.0, "payload_hash": "bbb"}],
                       ingested_at="2026-09-12T04:00:00+00:00")
    assert load_batch(db, *stale)["updated_count"] == 0
    assert query(db, "SELECT * FROM fact_observation") == before


def test_equal_ingestion_times_have_order_independent_payload_winner(tmp_path):
    first = make_batch(tmp_path, "hash-a", rows=[{"temperature_c": 20.0, "payload_hash": "aaa"}])
    second = make_batch(tmp_path, "hash-b", rows=[{"temperature_c": 21.0, "payload_hash": "bbb"}])
    for name, batches in (("forward", [first, second]), ("reverse", [second, first])):
        db = tmp_path / f"{name}.duckdb"
        for batch in batches:
            load_batch(db, *batch)
        assert query(db, "SELECT temperature_c, payload_hash FROM fact_observation") == [(21.0, "bbb")]


def test_checkpoint_is_monotonic_and_backfill_can_leave_it_unchanged(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    latest = make_batch(tmp_path, "latest", end="2026-09-12T06:00:00+00:00")
    load_batch(db, *latest)
    checkpoint_before = query(db, "SELECT * FROM source_checkpoints")
    load_batch(db, *make_batch(tmp_path, "earlier", end="2026-09-12T03:00:00+00:00"))
    assert query(db, "SELECT * FROM source_checkpoints") == checkpoint_before
    load_batch(db, *make_batch(tmp_path, "backfill", end="2026-09-12T08:00:00+00:00", advance=False))
    assert query(db, "SELECT * FROM source_checkpoints") == checkpoint_before


def test_failure_before_commit_rolls_back_every_table_and_retry_succeeds(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    batch = make_batch(tmp_path)
    with pytest.raises(RuntimeError, match="Injected failure"):
        load_batch(db, *batch, fail_before_commit=True)
    for table in ("dim_station", "dim_date", "fact_observation", "observation_versions", "etl_runs", "source_checkpoints"):
        assert query(db, f"SELECT count(*) FROM {table}") == [(0,)]
    assert load_batch(db, *batch)["inserted_count"] == 1
    assert get_checkpoints(db) == {"KORD": batch[0]["end"]}


def test_failure_during_update_preserves_existing_data_audit_and_checkpoint(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    load_batch(db, *make_batch(tmp_path))
    tables = ("dim_station", "dim_date", "fact_observation", "observation_versions", "etl_runs", "source_checkpoints")
    before = {table: query(db, f"SELECT * FROM {table}") for table in tables}
    correction = make_batch(tmp_path, "correction", rows=[{"temperature_c": 25.0, "payload_hash": "bbb"}],
                            ingested_at="2026-09-12T04:00:00+00:00", end="2026-09-12T04:00:00+00:00")
    correction[0]["stations"][0]["name"] = "Renamed airport"
    with pytest.raises(RuntimeError):
        load_batch(db, *correction, fail_before_commit=True)
    assert {table: query(db, f"SELECT * FROM {table}") for table in tables} == before


def test_batch_id_collision_is_rejected(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    load_batch(db, *make_batch(tmp_path))
    changed = make_batch(tmp_path, rows=[{"temperature_c": 30.0, "payload_hash": "ddd"}])
    with pytest.raises(ValueError, match="Batch ID collision"):
        load_batch(db, *changed)
    assert query(db, "SELECT temperature_c FROM fact_observation") == [(20.0,)]
    assert query(db, "SELECT count(*) FROM etl_runs") == [(1,)]


def test_manifest_change_is_also_a_batch_id_collision(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    manifest, result = make_batch(tmp_path)
    load_batch(db, manifest, result)
    changed = deepcopy(manifest)
    changed["advance_checkpoint"] = False
    with pytest.raises(ValueError, match="Batch ID collision"):
        load_batch(db, changed, result)


def test_empty_curated_batch_audits_rejections_and_advances_checkpoint(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    manifest, result = make_batch(tmp_path, rows=[])
    result["report"].update(input_count=3, rejected_count=2, duplicate_count=1)
    assert load_batch(db, manifest, result)["inserted_count"] == 0
    assert get_checkpoints(db) == {"KORD": manifest["end"]}
    assert query(db, "SELECT rejected_count, rejection_pct FROM dashboard_quality") == [(2, pytest.approx(200 / 3))]


@pytest.mark.parametrize("invalid_rows, message", [
    ([{}, {}], "duplicate business keys"),
    ([{"station_id": "UNKNOWN"}], "no station metadata"),
    ([{"observed_at": None}], "missing required fields"),
])
def test_invalid_curated_contract_never_advances_checkpoint(tmp_path, invalid_rows, message):
    db = tmp_path / "warehouse.duckdb"
    with pytest.raises(ValueError, match=message):
        load_batch(db, *make_batch(tmp_path, rows=invalid_rows))
    assert get_checkpoints(db) == {}
    assert query(db, "SELECT count(*) FROM etl_runs") == [(0,)]


def test_mismatched_quality_report_is_rejected(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    manifest, result = make_batch(tmp_path)
    result["report"]["missing_measurement_count"] = 1
    with pytest.raises(ValueError, match="Missing measurement count"):
        load_batch(db, manifest, result)
    assert get_checkpoints(db) == {}


def test_dashboard_exports_correct_values_and_headers_even_when_empty(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    empty = export_dashboard(db, tmp_path / "exports")
    assert set(empty) == {"dashboard_observations", "dashboard_daily", "dashboard_quality"}
    with Path(empty["dashboard_daily"]).open(newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        assert "measurement_completeness_pct" in reader.fieldnames
        assert list(reader) == []
    load_batch(db, *make_batch(tmp_path, rows=[{"temperature_c": None}]))
    exported = export_dashboard(db, tmp_path / "exports")
    with Path(exported["dashboard_observations"]).open(newline="") as csv_file:
        observations = list(csv.DictReader(csv_file))
    assert observations[0]["station_name"] == "Chicago, O'Hare"
    assert observations[0]["temperature_c"] == ""
    assert observations[0]["missing_measurement_count"] == "1"
    assert observations[0]["observation_date"] == "2026-09-12"
    assert not list((tmp_path / "exports").glob("*.tmp"))
