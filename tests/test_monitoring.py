"""Operational regressions: replays must not disguise a stopped live pipeline."""

import json
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from weather_pipeline.monitoring import check_health
from weather_pipeline.warehouse import _connect


NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)
SETTINGS = {"stations": ["KAUS", "KORD"]}


def dt(minutes=0):
    return (NOW - timedelta(minutes=minutes)).replace(tzinfo=None)


@pytest.fixture
def warehouse(tmp_path):
    connection = _connect(tmp_path / "warehouse.duckdb")
    connection.execute("INSERT INTO dim_date VALUES ('2026-09-13', 2026, 9, 13, 7)")
    connection.execute("""
        INSERT INTO etl_runs VALUES (
            'live', 'fingerprint', 'nws', ?, ?, ?, ?, 100, 98, 2, 0, 3,
            98, 0, 0, 'raw', 'curated', 'rejected'
        )
    """, [dt(180), dt(10), dt(10), dt(10)])
    for station in SETTINGS["stations"]:
        connection.execute("INSERT INTO dim_station VALUES (?, ?, 30, -98, ?)",
                           [station, station, dt(10)])
        connection.execute("""
            INSERT INTO fact_observation VALUES (
                ?, ?, '2026-09-13', 25, 60, 5, 'Fair', 30, -98, 'hash', ?, 'live', ?
            )
        """, [station, dt(20), dt(10), dt(10)])
        connection.execute("INSERT INTO source_checkpoints VALUES (?, ?, 'live', ?)",
                           [station, dt(10), dt(10)])
    connection.close()
    return tmp_path


def edit(warehouse, sql, parameters=None):
    connection = duckdb.connect(str(warehouse / "warehouse.duckdb"))
    try:
        connection.execute(sql, parameters or [])
    finally:
        connection.close()


def events(warehouse, *records):
    (warehouse / "events.jsonl").write_text("".join(json.dumps(record) + "\n" for record in records))


def event(minutes, status, **extra):
    return {"at": (NOW - timedelta(minutes=minutes)).isoformat(), "status": status, **extra}


def check(report, name):
    return next(item for item in report["checks"] if item["name"] == name)


def test_healthy_report_is_json_safe_and_read_only(warehouse):
    path = warehouse / "warehouse.duckdb"
    original, modified = path.read_bytes(), path.stat().st_mtime_ns
    report = check_health(warehouse, SETTINGS, now=NOW)
    assert report["status"] == "healthy"
    assert report["counts"]["observation_count"] == 2
    assert report["stations"][0]["observation_age_minutes"] == 20
    assert check(report, "rejection_rate")["details"]["rejection_pct"] == 2
    assert check(report, "missing_measurements")["details"]["missing_measurement_pct"] == pytest.approx(100 / 98)
    json.dumps(report, allow_nan=False)
    assert path.read_bytes() == original
    assert path.stat().st_mtime_ns == modified


def test_stale_station_is_not_hidden_by_fresh_other_station(warehouse):
    edit(warehouse, "UPDATE fact_observation SET observed_at = ? WHERE station_id = 'KAUS'", [dt(240)])
    report = check_health(warehouse, SETTINGS, now=NOW)
    assert report["status"] == "critical"
    assert check(report, "observations:KAUS")["status"] == "critical"
    assert check(report, "observations:KORD")["status"] == "healthy"


def test_missing_station_and_checkpoint_are_identified(warehouse):
    report = check_health(warehouse, {"stations": ["KAUS", "KJFK"]}, now=NOW)
    assert check(report, "observations:KJFK")["status"] == "critical"
    assert check(report, "checkpoint:KJFK")["status"] == "critical"
    assert report["stations"][1]["latest_observed_at"] is None


def test_recent_replay_does_not_refresh_old_live_checkpoint(warehouse):
    edit(warehouse, "UPDATE source_checkpoints SET updated_at = ?, watermark = ?", [dt(300), dt(300)])
    events(warehouse, event(1, "succeeded", batch_id="live", load={"status": "already_loaded"}))
    report = check_health(warehouse, SETTINGS, now=NOW)
    assert check(report, "checkpoint:KAUS")["status"] == "critical"
    assert report["stations"][0]["live_run_age_minutes"] == 300


def test_recent_completion_with_old_watermark_is_still_behind(warehouse):
    edit(warehouse, "UPDATE source_checkpoints SET watermark = ?", [dt(300)])
    report = check_health(warehouse, SETTINGS, now=NOW)
    assert report["stations"][0]["live_run_age_minutes"] == 10
    assert check(report, "checkpoint:KAUS")["status"] == "critical"


@pytest.mark.parametrize("status", ["failed", "ingestion_failed"])
def test_recent_failure_survives_unrelated_replay_success(warehouse, status):
    events(warehouse, event(5, status, batch_id="broken", error="API timeout"),
           event(1, "succeeded", batch_id="live", load={"status": "already_loaded"}))
    report = check_health(warehouse, SETTINGS, now=NOW)
    failure = check(report, "recent_failures")
    assert failure["status"] == "critical"
    assert failure["details"]["latest_failure"]["error"] == "API timeout"


def test_successful_retry_of_same_batch_resolves_failure(warehouse):
    events(warehouse, event(5, "failed", batch_id="live", error="Export failed"),
           event(1, "succeeded", batch_id="live", load={"status": "already_loaded"}))
    assert check(check_health(warehouse, SETTINGS, NOW), "recent_failures")["status"] == "healthy"


def test_new_live_checkpoint_resolves_older_ingestion_failure(warehouse):
    events(warehouse, event(20, "ingestion_failed", error="API timeout"))
    assert check(check_health(warehouse, SETTINGS, NOW), "recent_failures")["status"] == "healthy"


def test_retired_station_checkpoint_cannot_clear_current_ingestion_failure(warehouse):
    edit(warehouse, "INSERT INTO source_checkpoints VALUES ('RETIRED', ?, 'retired', ?)", [dt(1), dt(1)])
    events(warehouse, event(5, "ingestion_failed", error="API timeout"))
    assert check(check_health(warehouse, SETTINGS, NOW), "recent_failures")["status"] == "critical"


def test_recent_backfill_cannot_hide_bad_live_quality(warehouse):
    edit(warehouse, "UPDATE etl_runs SET input_count = 100, accepted_count = 90, rejected_count = 10")
    edit(warehouse, """
        INSERT INTO etl_runs SELECT 'backfill', input_fingerprint, source, window_start,
            window_end, source_ingested_at, ?, 100, 100, 0, 0, 0,
            100, 0, 0, raw_path, curated_path, rejected_path FROM etl_runs WHERE batch_id = 'live'
    """, [dt(1)])
    report = check_health(warehouse, SETTINGS, NOW)
    quality = check(report, "rejection_rate")
    assert quality["status"] == "warning"
    assert quality["details"]["batch_id"] == "live"
    assert quality["details"]["rejection_pct"] == 10


def test_empty_live_batch_does_not_divide_by_zero(warehouse):
    edit(warehouse, "UPDATE etl_runs SET input_count = 0, accepted_count = 0, rejected_count = 0, missing_measurement_count = 0")
    report = check_health(warehouse, SETTINGS, NOW)
    assert check(report, "rejection_rate")["status"] == "warning"
    json.dumps(report, allow_nan=False)


@pytest.mark.parametrize("column, table", [("observed_at", "fact_observation"),
                                           ("watermark", "source_checkpoints"),
                                           ("updated_at", "source_checkpoints")])
def test_future_timestamps_are_reported(warehouse, column, table):
    edit(warehouse, f"UPDATE {table} SET {column} = ? WHERE station_id = 'KAUS'", [dt(-10)])
    assert check_health(warehouse, SETTINGS, NOW)["status"] == "critical"


def test_small_clock_skew_is_tolerated(warehouse):
    edit(warehouse, "UPDATE source_checkpoints SET watermark = ?, updated_at = ?", [dt(-2), dt(-2)])
    assert check_health(warehouse, SETTINGS, NOW)["status"] == "healthy"


@pytest.mark.parametrize("kind", ["missing", "corrupt", "empty_file", "missing_schema", "empty_schema"])
def test_uninitialized_and_damaged_warehouses_are_actionable(tmp_path, kind):
    path = tmp_path / "warehouse.duckdb"
    if kind == "corrupt":
        path.write_text("not a database")
    elif kind == "empty_file":
        path.touch()
    elif kind == "missing_schema":
        duckdb.connect(str(path)).close()
    elif kind == "empty_schema":
        _connect(path).close()
    report = check_health(tmp_path, SETTINGS, NOW)
    assert report["status"] == "critical"
    if kind == "missing":
        assert "bootstrap" in check(report, "warehouse")["message"]
        assert not path.exists()
    elif kind == "empty_schema":
        assert check(report, "warehouse_records")["status"] == "critical"
    else:
        assert "restore" in check(report, "warehouse")["message"]


def test_locked_database_is_unavailable_instead_of_crashing(warehouse, monkeypatch):
    def locked(*args, **kwargs):
        assert kwargs["read_only"] is True
        raise duckdb.IOException("Conflicting lock is held in another process")
    monkeypatch.setattr("weather_pipeline.monitoring.duckdb.connect", locked)
    report = check_health(warehouse, SETTINGS, NOW)
    assert report["status"] == "critical"
    assert "retry after it finishes" in check(report, "warehouse")["message"]


@pytest.mark.parametrize("value", [True, False, 0, -1, float("nan"), float("inf"), "180"])
@pytest.mark.parametrize("name", ["max_observation_age_minutes", "max_run_age_minutes", "max_rejection_pct"])
def test_invalid_thresholds_are_rejected(tmp_path, name, value):
    with pytest.raises(ValueError, match=name):
        check_health(tmp_path, {**SETTINGS, "health": {name: value}}, NOW)


def test_threshold_overrides_change_health(warehouse):
    report = check_health(warehouse, {**SETTINGS, "health": {"max_observation_age_minutes": 15}}, NOW)
    assert check(report, "observations:KAUS")["status"] == "critical"


def test_event_history_is_bounded_and_bad_recent_lines_are_reported(warehouse):
    path = warehouse / "events.jsonl"
    path.write_text("unread old junk\n" * 100_000 + json.dumps(event(1, "failed", batch_id="bad")) + "\n")
    report = check_health(warehouse, SETTINGS, NOW)
    recent = check(report, "recent_failures")
    assert recent["status"] == "critical"
    assert recent["details"]["truncated"] is True
    assert recent["details"]["events_checked"] <= 2000
    assert check(report, "event_log")["status"] == "warning"


def test_future_failure_event_is_not_reported_as_a_current_failure(warehouse):
    events(warehouse, event(-60, "failed", batch_id="future"))
    report = check_health(warehouse, SETTINGS, NOW)
    assert check(report, "recent_failures")["status"] == "healthy"
    assert check(report, "event_log")["details"]["future_events"] == 1
