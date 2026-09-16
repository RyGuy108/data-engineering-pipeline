"""Proof that the shipped recording exercises the real pipeline without HTTP."""

import hashlib
import json
from importlib.resources import files
from unittest.mock import patch

import duckdb
import pytest

from weather_pipeline.demo import render_demo_markdown, run_demo


@pytest.mark.parametrize("kind", ["directory", "file", "symlink"])
def test_demo_refuses_existing_content_before_runtime_starts(tmp_path, kind):
    destination = tmp_path / "demo"
    if kind == "directory":
        destination.mkdir()
        sentinel = destination / "warehouse.duckdb"
        sentinel.write_bytes(b"live data must remain untouched")
    elif kind == "file":
        destination.write_bytes(b"existing file")
        sentinel = destination
    else:
        sentinel = tmp_path / "existing"
        sentinel.mkdir()
        destination.symlink_to(sentinel, target_is_directory=True)
    original = sentinel.read_bytes() if sentinel.is_file() else list(sentinel.iterdir())
    with patch("weather_pipeline.demo.create_spark") as create_spark:
        with pytest.raises(ValueError, match="destination must"):
            run_demo(destination)
        create_spark.assert_not_called()
    assert (sentinel.read_bytes() if sentinel.is_file() else list(sentinel.iterdir())) == original


def test_packaged_sample_has_complete_public_provenance():
    sample = json.loads(files("weather_pipeline").joinpath(
        "examples/recorded-nws-sample.json"
    ).read_text())
    assert sample["source"] == "recorded_nws_demo"
    sources = {source["batch_id"]: source for source in sample["source_batches"]}
    for batch in sample["batches"].values():
        source = sources[batch["source_batch_id"]]
        assert source["source_request"]["base_url"] == "https://api.weather.gov"
        assert source["ingested_at"].endswith("Z")
        assert source["end"].endswith("Z")
        assert "raw_path" not in source and "manifest_path" not in source
        for row in batch["records"]:
            payload = json.loads(row["raw_json"])
            assert hashlib.sha256(row["raw_json"].encode()).hexdigest() == row["payload_sha256"]
            assert row["batch_id"] == source["batch_id"]
            assert row["ingested_at"] == source["ingested_at"]
            assert payload["id"].startswith("https://api.weather.gov/stations/")
            assert "demo_synthetic_probe" not in payload


@pytest.fixture(scope="module")
def demo_result(tmp_path_factory):
    destination = tmp_path_factory.mktemp("recorded-demo")
    # An existing empty destination is supported. One actual Spark session and
    # scenario supplies all integration assertions instead of repeating the run.
    with patch("requests.sessions.Session.request", side_effect=AssertionError("Demo attempted HTTP")):
        report = run_demo(destination)
    return destination, report


@pytest.mark.spark
def test_recorded_demo_proves_replay_rollback_recovery_and_dashboard(demo_result):
    destination, report = demo_result
    assert report["status"] == "passed"
    assert report["network_required"] is False
    assert len(report["checks"]) >= 20
    assert all(check["status"] == "passed" for check in report["checks"])
    assert report["summary"]["observation_count"] == 18
    assert report["summary"]["station_count"] == 3
    assert report["summary"]["completed_batch_count"] == 2
    quality = report["phases"]["initial"]["quality"]
    assert {key: quality[key] for key in (
        "input_count", "accepted_count", "duplicate_count", "rejected_count", "missing_measurement_count"
    )} == {
        "input_count": 16, "accepted_count": 12, "duplicate_count": 1,
        "rejected_count": 3, "missing_measurement_count": 4,
    }
    assert report["phases"]["replay"]["load"]["status"] == "already_loaded"
    recovered = report["phases"]["recovered_incremental"]["load"]
    assert recovered["inserted_count"] == 6
    assert recovered["updated_count"] == 0
    assert recovered["unchanged_count"] == 3
    assert report["dashboard"]["audit_status"] == "passed"
    assert (destination / "tableau" / "weather_observatory.twbx").is_file()
    assert json.loads((destination / "report.json").read_text()) == report
    assert (destination / "report.md").read_text() == render_demo_markdown(report)
    with duckdb.connect(str(destination / "warehouse.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT DISTINCT source FROM etl_runs").fetchall() == [("recorded_nws_demo",)]
        assert connection.execute("SELECT count(*) FROM fact_observation WHERE humidity_pct > 100").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM fact_observation WHERE wind_kph IS NULL").fetchone()[0] == 5
    events = [json.loads(line) for line in (destination / "events.jsonl").read_text().splitlines()]
    assert [event["status"] for event in events] == ["succeeded", "succeeded", "failed", "succeeded"]
    assert events[2]["error"] == "Injected failure before warehouse commit"


def test_failed_runtime_leaves_a_readable_failure_report(tmp_path):
    destination = tmp_path / "failed-demo"
    with patch("weather_pipeline.demo.create_spark", side_effect=RuntimeError("Java unavailable")):
        with pytest.raises(RuntimeError, match="Java unavailable"):
            run_demo(destination)
    report = json.loads((destination / "report.json").read_text())
    assert report["status"] == "failed"
    assert report["error"] == "RuntimeError: Java unavailable"
    assert "Java unavailable" in (destination / "report.md").read_text()
    assert not (destination / "warehouse.duckdb").exists()


def test_markdown_distinguishes_recording_probes_and_native_tableau_check():
    rendered = render_demo_markdown({
        "status": "passed", "checks": [], "dashboard": {"audit_status": "passed"},
        "summary": {"observation_count": 18, "station_count": 3, "completed_batch_count": 2},
    })
    assert "does not represent current weather" in rendered
    assert "synthetic probes are quarantined" in rendered
    assert "Rendering in Tableau" in rendered
