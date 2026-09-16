"""Exercise the real raw → Spark → transactional warehouse → CSV boundary."""

import csv
import json
from pathlib import Path

import duckdb
import pytest

from weather_pipeline.pipeline import configure_runtime, process_manifest
from weather_pipeline.transform import create_spark
from weather_pipeline.warehouse import get_checkpoints, warehouse_summary


@pytest.fixture(scope="module")
def spark():
    configure_runtime()
    session = create_spark()
    yield session
    session.stop()


def observation(timestamp, temperature=20.0, humidity=60.0, wind=10.0):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-87.9, 41.9]},
        "properties": {
            "station": "https://api.weather.gov/stations/KORD",
            "timestamp": timestamp,
            "temperature": {"value": temperature, "unitCode": "wmoUnit:degC"},
            "relativeHumidity": {"value": humidity, "unitCode": "wmoUnit:percent"},
            "windSpeed": {"value": wind, "unitCode": "wmoUnit:km_h-1"},
            "textDescription": "Clear",
        },
    }


def manifest(tmp_path, batch_id, end, features):
    raw_path = tmp_path / f"{batch_id}.ndjson"
    raw_path.write_text("".join(json.dumps({
        "batch_id": batch_id, "station_id": "KORD", "ingested_at": end,
        "raw_json": json.dumps(feature, sort_keys=True, separators=(",", ":")),
    }) + "\n" for feature in features))
    return {
        "batch_id": batch_id, "ingested_at": end, "start": "2026-09-12T00:00:00Z",
        "end": end, "raw_path": str(raw_path), "source": "nws", "advance_checkpoint": True,
        "stations": [{"station_id": "KORD", "name": "Chicago O'Hare", "latitude": 41.9, "longitude": -87.9}],
        "windows": [{"station_id": "KORD", "start": "2026-09-12T00:00:00Z", "end": end}],
    }


def database_snapshot(db_path):
    # Includes load timestamps and version ledger, proving exact replay has no
    # hidden rewrites or checkpoint/audit side effects inside the warehouse.
    with duckdb.connect(str(db_path), read_only=True) as connection:
        return {
            table: connection.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
            for table in ("fact_observation", "dim_station", "dim_date", "observation_versions", "etl_runs", "source_checkpoints")
        }


def test_real_pipeline_replay_rollback_and_recovery(spark, tmp_path):
    data_dir = tmp_path / "pipeline"
    first = manifest(tmp_path, "first", "2026-09-12T13:00:00Z", [
        observation("2026-09-12T12:00:00Z"),
        observation("2026-09-12T12:00:00Z"),
        observation("2026-09-12T12:30:00Z", temperature=None, wind=None),
        observation("2026-09-12T12:40:00Z", humidity=101),
    ])
    initial = process_manifest(first, data_dir, spark=spark)
    for key, expected in {
        "input_count": 4, "accepted_count": 2, "rejected_count": 1,
        "duplicate_count": 1, "missing_measurement_count": 2,
    }.items():
        assert initial["quality"][key] == expected
    assert initial["load"]["status"] == "loaded"
    assert initial["load"]["inserted_count"] == 2
    db_path = data_dir / "warehouse.duckdb"
    initial_snapshot = database_snapshot(db_path)
    initial_exports = {key: Path(path).read_bytes() for key, path in initial["exports"].items()}
    initial_report = (data_dir / "reports" / "first.json").read_bytes()
    with duckdb.connect(str(db_path), read_only=True) as connection:
        committed_curated = Path(connection.execute("SELECT curated_path FROM etl_runs WHERE batch_id = 'first'").fetchone()[0])
    committed_files = {str(path.relative_to(committed_curated)): path.read_bytes()
                       for path in committed_curated.rglob("*") if path.is_file()}
    assert get_checkpoints(db_path) == {"KORD": "2026-09-12T13:00:00+00:00"}

    replay = process_manifest(first, data_dir, spark=spark)
    assert replay["load"]["status"] == "already_loaded"
    assert replay["load"]["inserted_count"] == replay["load"]["updated_count"] == 0
    assert database_snapshot(db_path) == initial_snapshot
    assert {key: Path(path).read_bytes() for key, path in replay["exports"].items()} == initial_exports

    collision_dir = tmp_path / "collision"
    collision_dir.mkdir()
    collision = manifest(collision_dir, "first", "2026-09-12T13:00:00Z", [
        observation("2026-09-12T12:00:00Z", temperature=25.0),
        observation("2026-09-12T12:00:00Z", temperature=25.0),
        observation("2026-09-12T12:30:00Z", temperature=None, wind=None),
        observation("2026-09-12T12:40:00Z", humidity=101),
    ])
    with pytest.raises(ValueError, match="Batch ID collision"):
        process_manifest(collision, data_dir, spark=spark)
    assert database_snapshot(db_path) == initial_snapshot
    assert (data_dir / "reports" / "first.json").read_bytes() == initial_report
    assert {str(path.relative_to(committed_curated)): path.read_bytes()
            for path in committed_curated.rglob("*") if path.is_file()} == committed_files

    correction = manifest(tmp_path, "correction", "2026-09-12T14:00:00Z", [
        observation("2026-09-12T12:00:00Z", temperature=23.0),
        observation("2026-09-12T13:30:00Z", temperature=24.0),
    ])
    with pytest.raises(RuntimeError, match="Injected failure before warehouse commit"):
        process_manifest(correction, data_dir, spark=spark, fail_before_commit=True)
    assert database_snapshot(db_path) == initial_snapshot
    assert get_checkpoints(db_path) == {"KORD": "2026-09-12T13:00:00+00:00"}
    assert {key: Path(path).read_bytes() for key, path in initial["exports"].items()} == initial_exports

    recovered = process_manifest(correction, data_dir, spark=spark)
    assert recovered["load"]["status"] == "loaded"
    assert recovered["load"]["inserted_count"] == recovered["load"]["updated_count"] == 1
    assert get_checkpoints(db_path) == {"KORD": "2026-09-12T14:00:00+00:00"}
    summary = warehouse_summary(db_path)
    assert summary["observation_count"] == 3
    assert summary["completed_batch_count"] == 2
    with duckdb.connect(str(db_path), read_only=True) as connection:
        assert connection.execute("SELECT temperature_c FROM fact_observation WHERE observed_at = '2026-09-12 12:00:00'").fetchone()[0] == 23.0
        completeness = connection.execute("SELECT measurement_completeness_pct FROM dashboard_daily").fetchone()[0]
        assert completeness == pytest.approx(100.0 * 7 / 9)
    with Path(recovered["exports"]["dashboard_observations"]).open(newline="") as handle:
        exported = list(csv.DictReader(handle))
    assert len(exported) == 3
    missing = next(row for row in exported if row["observed_at"] == "2026-09-12 12:30:00")
    assert missing["temperature_c"] == missing["wind_kph"] == ""
    assert missing["station_name"] == "Chicago O'Hare"
    events = [json.loads(line) for line in (data_dir / "events.jsonl").read_text().splitlines()]
    assert [event["status"] for event in events] == ["succeeded", "succeeded", "failed", "failed", "succeeded"]
