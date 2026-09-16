"""Spark integration tests: bad data stays visible and replay chooses stable rows."""

import hashlib
import json
from copy import deepcopy

import pytest

from weather_pipeline.pipeline import configure_runtime
from weather_pipeline.transform import create_spark, transform_batch


@pytest.fixture(scope="module")
def spark():
    configure_runtime()
    session = create_spark()
    yield session
    session.stop()


def feature(timestamp="2026-09-12T12:00:00+00:00", **properties):
    result = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-87.9, 41.9]},
        "properties": {
            "station": "https://api.weather.gov/stations/KORD",
            "timestamp": timestamp,
            "temperature": {"value": 20.0, "unitCode": "wmoUnit:degC"},
            "relativeHumidity": {"value": 65.0, "unitCode": "wmoUnit:percent"},
            "windSpeed": {"value": 10.0, "unitCode": "wmoUnit:km_h-1"},
            "textDescription": "Clear",
        },
    }
    result["properties"].update(properties)
    return result


def envelope(payload=None, **values):
    result = {
        "batch_id": "test-batch",
        "ingested_at": "2026-09-12T13:00:00Z",
        "station_id": "KORD",
        "raw_json": json.dumps(payload or feature(), sort_keys=True, separators=(",", ":")),
    }
    result.update(values)
    return result


def run(spark, tmp_path, records, name="run", stations=None, reference="2026-09-12T13:00:00Z"):
    raw_path = tmp_path / f"{name}.ndjson"
    raw_path.write_text("".join(json.dumps(record) + "\n" for record in records))
    return transform_batch(spark, raw_path, tmp_path / name, stations or ["KORD"], reference)


def test_normalizes_units_preserves_nulls_and_utc(spark, tmp_path):
    missing = feature(
        timestamp="2026-09-12T12:02:00Z", temperature=None,
        relativeHumidity={"value": None, "unitCode": "wmoUnit:percent"},
        windSpeed={"value": None, "unitCode": "wmoUnit:km_h-1"},
        textDescription=None,
    )
    missing["geometry"] = None
    records = [
        envelope(feature(
            timestamp="2026-09-12T07:00:00-05:00",
            temperature={"value": 68, "unitCode": "wmoUnit:degF"},
            windSpeed={"value": 5, "unitCode": "wmoUnit:m_s-1"},
        )),
        envelope(feature(
            timestamp="2026-09-12T12:01:00Z",
            temperature={"value": 293.15, "unitCode": "wmoUnit:K"},
            windSpeed={"value": 10, "unitCode": "wmoUnit:kn"},
        )),
        envelope(missing),
    ]
    result = run(spark, tmp_path, records)
    rows = spark.read.parquet(result["curated_path"]).orderBy("observed_at").collect()
    expected_counts = {
        "input_count": 3, "accepted_count": 3, "rejected_count": 0,
        "duplicate_count": 0, "missing_measurement_count": 3,
    }
    assert {key: result["report"][key] for key in expected_counts} == expected_counts
    assert rows[0].temperature_c == pytest.approx(20)
    assert rows[0].wind_kph == pytest.approx(18)
    # Cast in Spark so host-local Python datetime conversion cannot affect UTC assertions.
    assert spark.read.parquet(result["curated_path"]).selectExpr("min(observed_at)::string").first()[0] == "2026-09-12 12:00:00"
    assert rows[1].temperature_c == pytest.approx(20)
    assert rows[1].wind_kph == pytest.approx(18.52)
    assert rows[2].temperature_c is rows[2].humidity_pct is rows[2].wind_kph is None
    assert rows[2].latitude is rows[2].longitude is rows[2].text_description is None


def test_quarantines_missing_duplicate_invalid_and_mismatched_records(spark, tmp_path):
    bad_geometry = feature()
    bad_geometry["geometry"]["coordinates"] = [-181, 42]
    cases = [
        (envelope(station_id=""), "missing_station_id"),
        (envelope(station_id="KXXX"), "unknown_station"),
        (envelope(feature(station="https://api.weather.gov/stations/KLAX")), "source_station_mismatch"),
        (envelope(feature(timestamp="not-a-time")), "invalid_observed_at"),
        (envelope(feature(timestamp="2026-09-12T12:00:00")), "invalid_observed_at"),
        (envelope(feature(timestamp="2026-09-12T13:11:00Z")), "future_observed_at"),
        (envelope(ingested_at="2026-02-30T12:00:00Z"), "invalid_ingested_at"),
        (envelope(raw_json="{broken json"), "invalid_payload"),
        (envelope(feature(temperature={"value": "20", "unitCode": "wmoUnit:degC"})), "invalid_temperature_value"),
        (envelope(feature(temperature={"value": True, "unitCode": "wmoUnit:degC"})), "invalid_temperature_value"),
        (envelope(feature(temperature={"value": 100, "unitCode": "wmoUnit:degC"})), "temperature_out_of_range"),
        (envelope(feature(relativeHumidity={"value": 101, "unitCode": "wmoUnit:percent"})), "humidity_out_of_range"),
        (envelope(feature(windSpeed={"value": -1, "unitCode": "wmoUnit:km_h-1"})), "wind_out_of_range"),
        (envelope(feature(windSpeed={"value": 10, "unitCode": "furlongs/fortnight"})), "unsupported_windSpeed_unit"),
        (envelope(feature(temperature="unavailable")), "invalid_temperature_object"),
        (envelope(feature(textDescription=123)), "invalid_text_description"),
        (envelope(bad_geometry), "longitude_out_of_range"),
        (envelope(batch_id=123), "invalid_batch_id_type"),
    ]
    result = run(spark, tmp_path, [envelope(), envelope()] + [row for row, _ in cases])
    report = result["report"]
    assert report["accepted_count"] == 1
    assert report["duplicate_count"] == 1
    assert report["rejected_count"] == len(cases)
    assert report["input_count"] == report["accepted_count"] + report["rejected_count"] + report["duplicate_count"]
    rejected = spark.read.parquet(result["rejected_path"]).collect()
    for raw, reason in cases:
        assert any(row.raw_json == raw["raw_json"] and reason in row.reasons for row in rejected), reason
    assert all(row.raw_envelope for row in rejected)


def test_duplicate_winner_is_stable_for_reordered_replay(spark, tmp_path):
    old = envelope(feature(
        textDescription="old", temperature=None, relativeHumidity=None, windSpeed=None,
    ), ingested_at="2026-09-12T12:30:00Z")
    a = envelope(feature(textDescription="A"))
    b = envelope(feature(textDescription="B"))
    records = [old, a, b, deepcopy(a)]
    expected = max([a, b], key=lambda record: hashlib.sha256(record["raw_json"].encode()).hexdigest())
    first = run(spark, tmp_path, records, "first")
    second = run(spark, tmp_path, list(reversed(records)), "second")
    one = spark.read.parquet(first["curated_path"]).collect()
    two = spark.read.parquet(second["curated_path"]).collect()
    assert one == two
    assert one[0].payload_hash == hashlib.sha256(expected["raw_json"].encode()).hexdigest()
    assert one[0].text_description in {"A", "B"}
    assert first["report"]["duplicate_count"] == 3
    assert first["report"]["accepted_count"] == 1
    assert first["report"]["missing_measurement_count"] == 0


def test_empty_batch_produces_typed_empty_outputs(spark, tmp_path):
    result = run(spark, tmp_path, [], stations=["KORD", "KLAX"])
    assert all(result["report"][key] == 0 for key in (
        "input_count", "accepted_count", "rejected_count", "duplicate_count", "missing_measurement_count",
    ))
    assert result["report"]["empty_station_count"] == 2
    assert result["report"]["stale_station_count"] == 0
    assert result["report"]["station_coverage"] == [
        {"station_id": station, "accepted_count": 0, "gap_count": 0,
         "latest_observed_at": None, "minutes_behind_reference": None,
         "missing_in_batch": True, "stale": False}
        for station in ("KLAX", "KORD")
    ]
    curated = spark.read.parquet(result["curated_path"])
    assert curated.count() == 0
    assert dict(curated.dtypes)["observed_at"] == "timestamp"
    assert dict(curated.dtypes)["temperature_c"] == "double"
    assert spark.read.parquet(result["rejected_path"]).count() == 0


def test_malformed_envelope_is_preserved_in_quarantine(spark, tmp_path):
    raw_path = tmp_path / "broken.ndjson"
    original_line = '{"batch_id":"incomplete"'
    raw_path.write_text(original_line + "\n")
    result = transform_batch(spark, raw_path, tmp_path / "broken", ["KORD"], "2026-09-12T13:00:00Z")
    rejected = spark.read.parquet(result["rejected_path"]).first()
    assert rejected.raw_envelope == original_line
    assert "invalid_envelope" in rejected.reasons
    assert result["report"]["input_count"] == result["report"]["rejected_count"] == 1
    assert result["report"]["accepted_count"] == 0


def test_station_coverage_signals_stale_missing_and_gaps_after_dedup(spark, tmp_path):
    records = [envelope(feature(timestamp=timestamp)) for timestamp in (
        "2026-09-12T09:00:00Z", "2026-09-12T10:00:00Z", "2026-09-12T13:00:00Z", "2026-09-12T13:00:00Z",
    )]
    result = run(spark, tmp_path, records, stations=["KORD", "KLAX"], reference="2026-09-12T16:00:00Z")
    coverage = {station["station_id"]: station for station in result["report"]["station_coverage"]}
    assert result["report"]["empty_station_count"] == result["report"]["stale_station_count"] == 1
    assert coverage["KORD"] == {
        "station_id": "KORD", "accepted_count": 3, "gap_count": 1,
        "latest_observed_at": "2026-09-12T13:00:00.000000Z", "minutes_behind_reference": 180.0,
        "missing_in_batch": False, "stale": True,
    }
    assert coverage["KLAX"]["missing_in_batch"] is True
    assert coverage["KLAX"]["latest_observed_at"] is None
